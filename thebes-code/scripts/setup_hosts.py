# Script to setup EC2 cluster for cassandra using PBS AMI in
# AWS east

import argparse
from common_funcs import run_cmd
from common_funcs import run_cmd_single
from common_funcs import sed
from common_funcs import upload_file
from common_funcs import run_script
from common_funcs import fetch_file_single
from threading import Thread
from datetime import datetime
import os
from os import system # my pycharm sucks and can't find system by itself...
from time import sleep

#AMIs = {'us-east-1': 'ami-0cdf4965', 'us-west-1': 'ami-00b39045', 'us-west-2': 'ami-a4b83294',
#        'eu-west-1': 'ami-64636a10'}

# Upgraded AMIs
AMIs = {'us-east-1': 'ami-08188561'}

class Region:
    def __init__(self, name):
        self.name = name
        self.clusters = []
        self._ownsGraphite = False
        self.graphiteHost = None

    def ownsGraphite(self):
        return self._ownsGraphite

    def takeGraphiteOwnership(self):
        self._ownsGraphite = True

    def addCluster(self, cluster):
        self.clusters.append(cluster)

    def getTotalNumHosts(self):
        return self._ownsGraphite + sum([cluster.getNumHosts() for cluster in self.clusters])

    def getTotalNumHostsWithoutGraphite(self):
        return sum([cluster.getNumHosts() for cluster in self.clusters])

class Cluster:
    def __init__(self, regionName, clusterID, numServers, numClients, numTMs):
        self.regionName = regionName
        self.clusterID = clusterID
        self.numServers = numServers
        self.servers = []
        self.numClients = numClients
        self.clients = []
        self.numTMs = numTMs
        self.tms = []

    def allocateHosts(self, hosts):
        for host in hosts:
            if len(self.servers) < self.numServers:
                self.servers.append(host)
            elif len(self.clients) < self.numClients:
                self.clients.append(host)
            elif len(self.tms) < self.numTMs:
                self.tms.append(host)

        assert len(self.getAllHosts()) == self.getNumHosts(), "Don't have exactly as many hosts as I expect!" \
                                                              " (expect: %d, have: %d)" \
                                                              % (self.getNumHosts(), len(self.getAllHosts()))

    def getAllHosts(self):
        return self.servers + self.clients + self.tms

    def getNumHosts(self):
        return self.numServers + self.numClients + self.numTMs

class Host:
    def __init__(self, ip, regionName, instanceid):
        self.ip = ip
        self.regionName = regionName
        self.instanceid = instanceid

# UTILITIES
def run_cmd_in_thebes(hosts, cmd, user='root'):
    run_cmd(hosts, "cd /home/ubuntu/thebes/thebes-code; %s" % cmd, user)

def run_cmd_in_ycsb(hosts, cmd, user='root'):
    run_cmd(hosts, "cd /home/ubuntu/thebes/ycsb-0.1.4; %s" % cmd, user)


def get_instances(regionName):
    system("rm -f instances.txt")
    hosts = []
    system("ec2-describe-instances --region %s >> instances.txt" % regionName)

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "INSTANCE":
            ip = line[3]
            if ip == "terminated":
                continue
            status = line[5]
            if status.find("shutting") != -1:
                continue
            region = line[10]
            instanceid = line[1]
            hosts.append(Host(ip, region, instanceid))
    return hosts

def get_spot_request_ids(regionName):
    system("rm -f instances.txt")
    global AMIs
    ret = []
    system("ec2-describe-spot-instance-requests --region %s >> instances.txt" % regionName)

    for line in open("instances.txt"):
        line = line.split()
        if line[0] == "SPOTINSTANCEREQUEST":
            id = line[1]
            ret.append(id)

    return ret

def get_num_running_instances(regionName):
    system("ec2-describe-instance-status --region %s > /tmp/running.txt" % regionName)
    num_running = 0

    for line in open("/tmp/running.txt"):
        line = line.split()
        if line[0] == "INSTANCE" and line[3] == "running":
            num_running = num_running + 1

    system("rm /tmp/running.txt")
    return num_running

def get_num_nonterminated_instances(regionName):
    system("ec2-describe-instance-status --region %s > /tmp/running.txt" % regionName)
    num_nonterminated = 0

    for line in open("/tmp/running.txt"):
        line = line.split()
        if line[0] == "INSTANCE" and line[3] != "terminated":
            num_nonterminated = num_nonterminated + 1

    system("rm /tmp/running.txt")
    return num_nonterminated

def make_instancefile(name, hosts):
    f = open("hosts/" + name, 'w')
    for host in hosts:
        f.write("%s\n" % (host.ip))
    f.close

# MAIN STUFF
def check_for_instances(regions):
    numRunningAnywhere = 0
    for region in regions:
        numRunning = get_num_nonterminated_instances(region)
        numRunningAnywhere += numRunning

    if numRunningAnywhere > 0:
        pprint("NOTICE: You appear to have %d instances up already." % numRunningAnywhere)
        f = raw_input("Continue without terminating them? ")
        if f != "Y" and f != "y":
            exit(-1)


def provision_clusters(regions, use_spot, anti_slow):
    global AMIs

    for region in regions:
        assert region.name in AMIs, "No AMI for region '%s'" % region.name

        # Note: This number includes graphite, even though we won't start that up until a little later.
        f = raw_input("spinning up %d %s%s instances in %s; okay? " %
                      (region.getTotalNumHosts(), 
                      "spot" if use_spot else "normal",
                      " (+%d)" % len(region.clusters) if anti_slow else "",
                      region.name))

        if f != "Y" and f != "y":
            exit(-1)

        numHosts = region.getTotalNumHostsWithoutGraphite()
        if anti_slow: numHosts += len(region.clusters)
        if use_spot:
            provision_spot(region.name, numHosts)
        else:
            provision_instance(region.name, numHosts)

def provision_graphite(region):
    global AMIs
    if region == None:
        pprint('Graphite not enabled.')

    provision_instance(region.name, 1)


def provision_spot(regionName, num):
    global AMIs
    system("ec2-request-spot-instances %s --region %s -t t1.micro -price 0.02 " \
           "-k thebes -g thebes -n %d" % (AMIs[regionName], regionName, num));

def provision_instance(regionName, num):
    global AMIs
    system("ec2-run-instances %s --region %s -t m1.small " \
           "-k thebes -g thebes -n %d" % (AMIs[regionName], regionName, num));
    #system("ec2-run-instances %s -n %d -g 'cassandra' --t m1.large -k " \
    #   "'lenovo-pub' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" %
    #   (AMIs[region], n))


def wait_all_hosts_up(regions):
    for region in regions:
        pprint("Waiting for instances in %s to start..." % region.name)
        while True:
            numInstancesInRegion = get_num_running_instances(region.name)
            numInstancesExpected = region.getTotalNumHosts()
            if numInstancesInRegion >= numInstancesExpected:
                break
            sleep(5)
        pprint("All instances in %s alive!" % region.name)

    # Since ssh takes some time to come up
    pprint("Waiting for instances to warm up... ")
    sleep(60)
    pprint("Awake!")


    # Assigns hosts to clusters (and specifically as servers, clients, and TMs)
    # Also logs the assignments in the hosts/ files.
def assign_hosts(regions):
    allHosts = []
    allServers = []
    allClients = []
    hostsPerRegion = {}
    clusterId = 0
    system("mkdir -p hosts")

    for region in regions:
        hostsToAssign = get_instances(region.name)
        pprint("Assigning %d hosts to %s... " % (len(hostsToAssign), region.name))
        allHosts += hostsToAssign
        hostsPerRegion[region.name] = hostsToAssign

        if region.ownsGraphite():
            region.graphiteHost = hostsToAssign[0]
            make_instancefile("graphite.txt", [region.graphiteHost])
            hostsToAssign = hostsToAssign[1:]

        for cluster in region.clusters:
            cluster.allocateHosts(hostsToAssign[:cluster.getNumHosts()])
            hostsToAssign = hostsToAssign[cluster.getNumHosts():]

            # Note all the servers in our cluster.
            make_instancefile("cluster-%d-all.txt" % cluster.clusterID, cluster.getAllHosts())
            make_instancefile("cluster-%d-servers.txt" % cluster.clusterID, cluster.servers)
            make_instancefile("cluster-%d-clients.txt" % cluster.clusterID, cluster.clients)
            make_instancefile("cluster-%d-tms.txt" % cluster.clusterID, cluster.tms)
            allServers += cluster.servers
            allClients += cluster.clients

        remaining_hosts = ' '.join([h.instanceid for h in hostsToAssign])
        if remaining_hosts.strip() != '':
            pprint('Terminating excess %d instances in %s...' % (len(remaining_hosts, regionName)))
            system("ec2-terminate-instances --region %s %s" % (regionName, remaining_hosts))
        pprint("Done!")

    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", allHosts)
    make_instancefile("all-servers.txt", allServers)
    make_instancefile("all-clients.txt", allClients)
    for region, hosts in hostsPerRegion.items():
        make_instancefile("region-%s.txt" % region, hosts)

    pprint("Assigned all %d hosts!" % len(allHosts))


# Runs general setup over all hosts.
def setup_hosts(clusters):
    global SCRIPTS_DIR

    pprint("Enabling root SSH...")
    run_script("all-hosts", SCRIPTS_DIR + "/resources/enable_root_ssh.sh", user="ubuntu")
    pprint("Done")

    pprint("Uploading git key...")
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/git-repo.rsa", "/home/ubuntu/.ssh/id_rsa", user="ubuntu")
    pprint("Done")

    pprint("Uploading authorized key...")
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/git-repo.pub", "/home/ubuntu/git-repo.pub", user="ubuntu")
    pprint("Done")

    pprint("Appending authorized key...")
    run_cmd("all-hosts", "cat /home/ubuntu/git-repo.pub >> /home/ubuntu/.ssh/authorized_keys", user="ubuntu")
    pprint("Done")

    pprint("Uploading git to ssh config...")
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/config", "/home/ubuntu/.ssh/config", user="ubuntu")
    pprint("Done")

    pprint("Running startup scripts...")
    run_script("all-hosts", SCRIPTS_DIR + "/resources/node_self_setup.sh", user="ubuntu")
    pprint("Done")

def jumpstart_hosts(clusters):
    pprint("Enabling root SSH...")
    run_script("all-hosts", SCRIPTS_DIR + "/resources/enable_root_ssh.sh", user="ubuntu")
    pprint("Done")

    pprint("Exporting keys...")
    run_cmd("all-hosts", "echo export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 >> /root/.bashrc", user="root")
    run_cmd("all-hosts", "echo export JAVA_HOME=/usr/lib/jvm/java-6-openjdk-amd64 >> /home/ubuntu/.bashrc", user="root")
    pprint("Done")

    pprint("Resetting git...")
    run_cmd_in_thebes('all-hosts', 'git stash', user="ubuntu")
    pprint("Done")

    pprint("Pulling updates...")
    run_cmd_in_thebes('all-hosts', 'git pull', user="ubuntu")
    pprint("Done")

    pprint("Building thebes...")
    run_cmd_in_thebes('all-hosts', 'mvn package', user="ubuntu")
    pprint("Done")

    pprint("Building ycsb...")
    run_cmd_in_ycsb('all-clients', 'mvn clean', user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'bash fetch-thebes-jar.sh', user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'mvn package', user="ubuntu")
    pprint("Done")



# Messy string work to write out the thebes.yaml config.
def write_config(clusters, graphiteRegion):
    pprint("Writing thebes config out... ")
    #system("git checkout -B ec2-experiment")

    # resultant string: cluster_config: {1: [host1, host2], 2: [host3, host4]}
    cluster_config = []
    for cluster in clusters:
        cluster_config.append(str(cluster.clusterID) + ": [" + ", ".join([h.ip for h in cluster.servers]) + "]")
    cluster_config_str = "{" +  ", ".join(cluster_config) + "}"

    # resultant string: twopl_cluster_config: {1: [host1*, host2], 2: [host3, host4*]}
    twopl_cluster_config = []
    for cluster in clusters:
        # Put *s after servers owned by this cluster.
        twoplServerNames = [h.ip + "*" if i % len(clusters) == cluster.clusterID-1 else h.ip for i, h in enumerate(cluster.servers)]
        twopl_cluster_config.append(str(cluster.clusterID) + ": [" + ", ".join(twoplServerNames) + "]")
    twopl_cluster_config_str = "{" +  ", ".join(twopl_cluster_config) + "}"

    twopl_tm_config = []
    # resultant string: twopl_cluster_config: {1: host5, 2: host6}
    for cluster in clusters:
        if cluster.numTMs > 0:
            assert cluster.numTMs == 1, "Only support 1 TM per cluster at this time"
            twopl_tm_config.append(str(cluster.clusterID) + ": " + cluster.tms[0].ip)
    twopl_tm_config_str = "{" +  ", ".join(twopl_tm_config) + "}"

    sed(SCRIPTS_DIR + "/../conf/thebes.yaml", "^cluster_config: .*", "cluster_config: " + cluster_config_str)
    sed(SCRIPTS_DIR + "/../conf/thebes.yaml", "^twopl_cluster_config: .*", "twopl_cluster_config: " + twopl_cluster_config_str)
    sed(SCRIPTS_DIR + "/../conf/thebes.yaml", "^twopl_tm_config: .*", "twopl_tm_config: " + twopl_tm_config_str)
    sed(SCRIPTS_DIR + "/../conf/thebes.yaml", "^graphite_ip:.*", "graphite_ip: " + graphiteRegion.graphiteHost.ip)
    #system("git add ../conf/thebes.yaml")
    #system("git commit -m'Config for experiment @%s'" % str(datetime.datetime.now()))
    #system("git push origin :ec2-experiment") # Delete previous remote branch
    #system("git push origin ec2-experiment")
    pprint("Done")

    pprint("Uploading config file...")
    upload_file("all-hosts", SCRIPTS_DIR + "/../conf/thebes.yaml", "/home/ubuntu/thebes/thebes-code/conf", user="ubuntu")
    pprint("Done")

def stop_thebes_processes(clusters):
    pprint("Terminating java processes...")
    run_cmd("all-hosts", "killall -9 java")
    pprint('Termination command sent.')

def rebuild_all(clusters):
    pprint('Rebuilding clients and servers...')
    run_cmd_in_thebes("all-hosts", "git stash", user="ubuntu")
    run_cmd_in_thebes("all-hosts", "git pull", user="ubuntu")
    run_cmd_in_thebes("all-hosts", "mvn package", user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'mvn clean', user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'bash fetch-thebes-jar.sh', user="ubuntu")
    run_cmd_in_ycsb("all-clients", "mvn package", user="ubuntu")
    pprint('Servers re-built!')

def rebuild_clients(clusters):
    pprint('Rebuilding clients...')
    run_cmd_in_thebes("all-clients", "git stash", user="ubuntu")
    run_cmd_in_thebes("all-clients", "git pull", user="ubuntu")
    run_cmd_in_thebes("all-clients", "mvn package", user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'mvn clean', user="ubuntu")
    run_cmd_in_ycsb('all-clients', 'bash fetch-thebes-jar.sh', user="ubuntu")
    run_cmd_in_ycsb("all-clients", "mvn package", user="ubuntu")
    pprint('Clients re-built!')

def rebuild_servers(clusters):
    pprint('Rebuilding servers...')
    run_cmd_in_thebes("all-hosts", "git stash", user="ubuntu")
    run_cmd_in_thebes("all-hosts", "git pull", user="ubuntu")
    run_cmd_in_thebes("all-hosts", "mvn package", user="ubuntu")
    pprint('Servers re-built!')
    
CLIENT_ID = 0
def getNextClientID():
    global CLIENT_ID
    CLIENT_ID += 1
    return CLIENT_ID

def start_servers(clusters, use2PL, thebesArgString):
    baseCmd = "cd /home/ubuntu/thebes/thebes-code; rm *.log; screen -d -m "
    if not use2PL:
        runServerCmd = baseCmd + "java -ea -Dclusterid=%d -Dserverid=%d %s -jar hat/server/target/hat-server-1.0-SNAPSHOT.jar 1>server.log 2>&1"
    else:
        runServerCmd = baseCmd + "java -ea -Dclusterid=%d -Dserverid=%d %s -jar twopl/server/target/twopl-server-1.0-SNAPSHOT.jar 1>server.log 2>&1"

    runTMCmd = baseCmd + "java -ea -Dclusterid=%d -Dclientid=%d %s -jar twopl/tm/target/twopl-tm-1.0-SNAPSHOT.jar 1>tm.log 2>&1"


    pprint('Starting servers...')
    for cluster in clusters:
        for sid, server in enumerate(cluster.servers):
            pprint("Starting kv-server on [%s]" % server.ip)
            run_cmd_single(server.ip, runServerCmd % (cluster.clusterID, sid, thebesArgString), user="root")

        for tm in cluster.tms:
            pprint("Starting TM on [%s]" % tm.ip)
            run_cmd_single(tm.ip, runTMCmd % (cluster.clusterID, getNextClientID(), thebesArgString), user="root")


    pprint('Waiting for things to settle down...')
    sleep(20)
    pprint('Servers started!')


def setup_graphite(graphiteRegion):
    global SCRIPTS_DIR

    pprint("Setting up graphite on [%s]..." % graphiteRegion.graphiteHost.ip)
    upload_file("graphite", SCRIPTS_DIR + "/resources/graphite-settings.py", "/tmp/graphite-settings.py", user="ubuntu")
    upload_file("graphite", SCRIPTS_DIR + "/resources/graphite-aggregation-rules.conf", "/tmp/graphite-aggregation-rules.conf", user="ubuntu")
    run_script("graphite", SCRIPTS_DIR + "/resources/graphite-setup.sh", user="root")
    pprint("Done")

def restart_graphite(graphiteRegion):
    pprint("Stopping graphite on [%s]..." % graphiteRegion.graphiteHost.ip)
    run_cmd('graphite', 'sudo python /opt/graphite/bin/carbon-cache.py stop')
    pprint("Done")

    start_graphite(graphiteRegion)

def start_graphite(graphiteRegion):
    pprint("Starting graphite on [%s]..." % graphiteRegion.graphiteHost.ip)
    run_cmd('graphite', 'sudo python /opt/graphite/bin/carbon-cache.py start')
    pprint("Done")
    
def start_ycsb_clients(clusters, use2PL, thebesArgString, **kwargs):
    def startYCSB(runType, cluster, client, clientID):
        hosts = ','.join([host.ip for host in cluster.servers])
        run_cmd_single(client.ip,
                       'cd /home/ubuntu/thebes/ycsb-0.1.4;' \
                           'rm *.log;' \
                           'bin/ycsb %s thebes -p hosts=%s -threads %d -p fieldlength=%d -p fieldcount=1 -p operationcount=100000000 -p recordcount=%d -t ' \
                           ' -p maxexecutiontime=%d -P %s ' \
                           ' -DtransactionLengthDistributionType=%s -DtransactionLengthDistributionParameter=%d -Dclientid=%d -Dtxn_mode=%s -Dclusterid=%d -Dhat_isolation_level=%s -Datomicity_level=%s -Dconfig_file=../thebes-code/conf/thebes.yaml %s' \
                           ' 1>%s_out.log 2>%s_err.log' % (runType,
                                                           hosts,
                                                           kwargs.get("threads", 10) if runType != 'load' else 100,
                                                           kwargs.get("fieldlength", 1),
                                                           kwargs.get("recordcount", 10000),
                                                           kwargs.get("time", 60) if runType != 'load' else 10000000000,
                                                           kwargs.get("workload", "workloads/workloada"),
                                                           kwargs.get("lengthdistribution", "constant"),
                                                           kwargs.get("distributionparameter", 5),
                                                           clientID, 
                                                           "twopl" if use2PL else "hat",
                                                           cluster.clusterID,
                                                           kwargs.get("isolation_level", "NO_ISOLATION"),
                                                           kwargs.get("atomicity_level", "NO_ATOMICITY"),
                                                           thebesArgString,
                                                           runType,
                                                           runType))

    cluster = clusters[0]
    pprint("Loading YCSB on single client.")
    startYCSB('load', cluster, cluster.clients[0], 0)
    pprint("Done")

    ths = []
    pprint("Running YCSB on all clients.")
    
    for cluster in clusters:
        for i,client in enumerate(cluster.clients):
            t = Thread(target=startYCSB, args=('run', cluster, client, i+1))
            t.start()
            ths.append(t)

    for th in ths:
        th.join()
    pprint("Done")
    
def fetch_logs(runid, clusters):
    def fetchYCSB(rundir, client):
        client_dir = rundir+"/"+"C"+client.ip
        system("mkdir -p "+client_dir)
        fetch_file_single(client.ip, "/home/ubuntu/thebes/ycsb-0.1.4/*.log", client_dir)

    def fetchThebes(rundir, server):
        server_dir = rundir+"/"+"S"+server.ip
        system("mkdir -p "+server_dir)
        fetch_file_single(server.ip, "/home/ubuntu/thebes/thebes-code/*.log", server_dir) 

    outroot = './output/'+runid

    system("mkdir -p "+outroot)

    ths = []
    pprint("Fetching YCSB logs from clients.")
    for cluster in clusters:
        for i,client in enumerate(cluster.clients):
            t = Thread(target=fetchYCSB, args=(outroot, client))
            t.start()
            ths.append(t)

    for th in ths:
        th.join()
    pprint("Done")

    ths = []
    pprint("Fetching thebes logs from servers.")
    for cluster in clusters:
        for i,server in enumerate(cluster.servers):
            t = Thread(target=fetchThebes, args=(outroot, server))
            t.start()
            ths.append(t)

    for th in ths:
        th.join()
    pprint("Done")


#ssh root@ec2-50-17-17-32.compute-1.amazonaws.com "cd /home/ubuntu/thebes/ycsb-0.1.4;bash bin/ycsb.sh load thebes -p hosts=ec2-107-22-85-127.compute-1.amazonaws.com, ec2-107-21-167-213.compute-1.amazonaws.com, ec2-23-22-4-123.compute-1.amazonaws.com -threads 10 -fieldlength=1 -p fieldcount=1 -p operationcount=10000 -p recordcount=10000 -t  -p maxexecutiontime=60 -DtransactionLengthDistributionType=constant -DtransactionLengthDistributionParameter=5 -Dclientid=2 -Dtxn_mode=hat -Dclusterid=2 -Dconfig_file=../thebes-code/conf/thebes.yaml  1>load_out.log 2>load_err.log"


def terminate_clusters():
    for regionName in AMIs.keys():
        instance_ids = ' '.join([h.instanceid for h in get_instances(regionName)])
        spot_request_ids = ' '.join(get_spot_request_ids(regionName))

        if instance_ids.strip() != '':
            pprint('Terminating instances in %s...' % regionName)
            system("ec2-terminate-instances --region %s %s" % (regionName, instance_ids))
        else:
            pprint('No instances to terminate in %s, skipping...' % regionName)

        if spot_request_ids.strip() != '':
            pprint('Cancelling spot requests in %s...' % regionName)
            system("ec2-cancel-spot-instance-requests --region %s %s" % (regionName, spot_request_ids))
        else:
            pprint('No spot requests to cancel in %s, skipping...' % regionName)




SCRIPTS_DIR = ''
def detectScriptsDir():
    global SCRIPTS_DIR
    absPath = os.path.abspath('.')
    dirs = absPath.split(os.sep)
    for i in range(len(dirs)-1, 0, -1):
        if dirs[i] == 'thebes':
            SCRIPTS_DIR = os.sep.join(dirs[0:i+1])
            break
    SCRIPTS_DIR = os.path.join(SCRIPTS_DIR, 'thebes-code', 'scripts')
    assert os.path.exists(SCRIPTS_DIR), "Failed to detect scripts directory: " + SCRIPTS_DIR

def parseArgs(args):
    if args.xact_mode == 'hat':
        pprint('Using HAT mode')
        use2PL = False
    elif args.xact_mode == 'twopl':
        pprint('Using 2PL mode')
        use2PL = True
        assert args.tms <= 1, "More than 1 TM per cluster is not supported yet! (Config file does not allow it.)"
    else:
        pprint('Invalid mode (not hat or twopl).')
        exit(-1)

    graphiteRegion = None
    clusters = []
    regions = []
    clusterID = 1
    clusterConfig = args.clusters.split(",")
    for i in range(len(clusterConfig)):
        cluster = clusterConfig[i]
        if ":" in cluster:
            regionName = cluster.split(":")[0]
            numClustersInRegion = int(cluster.split(":")[1])
        else:
            regionName = cluster
            numClustersInRegion = 1

        newRegion = Region(regionName)
        regions.append(newRegion)
        for j in range(numClustersInRegion):
            newCluster = Cluster(regionName, clusterID, args.servers, args.clients, args.tms if use2PL else 0)
            clusterID += 1
            clusters.append(newCluster)
            newRegion.addCluster(newCluster)

        if regionName == args.graphite:
            newRegion.takeGraphiteOwnership()
            graphiteRegion = newRegion

    return regions, clusters, use2PL, graphiteRegion


def pprint(str):
    global USE_COLOR
    if USE_COLOR:
        print '\033[94m%s\033[0m' % str
    else:
        print str

def run_ycsb_trial(**kwargs):
    pprint("Restarting thebes clusters")
    assign_hosts(regions)
    stop_thebes_processes(clusters)
    write_config(clusters, graphiteRegion)
    restart_graphite(graphiteRegion)
    start_servers(clusters, use2PL, thebesArgString)
    start_ycsb_clients(clusters, use2PL, thebesArgString, **kwargs)
    runid = kwargs.get("runid", str(datetime.now()).replace(' ', '_'))
    fetch_logs(runid, clusters)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cassandra on EC2')
    parser.add_argument('--fetchlogs', '-f', action='store_true',
                        help='Fetch logs and exit')
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--setup', '-s', action='store_true',
                        help='Set up already running EC2 cluster')
    parser.add_argument('--terminate', '-t', action='store_true',
                        help='Terminate the EC2 cluster')
    parser.add_argument('--restart', '-r', action='store_true',
                        help='Restart thebes cluster')
    parser.add_argument('--rebuild', '-rb', action='store_true',
                        help='Rebuild thebes cluster')
    parser.add_argument('--rebuild_clients', '-rbc', action='store_true',
                        help='Rebuild thebes clients')
    parser.add_argument('--rebuild_servers', '-rbs', action='store_true',
                        help='Rebuild thebes servers')
    parser.add_argument('--num_servers', '-ns', dest='servers', nargs='?',
                        default=2, type=int,
                        help='Number of server machines per cluster, default=2')
    parser.add_argument('--num_clients', '-nc', dest='clients', nargs='?',
                        default=2, type=int,
                        help='Number of client machines per cluster, default=2')
    parser.add_argument('--num_tms', '-nt', dest='tms', nargs='?',
                        default=1, type=int,
                        help='Number of transaction managers per cluster, default=1')
    parser.add_argument('--xact_mode', '-x', dest='xact_mode', nargs='?',
                        default="hat", type=str,
                        help='Transaction mode of hat or twopl, default=hat')
    parser.add_argument('--clusters', '-c', dest='clusters', nargs='?',
                        default="us-east-1", type=str,
                        help='List of clusters to start, command delimited, default=us-east-1:1')
    parser.add_argument('--graphite', '-g', dest='graphite', nargs='?',
                        default="", type=str,
                        help='Which cluster graphite is hosted on, default=off')
    parser.add_argument('--no_spot', dest='no_spot', action='store_true',
                        help='Don\'t use spot instances, default off.')
    parser.add_argument('--anti_slow', dest='anti_slow', action='store_true',
                        help='Spawn an extra instance per cluster (and kill the slowest to start), default off.')
    parser.add_argument('--color', dest='color', action='store_true',
                        help='Print with pretty colors, default off.')
    parser.add_argument('-D', dest='thebes_args', action='append', default=[],
                        help='Parameters to pass along to the thebes servers/clients.')

    parser.add_argument('--ycsb_vary_constants_experiment', action='store_true', help='run experiment for varying constants')

    args = parser.parse_args()

    USE_COLOR = args.color
    pprint("Reminder: Run this script from an ssh-agent!")

    detectScriptsDir()
    (regions, clusters, use2PL, graphiteRegion) = parseArgs(args)
    thebesArgString = ' '.join(['-D%s' % arg for arg in args.thebes_args])

    if args.fetchlogs:
        pprint("Fetching logs")
        assign_hosts(regions)
        runid = str(datetime.now()).replace(' ', '_')
        fetch_logs(runid, clusters)
        exit(-1)

    if args.launch:
        pprint("Launching thebes clusters")
        check_for_instances(AMIs.keys())
        provision_clusters(regions, not args.no_spot, args.anti_slow)
        provision_graphite(graphiteRegion)
        wait_all_hosts_up(regions)

    if args.setup:
        assign_hosts(regions)
        #setup_hosts(clusters)
        jumpstart_hosts(clusters)
        write_config(clusters, graphiteRegion)
        #setup_graphite(graphiteRegion)
        start_graphite(graphiteRegion)
        start_servers(clusters, use2PL, thebesArgString)
        start_ycsb_clients(clusters, use2PL, thebesArgString, **kwargs)
        runid = str(datetime.now()).replace(' ', '_')
        fetch_logs(runid, clusters)

    if args.rebuild:
        pprint("Rebuilding thebes clusters")
        assign_hosts(regions)
        stop_thebes_processes(clusters)
        rebuild_all(clusters)

    if args.rebuild_clients:
        pprint("Rebuilding thebes clients")
        stop_thebes_processes(clusters)
        rebuild_clients(clusters)

    if args.rebuild_servers:
        pprint("Rebuilding thebes servers")
        stop_thebes_processes(clusters)
        rebuild_servers(clusters)

    if args.restart:
        run_ycsb_trial(runid="DEAULT_RUN",
                       threads=10,
                       distributionparameter=10,
                       atomicity_level="NO_ISOLATION",
                       isolation_level="NO_ATOMICITY")

    if args.terminate:
        pprint("Terminating thebes clusters")
        terminate_clusters()
        
    if args.ycsb_vary_constants_experiment:
        for transaction_length in [4, 8, 100]:
            for threads in [1, 10, 100]:
                for isolation_level in ["NO_ISOLATION", "READ_COMMITTED", "REPEATABLE_READ"]:
                    for atomicity_level in ["NO_ATOMICITY", "CLIENT"]:
                        if isolation_level == "NO_ISOLATION" and atomicity_level != "NO_ATOMICITY":
                            continue
                        if isolation_level == "NO_ISOLATION" and atomicity_level == "NO_ATOMICITY" and transaction_length != 4:
                            continue
                    
                        run_ycsb_trial(runid=("CONSTANT_TRANSACTION-%d-%s-%s-THREADS%d" % (transaction_length, 
                                                                                           isolation_level,
                                                                                           atomicity_level,
                                                                                           threads)),
                                       threads=threads,
                                       distributionparameter=transaction_length,
                                       atomicity_level=atomicity_level,
                                       isolation_level=isolation_level)
                
    if not args.launch and not args.rebuild and not args.restart and not args.terminate:
        parser.print_help()

