# Script to setup EC2 cluster for cassandra using PBS AMI in
# AWS east

import argparse
from common_funcs import run_cmd
from common_funcs import run_cmd_single
from common_funcs import sed
from common_funcs import upload_file
from common_funcs import run_script
import os
from os import system # my pycharm sucks and can't find system by itself...
from time import sleep

#AMIs = {'us-east-1': 'ami-7339b41a'}
AMIs = {'us-east-1': 'ami-0cdf4965', 'us-west-1': 'ami-00b39045', 'us-west-2': 'ami-a4b83294',
        'eu-west-1': 'ami-64636a10'}

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
        self.numServers = numServers;
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


def provision_clusters(regions, use_spot):
    global AMIs

    for region in regions:
        assert region.name in AMIs, "No AMI for region '%s'" % region.name

        # Note: This number includes graphite, even though we won't start that up until a little later.
        f = raw_input("spinning up %d %s instances in %s; okay? " %
                      (region.getTotalNumHosts(), "spot" if use_spot else "normal", region.name))

        if f != "Y" and f != "y":
            exit(-1)

        if use_spot:
            provision_spot(region.name, region.getTotalNumHostsWithoutGraphite())
        else:
            provision_instance(region.name, region.getTotalNumHostsWithoutGraphite())

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
    system("ec2-run-instances %s --region %s -t t1.micro " \
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
            assert numInstancesInRegion <= numInstancesExpected, "More instances running (%d) than started (%d)!" % (numInstancesInRegion, numInstancesExpected)
            if numInstancesInRegion == numInstancesExpected:
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

        pprint("Done!")

    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", allHosts)
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

    # resultant string: twopl_cluster_config: {1: host5, 2: host6}
    twopl_tm_config = []
    for cluster in clusters:
        if cluster.numTMs > 0:
            assert cluster.numTMs == 1, "Only support 1 TM per cluster at this time"
            twopl_tm_config.append(str(cluster.clusterID) + ": " + cluster.tms[0].ip)
    twopl_tm_config_str = "{" +  ", ".join(twopl_tm_config) + "}"

    sed("../conf/thebes.yaml", "^cluster_config: .*", "cluster_config: " + cluster_config_str)
    sed("../conf/thebes.yaml", "^twopl_cluster_config: .*", "twopl_cluster_config: " + twopl_cluster_config_str)
    sed("../conf/thebes.yaml", "^twopl_tm_config: .*", "twopl_tm_config: " + twopl_tm_config_str)
    sed("../conf/thebes.yaml", "^graphite_ip: .*", "graphite_ip: " + graphiteRegion.graphiteHost.ip)
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

def rebuild_servers(clusters):
    pprint('Rebuilding servers...')
    run_cmd("all-hosts", "cd /home/ubuntu/thebes/thebes-code; git stash", user="ubuntu")
    run_cmd("all-hosts", "cd /home/ubuntu/thebes/thebes-code; git pull", user="ubuntu")
    run_cmd("all-hosts", "cd /home/ubuntu/thebes/thebes-code; mvn package", user="ubuntu")
    pprint('Servers re-built!')


CLIENT_ID = 0
def getNextClientID():
    global CLIENT_ID
    CLIENT_ID += 1
    return CLIENT_ID

def start_servers(clusters, use2PL):
    baseCmd = "cd /home/ubuntu/thebes/thebes-code; screen -d -m bin/"
    if not use2PL:
        runServerCmd = baseCmd + "hat/run-hat-server.sh %d %d"
    else:
        runServerCmd = baseCmd + "twopl/run-twopl-server.sh %d %d"

    runTMCmd = baseCmd + "twopl/run-twopl-tm.sh %d %d"

    pprint('Starting servers...')
    for cluster in clusters:
        for sid, server in enumerate(cluster.servers):
            pprint("Starting kv-server on [%s]" % server.ip)
            run_cmd_single(server.ip, runServerCmd % (cluster.clusterID, sid), user="root")

        for tm in cluster.tms:
            pprint("Starting TM on [%s]" % tm.ip)
            run_cmd_single(tm.ip, runTMCmd % (cluster.clusterID, getNextClientID()), user="root")


    pprint('Waiting for things to settle down...')
    sleep(10)
    pprint('Servers started!')

def setup_and_start_graphite(graphiteRegion):
    global SCRIPTS_DIR

    pprint("Starting graphite on [%s]..." % graphiteRegion.graphiteHost.ip)
    upload_file("graphite", SCRIPTS_DIR + "/resources/graphite-settings.py", "/tmp/graphite-settings.py", user="ubuntu")
    run_script("graphite", SCRIPTS_DIR + "/resources/graphite-setup.sh", user="root")
    pprint("Done")


def terminate_clusters():
    all_instance_ids = ''
    all_spot_request_ids = ''

    for regionName in AMIs.keys():
        all_instance_ids += ' '.join([h.instanceid for h in get_instances(regionName)]) + ' '
        all_spot_request_ids += ' '.join(get_spot_request_ids(regionName)) + ' '

    if all_instance_ids.strip() != '':
        pprint('Terminating instances...')
        system("ec2-terminate-instances %s" % all_instance_ids)
    else:
        pprint('No instances to terminate, skipping...')

    if all_spot_request_ids.strip() != '':
        pprint('Cancelling spot requests...')
        system("ec2-cancel-spot-instance-requests %s" % all_spot_request_ids)
    else:
        pprint('No spot requests to cancel, skipping...')


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cassandra on EC2')
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--terminate', '-t', action='store_true',
                        help='Terminate the EC2 cluster')
    parser.add_argument('--restart', '-r', action='store_true',
                        help='Restart cassandra cluster')
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
    parser.add_argument('--color', dest='color', action='store_true',
                        help='Print with pretty colors, default off.')
    args = parser.parse_args()

    USE_COLOR = args.color
    pprint("Reminder: Run this script from an ssh-agent!")

    detectScriptsDir()
    (regions, clusters, use2PL, graphiteRegion) = parseArgs(args)


    if args.launch:
        pprint("Launching thebes clusters")
        check_for_instances(AMIs.keys())
        provision_clusters(regions, not args.no_spot)
        provision_graphite(graphiteRegion)
        wait_all_hosts_up(regions)
        assign_hosts(regions)
        setup_hosts(clusters)
        write_config(clusters, graphiteRegion)
        setup_and_start_graphite(graphiteRegion)
        start_servers(clusters, use2PL)

    if args.restart:
        pprint("Rebuilding and restarting thebes clusters")
        assign_hosts(regions)
        stop_thebes_processes(clusters)
        rebuild_servers(clusters)
        write_config(clusters, graphiteRegion)
        setup_and_start_graphite(graphiteRegion)
        start_servers(clusters, use2PL)


    if args.terminate:
        pprint("Terminating thebes clusters")
        terminate_clusters()

    if not args.launch and not args.restart and not args.terminate:
        parser.print_help()
