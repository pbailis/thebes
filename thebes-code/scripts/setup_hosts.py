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

AMIs = {'us-east-1': 'ami-7339b41a'}

class Region:
    def __init__(self, name):
        self.name = name
        self.clusters = []

    def addCluster(self, cluster):
        self.clusters.append(cluster)

    def getTotalNumHosts(self):
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
        print "NOTICE: You appear to have %d instances up already." % numRunningAnywhere
        f = raw_input("Continue without terminating them? ")
        if f != "Y" and f != "y":
            exit(-1)


def provision_clusters(regions, use_spot):
    global AMIs

    for region in regions:
        assert region.name in AMIs, "No AMI for region '%s'" % region.name

        f = raw_input("spinning up %d %s instances in %s; okay? " %
                      (region.getTotalNumHosts(), "spot" if use_spot else "normal", region.name))

        if f != "Y" and f != "y":
            exit(-1)

        if use_spot:
            provision_spot(region.name, region.getTotalNumHosts())
        else:
            provision_instance(region.name, region.getTotalNumHosts())

def provision_spot(regionName, num):
    global AMIs
    system("ec2-request-spot-instances %s --region %s -t t1.micro -price 0.02 " \
           "-k thebes -g thebes -n %d" % (AMIs[regionName], regionName, num));

def provision_instance(region, num):
    #system("ec2-run-instances %s -n %d -g 'cassandra' --t m1.large -k " \
    #   "'lenovo-pub' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" %
    #   (AMIs[region], n))
    print "Error: Non-spot instances not implemented!"
    exit(-1)


def wait_all_hosts_up(regions):
    for region in regions:
        print "Waiting for instances in %s to start..." % region.name
        while True:
            numInstancesInRegion = get_num_running_instances(region.name)
            numInstancesExpected = region.getTotalNumHosts()
            assert numInstancesInRegion <= numInstancesExpected, "More instances running (%d) than started (%d)!" % (numInstancesInRegion, numInstancesExpected)
            if numInstancesInRegion == numInstancesExpected:
                break
            sleep(5)
        print "All instances in %s alive!" % region.name

    # Since ssh takes some time to come up
    print "Waiting for instances to warm up... "
    sleep(30)
    print "Awake!"


# Assigns hosts to clusters (and specifically as servers, clients, and TMs)
# Also logs the assignments in the hosts/ files.
def assign_hosts(regions):
    allHosts = []
    hostsPerRegion = {}
    clusterId = 0
    system("mkdir -p hosts")

    for region in regions:
        hostsToAssign = get_instances(region.name)
        print "Assigning %d hosts to %s... " % (len(hostsToAssign), region.name),
        allHosts += hostsToAssign
        hostsPerRegion[region.name] = hostsToAssign

        for cluster in region.clusters:
            cluster.allocateHosts(hostsToAssign[:cluster.getNumHosts()])
            hostsToAssign = hostsToAssign[cluster.getNumHosts():]

            # Note all the servers in our cluster.
            make_instancefile("cluster-%d-all.txt" % cluster.clusterID, cluster.getAllHosts())
            make_instancefile("cluster-%d-servers.txt" % cluster.clusterID, cluster.servers)
            make_instancefile("cluster-%d-clients.txt" % cluster.clusterID, cluster.clients)
            make_instancefile("cluster-%d-tms.txt" % cluster.clusterID, cluster.tms)

        print "Done!"

    # Finally write the instance files for the regions and everything.
    make_instancefile("all-hosts.txt", allHosts)
    for region, hosts in hostsPerRegion.items():
        make_instancefile("region-%s.txt" % region, hosts)

    print "Assigned all %d hosts!" % len(allHosts)


# Messy string work to write out the thebes.yaml config.
def write_config(clusters):
    print "Writing thebes config out... ",
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
    #system("git add ../conf/thebes.yaml")
    #system("git commit -m'Config for experiment @%s'" % str(datetime.datetime.now()))
    #system("git push origin :ec2-experiment") # Delete previous remote branch
    #system("git push origin ec2-experiment")
    print "Done",


# Runs general setup over all hosts.
def setup_hosts(clusters):
    global SCRIPTS_DIR
    print SCRIPTS_DIR, SCRIPTS_DIR + "/resources/enable_root_ssh.sh"
    print "Enabling root SSH...",
    run_script("all-hosts", SCRIPTS_DIR + "/resources/enable_root_ssh.sh", user="ubuntu")
    print "Done"
    
    print "Uploading git key...",
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/git-repo.rsa", "/home/ubuntu/.ssh/id_rsa", user="ubuntu")
    print "Done"
    
    print "Uploading authorized key...",
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/git-repo.pub", "/home/ubuntu/git-repo.pub", user="ubuntu")
    print "Done"
    
    print "Appending authorized key...",
    run_cmd("all-hosts", "cat /home/ubuntu/git-repo.pub >> /home/ubuntu/.ssh/authorized_keys", user="ubuntu")
    print "Done"

    print "Uploading git to ssh config...",
    upload_file("all-hosts", SCRIPTS_DIR + "/resources/config", "/home/ubuntu/.ssh/config", user="ubuntu")
    print "Done"
    
    print "Running startup scripts...",
    run_script("all-hosts", SCRIPTS_DIR + "/resources/node_self_setup.sh", user="ubuntu")
    print "Done"

    print "Uploading config file...",
    upload_file("all-hosts", SCRIPTS_DIR + "/../conf/thebes.yaml", "/home/ubuntu/thebes/thebes-code/conf", user="ubuntu")
    print "Done"


def stop_thebes_processes(clusters):
    print "Terminating java processes..."
    run_cmd("all-hosts", "killall -9 java")
    print 'Termination command sent.'

def rebuild_servers(clusters):
    print 'Rebuilding servers...'
    run_cmd("all-hosts", "cd /home/ubuntu/thebes/thebes-code; git pull", user="ubuntu")
    run_cmd("all-hosts", "cd /home/ubuntu/thebes/thebes-code; mvn package", user="ubuntu")
    print 'Servers re-built!'


def start_servers(clusters, use2PL):
    baseCmd = "cd /home/ubuntu/thebes/thebes-code; screen -d -m bin/"
    if not use2PL:
        baseCmd += "hat/run-hat-server.sh %d %d"
    else:
        baseCmd += "twopl/run-twopl-server.sh %d %d"

    print 'Starting servers...'
    for cluster in clusters:
        for sid, server in enumerate(cluster.servers):
            print "Starting server on [%s]" % server.ip
            run_cmd_single(server.ip, baseCmd % (cluster.clusterID, sid), user="root")

    print 'Waiting for things to settle down...'
    sleep(5)
    print 'Servers started!'


def terminate_clusters():
    all_instance_ids = ''
    all_spot_request_ids = ''

    for regionName in AMIs.keys():
        all_instance_ids += ' '.join([h.instanceid for h in get_instances(regionName)]) + ' '
        all_spot_request_ids += ' '.join(get_spot_request_ids(regionName)) + ' '

    if all_instance_ids.strip() != '':
        print 'Terminating instances...'
        system("ec2-terminate-instances %s" % all_instance_ids)
    else:
        print 'No instances to terminate, skipping...'

    if all_spot_request_ids.strip() != '':
        print 'Cancelling spot requests...'
        system("ec2-cancel-spot-instance-requests %s" % all_spot_request_ids)
    else:
        print 'No spot requests to cancel, skipping...'


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
        print 'Using HAT mode'
        use2PL = False
    elif args.xact_mode == 'twopl':
        print 'Using 2PL mode'
        use2PL = True
        assert args.tms <= 1, "More than 1 TM per cluster is not supported yet! (Config file does not allow it.)"
    else:
        print 'Invalid mode (not hat or twopl).'
        exit(-1)

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

    return regions, clusters, use2PL

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
    parser.add_argument('--no_spot', dest='no_spot', default=False,
                        help='Don\'t use spot instances, default off.')
    args = parser.parse_args()
    
    detectScriptsDir()
    (regions, clusters, use2PL) = parseArgs(args)

    if args.launch:
        print "Launching thebes clusters"
        check_for_instances(AMIs.keys())
        provision_clusters(regions, not args.no_spot)
        wait_all_hosts_up(regions)
        assign_hosts(regions)
        write_config(clusters)
        setup_hosts(clusters)
        start_servers(clusters, use2PL)

    if args.restart:
        print "Rebuilding and restarting thebes clusters"
        assign_hosts(regions)
        rebuild_servers(clusters)
        stop_thebes_processes(clusters)
        start_servers(clusters, use2PL)


    if args.terminate:
        print "Terminating thebes clusters"
        terminate_clusters()

    if not args.launch and not args.restart and not args.terminate:
        parser.print_help()
