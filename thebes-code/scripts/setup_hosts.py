# Script to setup EC2 cluster for cassandra using PBS AMI in
# AWS east

import argparse
import datetime
from common_funcs import run_cmd
from common_funcs import sed
from common_funcs import upload_file
from common_funcs import run_script
import os
import os.path
from time import sleep

AMIs = {'us-east-1': 'ami-7339b41a'}

class Host:
    def __init__(self, ip, region, instanceid):
        self.ip = ip
        self.region = region
        self.instanceid = instanceid

def make_ec2(region, n, use_spot):
    if n == 0:
        return
    global AMIs
    f = raw_input("spinning up %d %s instances; okay? " %
                  (n, "spot" if use_spot else "normal"))

    if f != "Y" and f != "y":
        exit(-1)

    if use_spot:
        system("ec2-request-spot-instances %s --region %s -t t1.micro -price 0.02 " \
               "-k thebes -g thebes -n %d" % (AMIs[region], region, n));
    else:
        print "Error: Non-spot instances not implemented!"
        exit(-1)
        #system("ec2-run-instances %s -n %d -g 'cassandra' --t m1.large -k " \
        #   "'lenovo-pub' -b '/dev/sdb=ephemeral0' -b '/dev/sdc=ephemeral1'" %
        #   (AMIs[region], n))


def get_instances():
    system("rm -f instances.txt")
    global AMIs
    ret = []
    for region in AMIs.keys():
        system("ec2-describe-instances --region %s >> instances.txt" % region)

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
                ret.append(Host(ip, region, instanceid))

    return ret

def get_spot_request_ids():
    system("rm -f instances.txt")
    global AMIs
    ret = []
    for region in AMIs.keys():
        system("ec2-describe-spot-instance-requests --region %s >> instances.txt" % region)

        for line in open("instances.txt"):
            line = line.split()
            if line[0] == "SPOTINSTANCEREQUEST":
                id = line[1]
                ret.append(id)

    return ret

def get_num_running_instances(region):
    system("ec2-describe-instance-status --region %s > /tmp/running.txt" % region)
    num_running = 0

    for line in open("/tmp/running.txt"):
        line = line.split()
        if line[0] == "INSTANCE" and line[3] == "running":
            num_running = num_running + 1

    system("rm /tmp/running.txt")
    return num_running

def get_num_nonterminated_instances(region):
    system("ec2-describe-instance-status --region %s > /tmp/running.txt" % region)
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
        f.write("%s\n" % (host))
    f.close
    
def write_config():
    global cluster_config
    print "Writing config out..."
    #system("git checkout -B ec2-experiment")
    system("mkdir -p hosts")
    print cluster_config.values()
    all_hosts = [hostIP for cluster in cluster_config.values() for hostIP in cluster]
    make_instancefile("all-hosts.txt", all_hosts)
    sed("../conf/thebes.yaml", "^cluster_config: .*", "cluster_config: " + pp_cluster_config(cluster_config))
    #system("git add ../conf/thebes.yaml")
    #system("git commit -m'Config for experiment @%s'" % str(datetime.datetime.now()))
    #system("git push origin :ec2-experiment") # Delete previous remote branch
    #system("git push origin ec2-experiment")
    print "Done",

def pp_cluster_config(cluster_config):
    entries = []
    for i, servers in cluster_config.items():
        entries.append(str(i) + ": [" + ", ".join(servers) + "]")
    return "{" +  ", ".join(entries) + "}"

cluster_config = {}

def start_cluster(clusterid, region, num_hosts, use_spot):
    global cluster_config
    
    print "Starting EC2 %s hosts..." % region,
    make_ec2(region, num_hosts, use_spot)
    print "Done"

    hosts = get_instances()

    cluster_config[clusterid] = [h.ip for h in hosts]
    print "Waiting for instances to start..."
    while get_num_running_instances(region) != num_hosts:
        sleep(5)

    # Since ssh takes some time to come up
    sleep(30)
    print "Awake!"

def setup_clusters(num_clusters):
    global SCRIPTS_DIR
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
    upload_file("all-hosts", SCRIPTS_DIR + "../conf/thebes.yaml", "/home/ubuntu/thebes/thebes-code/conf", user="ubuntu")
    print "Done"

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

def terminate_cluster():
    all_instance_ids = ' '.join([h.instanceid for h in get_instances()])
    all_spot_request_ids = ' '.join(get_spot_request_ids())
    system("ec2-terminate-instances %s" % all_instance_ids)
    system("ec2-cancel-spot-instance-requests %s" % all_spot_request_ids)

SCRIPTS_DIR = ''
def detectScriptsDir():
    absPath = os.path.abspath('.')
    dirs = absPath.split(os.sep)
    for i in range(len(dirs)-1, 0, -1):
        if dirs[i] == 'thebes':
            SCRIPTS_DIR = os.sep.join(dirs[0:i+1])
            break
    SCRIPTS_DIR = os.path.join(SCRIPTS_DIR, 'thebes-code', 'scripts')
    assert os.path.exists(SCRIPTS_DIR), "Failed to detect scripts directory: " + SCRIPTS_DIR
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Setup cassandra on EC2')
    parser.add_argument('--launch', '-l', action='store_true',
                        help='Launch EC2 cluster')
    parser.add_argument('--terminate', '-t', action='store_true',
                        help='Terminate the EC2 cluster')
    parser.add_argument('--restart', '-r', action='store_true',
                        help='Restart cassandra cluster')
    parser.add_argument('--machines', '-n', dest='machines', nargs='?',
                        default=2, type=int,
                        help='Number of machines in cluster, default=2')
    parser.add_argument('--num_clusters', '-c', dest='clusters', nargs='?',
                        default=1, type=int,
                        help='Number of clusters, default=1')
    parser.add_argument('--no_spot', dest='no_spot', default=False,
                        help='Don\'t use spot instances, default off.')
    args = parser.parse_args()
    
    detectScriptsDir()

    if args.launch:
        print "Launching thebes cluster"
        regions = AMIs.keys()
        check_for_instances(regions)
        for i in range(1,args.clusters+1):
            start_cluster(i, regions[i-1], args.machines, not args.no_spot)
        write_config()
        setup_clusters(args.clusters)

    if args.restart:
        print "Restart not yet implemented"
        exit(-1)

    if args.terminate:
        print "Terminating thebes cluster"
        terminate_cluster()

    if not args.launch and not args.restart and not args.terminate:
        parser.print_help()
