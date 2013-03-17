import matplotlib
matplotlib.use('Agg')

from pylab import *
from os import listdir
from sys import argv

if len(argv) == 1:
    rootdir = "../output-wan"
else:
    rootdir = argv[1]

class RunResult:
    def __init__(self, ops, time, lats, avg):
        self.ops = ops
        self.time = time
        self.lats = lats
        self.avg = avg
        self.merges = 0

    def get_thru(self):
        return self.ops/(self.time/1000.)*self.merges

    def get_pctile_latency(self, pctile):
        target = self.ops*pctile

        total = 0
        for bucketno in range(0, len(self.lats)):
            total += self.lats[bucketno]
            if total >= target:
                return bucketno

    def get_average_latency(self):
        return self.avg/1000.

    def mergeResult(self, other):
        self.merges += 1
        self.time += other.time
        self.ops += other.ops

        totalops = float(self.ops+other.ops)
        self.avg = self.avg*(self.ops/totalops)+other.avg*(other.ops/totalops)

        if len(self.lats) == 0:
            self.lats = other.lats
            return True
            
        if len(self.lats) != len(other.lats):
            print "LATENCY MISMATCH:", len(self.lats), len(other.lats)
            return False

        for bucketno in range(0, len(self.lats)):
            self.lats[bucketno] += other.lats[bucketno]

        return True

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def processYCSB(d):
    lats = []

    ops = None
    avglat = None
    time = None

    for line in open(d+"/run_out.log"):
        line = line.split()
        if len(line) == 0:
            continue
        if line[0].find("OVERALL") != -1:
            if line[1].find("RunTime") != -1:
                time = float(line[2])
        if line[0].find("TRANSACTION") != -1:
            if line[1].find("AverageLatency") != -1:
                avglat = float(line[2])
            elif line[1].find("Operations") != -1:
                ops = int(line[2])
            elif is_number(line[1][:-1]) or is_number(line[1][1:-1]):
                if is_number(line[2]):
                    lats.append(int(line[2]))
                else:
                    print d, line

    if ops is None or time is None or len(lats) == 0 or avglat is None:
        print ops, time, len(lats), avglat
        return None

    return RunResult(ops, time, lats, avglat)

def processConfig(d):
    combinedResult = RunResult(0, 0, [], 0)
    for cd in listdir(d):
        if cd[0] == "C":
            result = processYCSB(d+"/"+cd)
            if result is None:
                print "PROCESSING FAILED ON", d
                return None
            if not combinedResult.mergeResult(result):
                print "MERGING FAILED ON", d
                return None

    return combinedResult

results = {}

txnlens = []

for config in listdir(rootdir):
    configsplit=config.split("-THREADS")
    nthreads = int(configsplit[1])
    txnlength = configsplit[0].split('-')[1]

    if int(txnlength) not in txnlens:
        txnlens.append(int(txnlength))

    configstr = configsplit[0].replace("-"+txnlength+"-", "")
    configstr = configstr.replace("-"+txnlength, "")

    r = processConfig(rootdir+"/"+config)
    if r is None:
        print "SKIPPING", config
        continue
    r.threads = nthreads
    r.txnlength = int(txnlength)

    if configstr not in results:
        results[configstr] = []

    results[configstr].append(r)

txnlens.sort()

PCT = 0
AVG = 1

for latplot in [PCT, AVG]:
    for txnlen in txnlens:
        for cs in results.keys():
            pairs = []

            for r in results[cs]:
                if str(r.txnlength) != str(txnlen):
                    continue

                print cs, txnlen, "THREADS", r.threads, "THRU", r.get_thru(), "LATS", r.get_average_latency(), r.get_pctile_latency(.999)

                if latplot == PCT:
                    lat = r.get_pctile_latency(.999)
                else:
                    lat = r.get_average_latency()

                pairs.append((r.get_thru(), lat))

            pairs.sort(key=lambda x: x[1])
            l = cs.replace("CONSTANT_TRANSACTION", "")
            plot([p[0] for p in pairs], [p[1] for p in pairs], label=l)

        legend(loc="upper right")
        title("Transactions of length "+str(txnlen))
        gca().set_yscale('log')
        xlabel("Throughput (Txns/s)")
        ylabel(("99.9th Percentile" if latplot == PCT else "Average")+" Latency (ms)")

        legstr = "PCTILE" if latplot == PCT else "AVG"

        savefig(str(txnlen)+legstr+"-plot.pdf")
        clf()
