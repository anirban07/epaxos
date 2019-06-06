import sys
import numpy as np
import matplotlib.pyplot as plt
import re
import scipy
import scipy.stats
import host_config as config

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), scipy.stats.sem(a)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, m-h, m+h

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python stats.py [filename] [N]")
    filename = sys.argv[1]
    N = int(sys.argv[2])
    stats = dict()
    conflicts = -1
    Q = -1
    T = -1
    with open(filename, 'r') as f:
        for i in range(N):
            line = f.readline()
            serverName = config.IP_MAPPINGS[line.split("@")[1].strip()]
            stats[serverName] = []
            line = f.readline()
            line = line[(line.find("-c") + 2):].strip()
            if conflicts == -1:
                conflicts = int(re.search(r'\d+', line).group())
            line = line[(line.find("-q") + 2):].strip()
            if Q == -1:
                Q = int(re.search(r'\d+', line).group())
            line = line[(line.find("-T") + 2):].strip()
            if T == -1:
                T = int(re.search(r'\d+', line).group())
            for j in range(T * Q):
                stats[serverName].append(float(f.readline().strip()))

    print('Servers:', N)
    print('Clients Per Server:', T)
    print('Requests Per Client:', Q)
    print('Conflicts: %d%%' % conflicts)
    for server in stats:
        stats[server] = np.array(stats[server])
        latencies = stats[server]
        print('Server stats:', server)
        print('\tMean Latency: %fms' % np.mean(latencies))
        print('\tMax Latency: %fms' % max(latencies))
        print('\tMin Latency: %fms' % min(latencies))
        print('\tStd. Dev.: %fms' % np.std(latencies))
        print('\t99th Percentile: %fms' % np.percentile(latencies, 99))
        _, lo, hi = mean_confidence_interval(latencies)
        print('\t95%% Confidence Interval: [%f, %f]' % (lo, hi))
        print()

    # Make Bar Graph
    barWidth = 0.3
    latBars = []
    confBars = []
    for server in stats:
        latBars.append(np.mean(stats[server]))
        _, lo, hi = mean_confidence_interval(stats[server])
        confBars.append(np.std(stats[server]) * 2)

    lats = np.arange(len(latBars))
    plt.bar(lats, latBars, width=barWidth, color='orange', edgecolor='black', yerr=confBars, capsize=7)
    plt.xticks([x for x in range(len(latBars))], [server for server in stats])
    plt.ylabel('Latency (ms)')
    plt.title('Latencies For Servers')
    plt.show()