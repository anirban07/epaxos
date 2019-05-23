import json
import sys
import numpy as np
import matplotlib.pyplot as plt

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python stats.py [filename]")
    filename = sys.argv[1]
    with open(filename, 'r') as f:
        datastore = json.load(f)

    latencies = np.divide(np.array(datastore["LatenciesNano"]), 10e3)
    num_reqs = datastore["ReqsNb"]
    conflicts = datastore["Conflicts"]
    mean_nanos = np.mean(latencies)
    print('Number of requests', num_reqs)
    print('Conflict percentage', conflicts)
    print('mean latency: ' + str(mean_nanos / 10e3) + 'us')

    fig, axs = plt.subplots()
    axs.hist(latencies)
    axs.boxplot(latencies)
    plt.show()