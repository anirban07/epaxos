import json
import sys
import statistics

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python stats.py [filename]")
    filename = sys.argv[1]
    with open(filename, 'r') as f:
        datastore = json.load(f)

    mean_nanos = statistics.mean(datastore["LatenciesNano"])
    print('mean latency: ' + str(mean_nanos / 10e3) + 'us')