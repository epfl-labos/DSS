#!/usr/bin/env python

import sys
import json

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: {} <num_nodes>".format(sys.argv[0])

    num_nodes = int(sys.argv[1])

    print json.dumps({ "rack": "default-rack", "nodes": [ { "node": "node{}".format(node_id) } for node_id in xrange(1, num_nodes + 1) ] },
            sort_keys=True, indent=4, separators=(',', ': '))

