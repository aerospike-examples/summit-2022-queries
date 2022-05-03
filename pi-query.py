# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import expressions as exp
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import map_operations as mh
import sys

if options.set == "summit":
    options.set = "users"
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except exception.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    print(e)
    sys.exit(1)

cnt = [0]
def counter(part_id, rec):
    cnt[0] = cnt[0] + 1

# query for the number of records in all the partitions
query_policy = {"partition_filter": {"begin": 0, "count": 4096}}
query = client.query(options.namespace, options.set)
query.foreach(counter, query_policy, {"nobins": True})
print("There are {} records in general".format(cnt[0]))

# query for the number of records in partition 0
cnt[0] = 0
query_policy = {"partition_filter": {"begin": 0, "count": 1}}
query = client.query(options.namespace, options.set)
query.foreach(counter, query_policy, {"nobins": True})
print("\nThere are {} records in partition 0".format(cnt[0]))

# query for records in partition 4095, but ditch the query after one result
def result(part_id, rec):
    _, _, b = rec
    print(b["account"])
    return False

print("\nRecords in partition 4095:")
query_policy = {"partition_filter": {"begin": 4095, "count": 1}}
query = client.query(options.namespace, options.set)
query.foreach(result, query_policy)

client.close()
