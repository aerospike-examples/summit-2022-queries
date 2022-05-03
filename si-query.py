# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike import predicates as pred
from aerospike_helpers import expressions as exp
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
def result(part_id, rec):
    k, _, b = rec
    cnt[0] = cnt[0] + 1
    print("userkey: {} partition ID {}".format(k[2], part_id))
    print(b["account"], "\n")

query_policy = {
    "partition_filter": {"begin": 0, "count": 4096}
}
query = client.query(options.namespace, options.set)
query.where(pred.contains("vehicles", aerospike.INDEX_TYPE_LIST, "8ZVU730"))
query.foreach(result, query_policy)
print("There are {} users with this vehicle".format(cnt[0]))

client.close()
