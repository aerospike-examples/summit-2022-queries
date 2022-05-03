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
def counter(part_id, rec):
    cnt[0] = cnt[0] + 1

filter_exp = exp.Eq(
    exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.STRING, "gender", exp.MapBin("account")),
    "f"
).compile()
query_policy = {
    "partition_filter": {"begin": 0, "count": 4096},
    "expressions": filter_exp}
query = client.query(options.namespace, options.set)
query.where(pred.between("dob", 19800101, 1980107))
query.foreach(counter, query_policy, {"nobins": True})
print("\nThere are {} female users born in the first week of 1980".format(cnt[0]))

client.close()
