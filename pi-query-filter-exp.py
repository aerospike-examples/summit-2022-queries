# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
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

filter_exp = exp.Let(
  exp.Def(
    "dob", exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.INTEGER, "dob", exp.MapBin("account"))
  ),
  exp.Def(
    "gender", exp.MapGetByKey(None, aerospike.MAP_RETURN_VALUE, exp.ResultType.STRING, "gender", exp.MapBin("account"))
  ),
  exp.And(
    exp.And(
      exp.GE(exp.Var("dob"), 19800101),
      exp.LE(exp.Var("dob"), 19800107)
    ),
    exp.Eq(exp.Var("gender"), "f")
  )
).compile()
cnt[0] = 0
query_policy = {
    "partition_filter": {"begin": 0, "count": 4096},
    "expressions": filter_exp}
query = client.query(options.namespace, options.set)
query.foreach(counter, query_policy, {"nobins": True})
print("\nThere are {} female users born in the first week of 1980".format(cnt[0]))

client.close()
