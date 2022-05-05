from args import options
from aerospike_rest.api import AerospikeRestApi

if options.set == "summit":
    options.set = "users"
api = AerospikeRestApi("http://localhost:8080/v1")

headers = {"Content-Type": "application/json"}
params = {"sendKey": "true", "recordBins": "account", "jsonPath": "$.vehicles.9MOB012"}

vehicle = {
    "make": "tesla",
    "model": "model 3",
    "color": "blue",
    "year": 2021,
}
# add a vehicle to this user's account
uri = "/document/{}/{}/someguy".format(options.namespace, options.set)
api.put(uri, vehicle, params, headers, timeout=3)


# update the indexed vehicle list bin
uri = "/operate/{}/{}/someguy".format(options.namespace, options.set)
body = [
  {
    "operation": "LIST_APPEND",
    "opValues": {
      "bin": "vehicles",
      "value": "9MOB012"
    }
  }
]

res = api.post(uri, body, params, headers)
