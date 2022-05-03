# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception
from aerospike_helpers import cdt_ctx as ctx
from aerospike_helpers import expressions as exp
from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import map_operations as mh
from aerospike_helpers.operations import expression_operations as opexp
import random
import string
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

client.truncate(options.namespace, options.set, 0)
policy = {"key": aerospike.POLICY_KEY_SEND}
map_policy = {
    "map_write_mode": aerospike.MAP_UPDATE,
    "map_order": aerospike.MAP_KEY_ORDERED,
}

alnum = string.ascii_uppercase + string.digits
brands = [
    "acura",
    "audi",
    "bmw",
    "chevrolet",
    "dodge",
    "ford",
    "gmc",
    "honda",
    "hyndai",
    "jeep",
    "kia",
    "lexus",
    "mazda",
    "nissan",
    "subaru",
    "tesla",
    "toyota",
    "volkswagen",
]
colors = [
    "white",
    "gray",
    "blue",
    "red",
    "black",
    "olive",
    "green",
    "orange",
    "yellow",
    "blue",
]
dob_sync_exp = exp.MapGetByKey(
    None,
    aerospike.MAP_RETURN_VALUE,
    exp.ResultType.INTEGER,
    "dob",
    exp.MapBin("account"),
).compile()
gender_sync_exp = exp.MapGetByKey(
    None,
    aerospike.MAP_RETURN_VALUE,
    exp.ResultType.STRING,
    "gender",
    exp.MapBin("account"),
).compile()
vehicles = [ctx.cdt_ctx_map_key("vehicles")]
vehicle_sync_exp = exp.MapGetByIndexRangeToEnd(
    vehicles, aerospike.MAP_RETURN_KEY, 0, exp.MapBin("account")
).compile()
cards = [ctx.cdt_ctx_map_key("cards")]
card_sync_exp = exp.MapGetByIndexRangeToEnd(
    cards, aerospike.MAP_RETURN_KEY, 0, exp.MapBin("account")
).compile()

for b in range(400):
    batch = []
    for i in range(2500):
        pk = "".join(random.choices(string.ascii_lowercase, k=8))
        fname = "".join(random.choices(string.ascii_lowercase, k=6))
        lname = "".join(random.choices(string.ascii_lowercase, k=8))
        dob = (
            random.randrange(1945, 2003) * 10000
            + random.randrange(1, 12) * 100
            + random.randrange(1, 29)
        )
        gender = random.choices("mfo", [4977, 4977, 46])[0]
        ccn = str(random.randrange(4111111111111111, 4999999999999999))
        cvv = random.randrange(100, 999)
        exp = random.randrange(23, 25) * 100 + random.randrange(1, 12)
        lic = "".join(random.choices(alnum, k=7))
        make = random.choice(brands)
        color = random.choice(colors)
        year = random.randrange(2000, 2022)
        user = {
            "fname": fname,
            "lname": lname,
            "dob": dob,
            "gender": gender,
            "cards": {
                ccn: {
                    "alias": "default",
                    "cvv": cvv,
                    "exp": exp,
                }
            },
            "vehicles": {lic: {"make": make, "color": color, "year": year}},
        }
        key = (options.namespace, options.set, pk)
        ops = [
            mh.map_put_items("account", user, map_policy),
            opexp.expression_write("dob", dob_sync_exp),
            opexp.expression_write("gender", gender_sync_exp),
            opexp.expression_write("vehicles", vehicle_sync_exp),
            opexp.expression_write("cards", card_sync_exp),
        ]
        batch.append(br.Write(key, ops, policy))
    client.batch_write(br.BatchRecords(batch))

    user = {
        "fname": "Some",
        "lname": "Guy",
        "dob": 19800101,
        "gender": "m",
        "cards": {
            "4141232356567878": {
                "alias": "default",
                "cvv": 123,
                "exp": 2502,
                "issuer": "visa",
            }
        },
        "vehicles": {
            "8CZU999": {
                "make": "toyota",
                "color": "gray",
                "model": "rav4",
                "year": 2018,
            },
            "8ZVU730": {
                "make": "toyota",
                "model": "prius",
                "color": "black",
                "year": 2013,
            },
        },
    }
    ops = [
        mh.map_put_items("account", user, map_policy),
        opexp.expression_write("dob", dob_sync_exp),
        opexp.expression_write("gender", gender_sync_exp),
        opexp.expression_write("vehicles", vehicle_sync_exp),
        opexp.expression_write("cards", card_sync_exp),
    ]
    client.operate((options.namespace, options.set, "someguy"), ops, policy=policy)

    user = {
        "fname": "Hi",
        "lname": "There",
        "dob": 19800326,
        "gender": "f",
        "cards": {
            "4141232356567878": {
                "alias": "default",
                "cvv": 123,
                "exp": 2502,
            }
        },
        "vehicles": {
            "8CZU999": {
                "make": "toyota",
                "model": "rav4",
                "color": "gray",
                "year": 2018,
            },
            "8ZVU730": {
                "make": "toyota",
                "model": "prius",
                "color": "black",
                "year": 2013,
            },
        },
    }
    ops = [
        mh.map_put_items("account", user, map_policy),
        opexp.expression_write("dob", dob_sync_exp),
        opexp.expression_write("gender", gender_sync_exp),
        opexp.expression_write("vehicles", vehicle_sync_exp),
        opexp.expression_write("cards", card_sync_exp),
    ]
    client.operate((options.namespace, options.set, "hithere"), ops, policy=policy)

client.close()
