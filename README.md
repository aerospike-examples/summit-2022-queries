# Aerospike Summit 2022 Secondary Index Presentation
Companion code for the Aerospike Summit 2022 roadshow presentation "Building with Secondary Indexes and Document API in Aerospike Database 6".

## Dependencies
This demonstration includes optional use of the [Aerospike gateway's](https://github.com/aerospike/aerospike-client-rest)
Document API (AKA the 'REST client'). If you would like to run
`document-api-example.py` you need to do get the following:

 - Aerospike Database 6.0
 - Python 3.7+
 - Aerospike Python Client >= 7.0.0
 - The "REST client" from the [download page](https://aerospike.com/download/#clients)
 - The Swagger generated Python client for the REST (a git submodule)

```
pip install aerospike

git submodule init # fetches the AerospikeRestApi client source
cd aerospike-python-rest
python setup.py build
python setup.py install
```

## Sequence
The presentation mentions which scripts are being run for each slide. The
sequence is as follows:

 1. `fill.py` to fill the database with sample data
 2. `pi-query.py` to demonstrate partitioned PI queries (FKA _scans_)
 3. `pi-query-filter-exp.py` to demonstrate the use of filter expressiosn with a PI query
 4. `pi-query-paginated.py` to demonstrate query pagination
 5. `document-api-example.py` to show an alternative way to add documents
 5. `si-query-filter-exp.py` combining an SI query with a filter expression
 6. `si-query.py` demonstrates an SI query with no filter expression

## Options
All the scripts support the following options:

```
optional arguments:
  --help                Displays this message.
  -U <USERNAME>, --username <USERNAME>
                        Username to connect to database.
  -P <PASSWORD>, --password <PASSWORD>
                        Password to connect to database.
  -h <ADDRESS>, --host <ADDRESS>
                        Address of Aerospike server.
  -p <PORT>, --port <PORT>
                        Port of the Aerospike server.
  -n <NS>, --namespace <NS>
                        Namespace name to use
  -s <SET>, --set <SET>
                        Set name to use.
  --services-alternate  Use services alternate
```
