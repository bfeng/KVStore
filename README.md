# KVStore

## Build

```shell script
mvn clean package
```

## Run

```shell script
KVStore % python3 test.py -h
usage: test.py [-h] [--w W] [--t T] [--m {Sequential,Causal,Eventual,Linear}]
Launch the kv store test
optional arguments:
  -h, --help            show this help message and exit
  -w W                 the number of workers
  -t T                 the number of requests
  -m {Sequential,Causal,Eventual,Linear}
                        the consistency model
```

## Validate the Consistency

```shell script
KVStore % python3 validation.py -h
usage: validation.py [-h] [-m {Sequential,Causal,Eventual,Linear}]

validate the result

optional arguments:
  -h, --help            show this help message and exit
  -m {Sequential,Causal,Eventual,Linear}
                        the consistency model
```

## [Project Requirement](http://homes.sice.indiana.edu/prateeks/ds/kv-project.html)