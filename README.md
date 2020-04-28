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
  --w W                 the number of workers
  --t T                 the number of requests
  --m {Sequential,Causal,Eventual,Linear}
                        the consistency model
```

## Check output

```shell script
KVStore % cd logs
```

## [Project Requirement](http://homes.sice.indiana.edu/prateeks/ds/kv-project.html)