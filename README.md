# KVStore

## Build

```shell script
mvn clean package
```

## Run

1. Master
    ```shell script
    ./start-master.sh
    ```
2. Worker 0
    ```shell script
    ./start-worker-0.sh
    ```
   
3. Worker 1
    ```shell script
    ./start-worker-1.sh
    ```
    
3. Worker 2
    ```shell script
    ./start-worker-2.sh
    
4. Generate test cases
    ```shell script
    ./python3 gen_test.py 1000
    ```

5. Run client
    ```shell script
    ./start-client.sh
