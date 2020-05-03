package kvstore.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.ReadReq;
import kvstore.common.ReadResp;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.servers.MasterServiceGrpc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class KVClient {
    private static final Logger logger = Logger.getLogger(KVClient.class.getName());
    private int clientId;
    private int serverPort;
    private String serverIp;
    private List<ReqContent> reqList;
    ManagedChannel masterChannel;

    public KVClient(String configuration, int clientId) {
        this.clientId = clientId;
        reqList = new ArrayList<>();
        try {
            parse(configuration);
            logger.info("configure server: " + serverIp + ":" + Integer.toString(serverPort));
        } catch (IOException e) {
            //
        }
        this.masterChannel = ManagedChannelBuilder.forAddress(serverIp, serverPort).usePlaintext().build();
    }

    private void parse(String configuration) throws IOException {
        List<String> allLines = Files.readAllLines(Paths.get(configuration));
        for (String line : allLines) {
            String[] conf = line.split(":");
            switch (conf[0]) {
                case "server":
                    this.serverIp = conf[1];
                    this.serverPort = Integer.parseInt(conf[2]);
                    break;
                case "request":
                    reqList.add(new ReqContent(conf[1], conf[2], conf[3], conf[4]));
                    break;
                default:
                    System.err.println("Unknown conf: " + line);
            }
        }
    }

    static class ReqContent {
        public int action;
        public String key;
        public String value;
        public String option;

        public ReqContent(String action, String key, String value, String option) {
            switch (action) {
                case "GET":
                    this.action = 0;
                    break;
                case "SET":
                    this.action = 1;
                    break;
                default:
                    System.err.println("Undefined operation: " + action);
            }
            this.key = key;
            this.value = value;

            switch (option) {
                case "Sequential":
                    this.option = "Sequential";
                    break;
                case "Causal":
                    this.option = "Causal";
                    break;
                case "Eventual":
                    this.option = "Eventual";
                    break;
                case "Linear":
                    this.option = "Linear";
                    break;
                default:
                    System.err.println("Undefined consistency option: " + option);
            }
        }

        public int getAct() {
            return this.action;
        }

        public String getKey() {
            return this.key;
        }

        public String getVal() {
            return this.value;
        }

        public String getOpt() {
            return this.option;
        }
    }

    void write(String key, String value, CountDownLatch finishLatch) {
        MasterServiceGrpc.MasterServiceStub asyncStub = MasterServiceGrpc.newStub(masterChannel);
        WriteReq writeReq = WriteReq.newBuilder().setKey(key).setVal(value).build();
        asyncStub.writeMsg(writeReq, new StreamObserver<WriteResp>() {

            @Override
            public void onNext(WriteResp resp) {
                logger.info(String.format("RPC %d: Worker[%d] has done", resp.getStatus(), resp.getReceiver()));

            }

            @Override
            public void onError(Throwable t) {
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {

                finishLatch.countDown();
            }
        });
    }

    void readSync(String key, String mode, CountDownLatch finishLatch) {
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(masterChannel);
        ReadReq readReq = ReadReq.newBuilder().setKey(key).setMode(mode).build();
        ReadResp resp = stub.readMsg(readReq);
        finishLatch.countDown();
    }

    void writeSync(String key, String value, String mode, CountDownLatch finishLatch) {
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(masterChannel);
        WriteReq writeReq = WriteReq.newBuilder().setKey(key).setVal(value).setMode(mode).build();
        WriteResp resp = stub.writeMsg(writeReq);
        finishLatch.countDown();
    }

    public void closeMasterChannel() throws InterruptedException {
        this.masterChannel.shutdown();
    }

    public static void main(String[] args) throws InterruptedException {
        KVClient client = new KVClient(args[0], Integer.parseInt(args[1]));
        final CountDownLatch finishLatch = new CountDownLatch(client.reqList.size());

        /*
         * The code below uses asynchronous calls, which means all request are sent to Master at almost the same time.
         * This will exhaust the hardware resources if the concurrency is too large.
         *
//        for (ReqContent req : client.reqList) {
//            Thread.sleep(new Random().nextInt(2) * 1000);
//            logger.info(String.format("%s:%s:%s:%s", req.getAct(), req.getKey(), req.getVal(), req.getOpt()));
//            if (req.getAct() == 1) {
//                client.write(req.getKey(), req.getVal(), finishLatch);
//            }
//        }
         */

        /*
         * All threads in the pool are running concurrently. A synchronous call is wrapped in each thread.
         * Threads are re-usable in the pool. If one request is sent to Master, another one is picked up.
         * There are at most as many requests as the pool size to be sent to Master.
         * So, the concurrency is limited by the pool size., which means it won't exhaust the hardware resources.
         */
        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        CompletionService<Integer> service = new ExecutorCompletionService<>(threadPool);
        for (ReqContent req : client.reqList) {
            logger.info(String.format("%s:%s:%s:%s", req.getAct(), req.getKey(), req.getVal(), req.getOpt()));
            if (req.getAct() == 0) {
                service.submit(() -> {
                    client.readSync(req.getKey(), req.getOpt(), finishLatch);
                    return 0;
                });
            } else if (req.getAct() == 1) {
                service.submit(() -> {
                    client.writeSync(req.getKey(), req.getVal(), req.getOpt(), finishLatch);
                    return 0;
                });
            }
        }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.MINUTES)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
            logger.warning("The client can not finish within 1 minutes");
        }

        client.closeMasterChannel();
    }
}
