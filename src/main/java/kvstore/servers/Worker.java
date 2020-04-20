package kvstore.servers;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.consistency.SeqScheduler;
import kvstore.consistency.SeqScheduler.taskEntry;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private Map<String, String> dataStore = new ConcurrentHashMap<>();
    private SeqScheduler sche;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
        this.sche = new SeqScheduler(16);
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port).addService(new WorkerService(this)).build().start();
        logger.info(String.format("Worker[%d] started, listening on %d", workerId, port));
        /* Start the scheduler */
        Thread scheThread = new Thread(this.sche);
        scheThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown
            // hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Worker.this.reportStatusToMaster(ServerStatus.DOWN);
                Worker.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
        this.reportStatusToMaster(ServerStatus.READY);
    }

    /**
     * Tell Master that I'm ready!
     */
    private void reportStatusToMaster(ServerStatus statusCode) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(this.getMasterConf().ip, this.getMasterConf().port)
                .usePlaintext().build();
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
        WorkerStatus status = WorkerStatus.newBuilder().setWorkerId(workerId).setStatus(statusCode.getValue()).build();
        MasterResponse response = stub.reportStatus(status);
        logger.info(String.format("RPC: %d: Worker[%d] is registered with Master", response.getStatus(), workerId));
        channel.shutdown();
    }

    /**
     * Propagate the write rquest to other workers
     * 
     * @TODO: What if acks are not return?
     */
    private void bcastWriteReq(WriteReq req, int logicTime) {
        for (int i = 0; i < getWorkerConf().size(); i++) {
            if (i != workerId) { /* Do not send to the worker self */
                ServerConfiguration sc = getWorkerConf().get(i);
                ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
                WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);
                WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i)
                        .setRequest(req).setSenderClock(logicTime).build();
                BcastResp resp = stub.handleBcastWrite(writeReqBcast);
                // logger.info(
                // String.format("RPC %d: Worker[%d] has done this replica", resp.getStatus(),
                // resp.getReceiver()));
                channel.shutdown();
            }
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Worker server = new Worker(args[0], Integer.parseInt(args[1]));
        server.start();
        server.blockUntilShutdown();
    }

    static class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {
        private final Worker worker;
        private int logicTime;

        WorkerService(Worker worker) {
            this.worker = worker;
            this.logicTime = 0;
        }

        @Override
        public void handleWrite(WriteReq request, StreamObserver<WriteResp> responseObserver) {
            logicTime++; /* Update for the write event */
            int tmpTime = logicTime; /* Save the current logic time for logging */
            taskEntry t = new taskEntry(-1, -1);
            try {
                t = worker.sche.addTask(logicTime, worker.workerId);
                t.sem.acquire();

            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }

            /* Propagate the write operations */
            logicTime++; /* Update for the brodcast */
            worker.bcastWriteReq(request, logicTime);

            /* Write to the data store */
            worker.dataStore.put(request.getKey(), request.getVal());
            logger.info(String.format("<<<<<<<<<<<Worker[%d][%d]: write , key=%s,val=%s>>>>>>>>>>>", worker.workerId,
                    tmpTime, request.getKey(), request.getVal()));

            /* Release lock and coundown to let scheduler continue */
            t.finisLatch.countDown();

            /* Construbt return message to the master */
            WriteResp resp = WriteResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        /**
         * Return the acknowledgement imeediately and enqueue the received write
         * operation
         * 
         * @TODO: To add the logic time updates
         */
        @Override
        public void handleBcastWrite(WriteReqBcast request, StreamObserver<BcastResp> responseObserver) {
            logicTime = Math.max(request.getSenderClock(), logicTime); /* Update logic time */
            Thread receivedWrite = new Thread() {
                public void run() {
                    logicTime++; /*  */
                    int tmpTime = logicTime;
                    taskEntry t = new taskEntry(-1, -1);
                    try {
                        t = worker.sche.addTask(logicTime, worker.workerId);
                        t.sem.acquire();

                    } catch (InterruptedException e) {
                        logger.info(e.getMessage());
                    }

                    logicTime++; /* Increase the logic time by 1 when deliver the write operation */
                    worker.dataStore.put(request.getRequest().getKey(), request.getRequest().getVal());
                    logger.info(String.format(
                            "Worker[%d]: replica received from Worker[%d], <<<<<<<<<write key=%s, val=%s>>>>>>>>>",
                            worker.workerId, request.getSender(), request.getRequest().getKey(),
                            request.getRequest().getVal()));

                    /* Release lock and coundown to let scheduler continue */
                    t.finisLatch.countDown();
                }
            };
            /*
             * @TODO: What if return before adding the new taks entry? the clocl will be
             * messed up
             */
            receivedWrite.start();

            /* Return */
            BcastResp resp = BcastResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
