package kvstore.servers;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.consistency.SeqScheduler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private Map<String, String> dataStore = new ConcurrentHashMap<>();

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port).addService(new WorkerService(this)).build().start();
        logger.info(String.format("Worker[%d] started, listening on %d", workerId, port));
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
     */
    private void bcastWriteReq(WriteReq req) {
        for (int i = 0; i < getWorkerConf().size(); i++) {
            ServerConfiguration sc = getWorkerConf().get(i);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);
            WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i).setRequest(req)
                    .build();
            BcastResp resp = stub.handleBcastWrite(writeReqBcast);
            logger.info(
                    String.format("RPC %d: Worker[%d] has done this replica", resp.getStatus(), resp.getReceiver()));
            channel.shutdown();
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
        private SeqScheduler sche;

        WorkerService(Worker worker) {
            this.worker = worker;
            this.logicTime = 0;
            /*
             * Initiating the scheduler with the initia capacity 16 which will automaticlly
             * grow
             */
            sche = new SeqScheduler(16);
        }

        @Override
        public void handleWrite(WriteReq request, StreamObserver<WriteResp> responseObserver) {
            logicTime++; /* Increase the logic time by 1 */

            int tmpTime = logicTime;
            try {
                this.sche.seqWait(this.logicTime, worker.workerId); /* Wait */
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }

            /* Log message when writing to the data store */
            logger.info(String.format("<<<<<<<<<<<Worker[%d], Clock[%d]: write , key=%s, val=%s>>>>>>>>>>>",
                    worker.workerId, tmpTime, request.getKey(), request.getVal()));

            /* Write to the data store */
            worker.dataStore.put(request.getKey(), request.getVal());

            /* Resume the next top write rquest */
            try {
                this.sche.seqResume();
            } catch (InterruptedException e) {
                logger.info(String.format("sqResume Failed %s", e.getMessage()));
            }

            /* Construbt return message to the master */
            WriteResp resp = WriteResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        @Override
        public void handleBcastWrite(WriteReqBcast request, StreamObserver<BcastResp> responseObserver) {
            logger.info(
                    String.format("Worker[%d]: replica received from Worker[%d] write key=%s, val=%s", worker.workerId,
                            request.getSender(), request.getRequest().getKey(), request.getRequest().getVal()));
            if (request.getSender() != worker.workerId) {
                worker.dataStore.put(request.getRequest().getKey(), request.getRequest().getVal());
            }
            BcastResp resp = BcastResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
