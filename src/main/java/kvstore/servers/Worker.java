package kvstore.servers;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.consistency.BcastAckTask;
import kvstore.consistency.Scheduler;
import kvstore.consistency.WriteTask;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private Scheduler sche;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
        this.sche = new Scheduler(getWorkerConf().size());
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port).addService(new WorkerService(this)).build().start();
        // logger.info(String.format("Worker[%d] started, listening on %d", workerId,
        // port));

        /* Start the scheduler */
        (new Thread(this.sche)).start();

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
        // logger.info(String.format("RPC: %d: Worker[%d] is registered with Master",
        // response.getStatus(), workerId));
        channel.shutdown();
    }

    /**
     * Propagate the write rquest to other workers
     *
     * @TODO: What if acks are not return
     */
    private void bcastWriteReq(WriteReq req, int clock) {
        for (int i = 0; i < getWorkerConf().size(); i++) {
            ServerConfiguration sc = getWorkerConf().get(i);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);
            WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i).setRequest(req)
                    .setSenderClock(clock).build();
            BcastResp resp = stub.handleBcastWrite(writeReqBcast);
            // logger.info(String.format("<<<Worker[%d] --broadcast Message[%d][%d]-->
            // Worker[%d]>>>", workerId,
            // writeReqBcast.getSenderClock(), writeReqBcast.getSender(),
            // resp.getReceiver()));
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
        private AtomicInteger globalClock; /* A server would maintain a logic clock */

        WorkerService(Worker worker) {
            this.worker = worker;
            this.globalClock = new AtomicInteger(0);
        }

        /**
         * When receiving a write request, the worker broadcasts the message to other
         * workers
         *
         * @TODO: Currently the worker doesn't return status to the master
         */
        @Override
        public void handleWrite(WriteReq request, StreamObserver<WriteResp> responseObserver) {
            /* Update the clock for issuing a write operation */
            globalClock.incrementAndGet();

            /* Broadcast the issued write operation */
            /* Update the clock for broadcasting the message */
            worker.bcastWriteReq(request, globalClock.incrementAndGet());

            /* Return */
            WriteResp resp = WriteResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        @Override
        public void handleBcastWrite(WriteReqBcast request, StreamObserver<BcastResp> responseObserver) {
            /* Update clock by comparing with the sender */
            globalClock.set(Math.max(globalClock.get(), request.getSenderClock()));
            /* Update clock for having received the broadcasted message */
            globalClock.incrementAndGet();

            try {
                /* Create a new write task */
                WriteTask newWriteTASK = new WriteTask(globalClock, request.getSenderClock(), request.getSender(),
                        request.getRequest(), worker.dataStore);

                /* Attach a bcastAckTask for this write task */
                newWriteTASK.setBcastAckTask(new BcastAckTask(globalClock, request.getSenderClock(),
                        request.getSender(), worker.workerId, worker.getWorkerConf()));

                /* Enqueue a new write task */
                worker.sche.addTask(newWriteTASK);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            /* Return */
            BcastResp resp = BcastResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        @Override
        public void handleAck(AckReq request, StreamObserver<AckResp> responseObserver) {

            /* Update clock compared with the sender */
            /* Update the clock for having received the acknowledgement */
            globalClock.set(Math.max(globalClock.get(), request.getSenderClock()));
            globalClock.incrementAndGet();

            /* Updata the acks number for the specified message */
            globalClock.incrementAndGet(); /* Update the clock for having updated the acknowledgement */

            /* The below is for debugging */
            // Boolean[] ackArr = worker.sche.updateAck(request);

            // logger.info(String.format("<<<Worker[%d] <--ACK_Message[%d][%d]--Worker[%d]
            // \n Current ack array: %s >>>",
            // worker.workerId, request.getClock(), request.getId(), request.getSender(),
            // Arrays.toString(ackArr)));

            /* Return */
            AckResp resp = AckResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
