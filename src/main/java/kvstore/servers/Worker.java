package kvstore.servers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.consistency.BcastAckTask;
import kvstore.consistency.Scheduler;
import kvstore.consistency.WriteTask;
import kvstore.consistency.taskEntry;
import kvstore.servers.AckReq;
import kvstore.servers.AckResp;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private Map<String, String> dataStore = new ConcurrentHashMap<>();
    private Scheduler sche;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
        this.sche = new Scheduler(16);
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
     * @TODO: What if acks are not return
     */
    private void bcastWriteReq(WriteReq req, int clock) {
        for (int i = 0; i < getWorkerConf().size(); i++) {
            if (i != workerId) { /* Here it does not send to the worker self */
                ServerConfiguration sc = getWorkerConf().get(i);
                ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
                WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);
                WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i)
                        .setRequest(req).setSenderClock(clock).build();
                BcastResp resp = stub.handleBcastWrite(writeReqBcast);
                logger.info(String.format("Worker[%d] asks ack from Worker[%d]", workerId, resp.getReceiver()));
                channel.shutdown();
            }
        }
    }

    /**
     * Propagate the acknowledgements to other workers
     */
    private void bcastAcks(WriteReqBcast writeReqBcast, int clock) {
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

        @Override
        public void handleWrite(WriteReq request, StreamObserver<WriteResp> responseObserver) {
            /* Update the clock */
            globalClock.incrementAndGet();

            /* Add the task to the priority queue for scheduling */
            try {
                worker.sche.addTask(new WriteTask(globalClock, globalClock.get(), worker.workerId,
                        worker.getWorkerConf().size(), request, worker.dataStore));
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            /* Brodcast the write operation */
            worker.bcastWriteReq(request, globalClock.get());

            /* Construbt return message to the master */
            WriteResp resp = WriteResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        /**
         * Return the acknowledgement immediately and enqueue the received write
         * operation
         */
        @Override
        public void handleBcastWrite(WriteReqBcast request, StreamObserver<BcastResp> responseObserver) {
            /* Update clock compared with the sender */
            globalClock.set(Math.max(globalClock.get(), request.getSenderClock()));

            try {
                /* Enqueue a new write task */
                worker.sche.addTask(new WriteTask(globalClock, request.getSenderClock(), request.getSender(),
                        worker.getWorkerConf().size(), request.getRequest(), worker.dataStore));

                /* Enqueue a new ackowledgement task as soon as received a message */
                (new Thread(new BcastAckTask(globalClock, request.getSenderClock(), request.getSender(), 0,
                        worker.workerId, worker.getWorkerConf()))).start();

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
            logger.info(String.format("Receiv ack from Worker[%d] for Clock[%d], Id[%d]", request.getSender(),
                    request.getClock(), request.getId()));
            AckResp resp = AckResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
