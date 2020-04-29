package kvstore.servers;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kvstore.common.WriteReq;
import kvstore.common.WriteResp;
import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.schedulers.CausalScheduler;
import kvstore.consistency.schedulers.SequentialScheduler;
import kvstore.consistency.tasks.BcastAckTask;
import kvstore.consistency.tasks.WriteTask;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.consistency.timestamps.VectorTimestamp;

public class Worker extends ServerBase {
    public static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;
    private final Map<String, String> dataStore = new ConcurrentHashMap<>();
    private SequentialScheduler seqSche;
    private CausalScheduler causalSche;
    private ManagedChannel masterChannel;
    private ManagedChannel[] workerChannels;
    private WorkerServiceGrpc.WorkerServiceBlockingStub[] workerStubs;
    private WorkerServiceGrpc.WorkerServiceBlockingStub masterStub;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
        initStubs();
        initLogger();
        this.seqSche = new SequentialScheduler(new ScalarTimestamp(0, workerId), getWorkerConf().size());
        this.causalSche = new CausalScheduler(new VectorTimestamp(), getWorkerConf().size());
    }

    private void initLogger() throws SecurityException, IOException {
        /* Configure the logger to outpu the log into files */
        File logDir = new File("./logs/");
        if (!logDir.exists())
            logDir.mkdir();
        FileHandler fh = new FileHandler("logs/worker_" + workerId + ".log");
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        logger.addHandler(fh);
    }

    /**
     * Initialize channesl to other workers
     */
    private void initStubs() {
        /*
         * Create an array of channels, and the index is corresponded with the worker id
         */
        this.masterChannel = ManagedChannelBuilder.forAddress(this.getMasterConf().ip, this.getMasterConf().port)
                .usePlaintext().build();
        this.masterStub = WorkerServiceGrpc.newBlockingStub(this.masterChannel);

        this.workerChannels = new ManagedChannel[getWorkerConf().size()];
        this.workerStubs = new WorkerServiceGrpc.WorkerServiceBlockingStub[getWorkerConf().size()];

        for (int i = 0; i < getWorkerConf().size(); i++) {
            ServerConfiguration sc = getWorkerConf().get(i);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
            this.workerStubs[i] = WorkerServiceGrpc.newBlockingStub(channel);
            this.workerChannels[i] = channel;
        }

    }

    private void shutdownAllChannels() {
        this.masterChannel.shutdownNow();
        for (int i = 0; i < getWorkerConf().size(); i++) {
            this.workerChannels[i].shutdownNow();
        }
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port).addService(new WorkerService(this)).build().start();
        // logger.info(String.format("Worker[%d] started, listening on %d", workerId,
        // port));

        /* Start the scheduler */
        (new Thread(this.seqSche)).start();
        (new Thread(this.causalSche)).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown
            // hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Worker.this.shutdownAllChannels();
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
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(this.masterChannel);
        WorkerStatus status = WorkerStatus.newBuilder().setWorkerId(workerId).setStatus(statusCode.getValue()).build();
        MasterResponse response = stub.reportStatus(status);
        logger.info(String.format("RPC: %d: Worker[%d] is registered with Master", response.getStatus(), workerId));
    }

    /**
     * Propagate the write rquest to other workers
     * 
     * @throws InterruptedException
     * 
     */
    private void bcastWriteReq(WriteReq req, Timestamp ts) throws InterruptedException {
        if (req.getMode().equals("Sequential")) {
            int clock = ((ScalarTimestamp) (ts)).localClock;
            for (int i = 0; i < getWorkerConf().size(); i++) {
                WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i)
                        .setRequest(req).setSenderClock(clock).setMode(req.getMode()).build();
                BcastResp resp = workerStubs[i].handleBcastWrite(writeReqBcast);
                // logger.info(String.format("<<<Worker[%d]
                // --broadcastMessage[%d][%d]-->Worker[%d]>>>", workerId,
                // writeReqBcast.getSenderClock(), writeReqBcast.getSender(),
                // resp.getReceiver()));
            }
        } else if (req.getMode().equals("Causal")) {
            for (int i = 0; i < getWorkerConf().size(); i++) {
                VectorTimestamp vts = (VectorTimestamp) ts;

                // WriteReqBcast writeReqBcast =
                // WriteReqBcast.newBuilder().setSender(workerId).setReceiver(i)
                // .setRequest(req).setMode(req.getMode()).addAllVts(this.causalSche.).build();
                // BcastResp resp = workerStubs[i].handleBcastWrite(writeReqBcast);
                // logger.info(String.format("<<<Worker[%d]
                // --broadcastMessage[%d][%d]-->Worker[%d]>>>", workerId,
                // writeReqBcast.getSenderClock(), writeReqBcast.getSender(),
                // resp.getReceiver()));
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

        WorkerService(Worker worker) {
            this.worker = worker;
        }

        /**
         * When receiving a write request, the worker broadcasts the message to other
         * workers
         *
         * @TODO: Currently the worker doesn't return status to the master
         */
        @Override
        public void handleWrite(WriteReq request, StreamObserver<WriteResp> responseObserver) {
            try {
                /* Update the clock for issuing a write operation */
                /* Broadcast the issued write operation */
                if (request.getMode().equals("Sequential")) {
                    worker.bcastWriteReq(request, worker.seqSche.incrementAndGetTimeStamp());
                } else if (request.getMode().equals("Causal")) {
                    // worker.bcastWriteReq(request, worker.causalSche.incrementAndGetTimeStamp());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            /* Return */
            WriteResp resp = WriteResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        @Override
        public void handleBcastWrite(WriteReqBcast request, StreamObserver<BcastResp> responseObserver) {
            switch (request.getMode()) {
                case "Sequential":
                    /* Update clock by comparing with the sender */
                    /* Update clock for having received the broadcasted message */
                    worker.seqSche.updateAndIncrementTimeStamp(request.getSenderClock());

                    /* Create a new write task */
                    ScalarTimestamp ts = new ScalarTimestamp(request.getSenderClock(), request.getSender());
                    WriteTask<ScalarTimestamp> newWriteTASK = new WriteTask<ScalarTimestamp>(ts, request.getRequest(),
                            worker.dataStore);

                    /* Attach a bcastAckTask for this write task */
                    newWriteTASK.setBcastAckTask(new BcastAckTask(ts, worker.workerId, worker.workerStubs));

                    /* Enqueue a new write task */
                    worker.seqSche.addTask(newWriteTASK);
                    break;

                case "Causal":
                    /* Create a new write task */
                    // SeqWriteTask newWriteTASK = new SeqWriteTask(request.getSenderClock(),
                    // request.getSender(),
                    // request.getRequest(), worker.dataStore);
                    break;

                default:
                    break;
            }

            /* Return */
            BcastResp resp = BcastResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        /**
         * Handle acks by updating the current acks map
         */
        @Override
        public void handleAck(AckReq request, StreamObserver<AckResp> responseObserver) {

            /* Update clock compared with the sender */
            /* Update the clock for having updated the acknowledgement */
            worker.seqSche.updateAndIncrementTimeStamp(request.getSenderClock());

            /* Updata the acks number for the specified message */
            ScalarTimestamp ts = new ScalarTimestamp(request.getClock(), request.getId());
            Boolean[] ackArr = ((SequentialScheduler) (worker.seqSche)).updateAck(ts, request.getSender());

            /* The below is for debugging */
            // logger.info(String.format("<<<Worker[%d] <--ACK_Message[%d][%d]--Worker[%d]\n
            // Current ack array: %s >>>",
            // worker.workerId, request.getClock(), request.getId(), request.getSender(),
            // Arrays.toString(ackArr)));

            /* Return */
            AckResp resp = AckResp.newBuilder().setReceiver(worker.workerId).setStatus(0).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }
}
