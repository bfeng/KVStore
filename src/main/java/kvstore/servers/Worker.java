package kvstore.servers;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class Worker extends ServerBase {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final int workerId;
    private final int port;

    public Worker(String configuration, int workerId) throws IOException {
        super(configuration);
        this.workerId = workerId;
        this.port = getWorkerConf().get(workerId).port;
    }


    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new WorkerService(this))
                .build()
                .start();
        logger.info(String.format("Worker[%d] started, listening on %d", workerId, port));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
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
                .usePlaintext()
                .build();
        MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);
        WorkerStatus status = WorkerStatus.newBuilder()
                .setWorkerId(workerId)
                .setStatus(statusCode.getValue())
                .build();
        MasterResponse response = stub.reportStatus(status);
        logger.info(String.format("RPC: %d: Worker[%d] is registered with Master", response.getStatus(), workerId));
        channel.shutdown();
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
    }
}
