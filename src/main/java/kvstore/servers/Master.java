package kvstore.servers;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Master extends ServerBase {
    private static final Logger logger = Logger.getLogger(Master.class.getName());

    private final int port;

    private Map<Integer, Integer> clusterStatus;

    public Master(String configuration) throws IOException {
        super(configuration);
        this.port = getMasterConf().port;
        this.clusterStatus = new HashMap<>();
    }

    private synchronized void updateStatus(int workerId, int code) {
        this.clusterStatus.put(workerId, code);
        logger.info(String.format("Master: Worker[%d] status code is %d", workerId, code));
    }

    @Override
    protected void start() throws IOException {
        /* The port on which the server should run */
        server = ServerBuilder.forPort(port)
                .addService(new MasterService(this))
                .build()
                .start();
        logger.info("Master started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                Master.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
        }));
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Master server = new Master(args[0]);
        server.start();
        server.blockUntilShutdown();
    }

    private static class MasterService extends MasterServiceGrpc.MasterServiceImplBase {
        private final Master master;

        MasterService(Master master) {
            this.master = master;
        }

        @Override
        public void reportStatus(WorkerStatus request, StreamObserver<MasterResponse> responseObserver) {
            int workerId = request.getWorkerId();
            int status = request.getStatus();
            master.updateStatus(workerId, status);
            MasterResponse response = MasterResponse.newBuilder().setStatus(0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}