package kvstore.consistency;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvstore.servers.AckReq;
import kvstore.servers.AckResp;
import kvstore.servers.Worker;
import kvstore.servers.WorkerServiceGrpc;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class BcastAckTask extends TaskEntry {
    private static final Logger logger = Logger.getLogger(BcastAckTask.class.getName());
    private List<Worker.ServerConfiguration> workerConf;
    private int senderId;

    /**
     * @param localClock the clock of the message to acknowledge
     * @param id         the id of the message to acknowledge
     * @param senderId   the senderId who sends the ack
     */
    public BcastAckTask(int localClock, int id, int senderId, List<Worker.ServerConfiguration> workerConf) {
        super(localClock, id);
        this.workerConf = workerConf;
        this.senderId = senderId;
    }

    /**
     * Acknowledgement back to other workers for the specified message
     */
    @Override
    public void run() {
        /* Send acks including the self */
        for (int i = 0; i < workerConf.size(); i++) {
            Worker.ServerConfiguration sc = this.workerConf.get(i);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(sc.ip, sc.port).usePlaintext().build();
            WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);

            AckReq request = AckReq.newBuilder().setClock(localClock).setId(id).setReceiver(i).setSender(senderId)
                    .build();
            AckResp resp = stub.handleAck(request);

            /* For debugging */
            logger.info(String.format("Worker[%d] --ACK_Message[%d][%d]--> Worker[%d]", senderId, request.getClock(),
                    request.getId(), i));
            channel.shutdown();
        }
    }

}