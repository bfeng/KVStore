package kvstore.consistency.tasks;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import kvstore.consistency.bases.TaskEntry;
import kvstore.servers.AckReq;
import kvstore.servers.AckResp;
import kvstore.servers.Worker;
import kvstore.servers.WorkerServiceGrpc;
import kvstore.servers.WorkerServiceGrpc.WorkerServiceBlockingStub;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class BcastAckTask extends TaskEntry {
    private int senderId;
    public int localClock;
    public int id;
    private WorkerServiceBlockingStub[] workerStubs;

    /**
     * @param localClock the clock of the message to acknowledge
     * @param id         the id of the message to acknowledge
     * @param senderId   the senderId who sends the ack
     */
    public BcastAckTask(int localClock, int id, int senderId, WorkerServiceBlockingStub[] workerStubs) {
        this.localClock = localClock;
        this.id = id;
        this.workerStubs = workerStubs;
        this.senderId = senderId;
    }

    /**
     * Acknowledgement back to other workers for the specified message
     */
    @Override
    public void run() {
        /* Send acks including the self */
        for (int i = 0; i < this.workerStubs.length; i++) {
            AckReq request = AckReq.newBuilder().setClock(localClock).setId(id).setReceiver(i).setSender(senderId)
                    .build();
            AckResp resp = this.workerStubs[i].handleAck(request);

            /* For debugging */
            Worker.logger.info(String.format("Worker[%d] --ACK_Message[%d][%d]--> Worker[%d]", senderId,
                    request.getClock(), request.getId(), i));
        }
    }

    @Override
    public String getTaskId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int minus(TaskEntry taskEntry) {
        // TODO Auto-generated method stub
        return 0;
    }

}