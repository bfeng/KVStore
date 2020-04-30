package kvstore.consistency.tasks;

import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.servers.AckReq;
import kvstore.servers.AckResp;
import kvstore.servers.Worker;
import kvstore.servers.WorkerServiceGrpc.WorkerServiceBlockingStub;

public class BcastAckTask extends TaskEntry<Timestamp> {
    private int senderId;
    public int id;
    private WorkerServiceBlockingStub[] workerStubs;

    /**
     * @param localClock the clock of the message to acknowledge
     * @param id         the id of the message to acknowledge
     * @param senderId   the senderId who sends the ack
     */
    public BcastAckTask(Timestamp ts, int senderId, WorkerServiceBlockingStub[] workerStubs) {
        super(ts);
        this.workerStubs = workerStubs;
        this.senderId = senderId;
    }

    /**
     * Acknowledgement back to other workers for the specified message
     */
    @Override
    public void run() {
        /* Send acks including the self */
        if (this.ts instanceof ScalarTimestamp) {
            int localClock = ((ScalarTimestamp) (this.ts)).localClock;
            int id = ((ScalarTimestamp) (this.ts)).id;

            for (int i = 0; i < this.workerStubs.length; i++) {
                AckReq request = AckReq.newBuilder().setClock(localClock).setId(id).setReceiver(i).setSender(senderId)
                        .build();
                AckResp resp = this.workerStubs[i].handleAck(request);

                /* For debugging */
                // Worker.logger.info(String.format("Worker[%d] --ACK_Message[%d][%d]--> Worker[%d]", senderId,
                //         request.getClock(), request.getId(), i));
            }
        }
    }

    @Override
    public String getTaskId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int compareTo(TaskEntry<Timestamp> o) {
        // TODO Auto-generated method stub
        return 0;
    }

}