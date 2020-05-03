package kvstore.consistency.tasks;

import kvstore.common.WriteReq;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.BcastResp;
import kvstore.servers.WorkerServiceGrpc.WorkerServiceBlockingStub;
import kvstore.servers.WriteReqBcast;

public class BcastWriteTask<T extends Timestamp> extends TaskEntry<T> {
    public int workerId;
    private WorkerServiceBlockingStub[] workerStubs;
    private WriteReq req;

    public BcastWriteTask(T ts, int workerId, WriteReq req, WorkerServiceBlockingStub[] workerStubs) {
        super(ts);
        this.req = req;
        this.workerStubs = workerStubs;
        this.workerId = workerId;
    }

    public void setTimestamp(T currentTs) {
        this.ts = currentTs;
        return;
    }

    private void bcastWriteReq() throws InterruptedException {
        if (req.getMode().equals("Sequential")) {
            int clock = ((ScalarTimestamp) (ts)).localClock;
            for (int i = 0; i < this.workerStubs.length; i++) {
                WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(this.workerId).setReceiver(i)
                        .setRequest(this.req).setSenderClock(clock).setMode(this.req.getMode()).build();
                BcastResp resp = workerStubs[i].handleBcastWrite(writeReqBcast);
                // Worker.logger.info(String.format("<<<Worker[%d]--broadcastMessage[%d][%d]-->Worker[%d]>>>",
                // workerId,
                // writeReqBcast.getSenderClock(), writeReqBcast.getSender(),
                // resp.getReceiver()));
            }
        } else if (req.getMode().equals("Causal")) {
            VectorTimestamp vts = (VectorTimestamp) ts;
            // Worker.logger.info(String.format("Vts to multicast: %s",
            // vts.value.toString()));
            for (int i = 0; i < this.workerStubs.length; i++) {
                if (i != this.workerId) { /* Don not send to itself in this case */

                    WriteReqBcast writeReqBcast = WriteReqBcast.newBuilder().setSender(this.workerId).setReceiver(i)
                            .setRequest(this.req).setMode(req.getMode()).addAllVts(vts.value).build();

                    BcastResp resp = workerStubs[i].handleBcastWrite(writeReqBcast);
                    // logger.info(String.format("<<<Worker[%d]
                    // --broadcastMessage[%d][%d]-->Worker[%d]>>>", workerId,
                    // writeReqBcast.getSenderClock(), writeReqBcast.getSender(),
                    // resp.getReceiver()));
                }

            }
        }

    }

    @Override
    public void run() {

        try {
            // Thread.sleep(new Random().nextInt(2 * 1000));
            bcastWriteReq();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String getTaskId() {
        return null;
    }

    @Override
    public int compareTo(TaskEntry<T> o) {
        return this.ts.minus(o.ts);
    }

}