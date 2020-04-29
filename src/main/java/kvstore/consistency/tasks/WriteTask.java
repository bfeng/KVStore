package kvstore.consistency.tasks;

import java.util.Map;

import kvstore.common.WriteReq;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.servers.Worker;
/**
 * TaskEntry<ScalarTimestamp>
 *      ↓
 * WritTask<ScalarTimestamp> Acceptable for sequentialScheduler
*/
public class WriteTask<T extends Timestamp> extends TaskEntry<T> {
    private WriteReq writeReq;
    private Map<String, String> dataStore;
    private BcastAckTask bcastAckTask;
    private int bcastCount;
    public int localClock;
    public int id;

    /**
     * A runable class for implementing writing to the data store
     * 
     * @param localClock the clock when issues the write task
     * @param id         the id of the woker which creates the task
     * @param writeReq   the write reqest sent by the master
     * @param dataStore  the reference to the data store of the current worker
     */
    public WriteTask(T ts, WriteReq writeReq, Map<String, String> dataStore) {
        super(ts);
        this.writeReq = writeReq;
        this.dataStore = dataStore;
        this.bcastAckTask = null;
        this.bcastCount = 0;
    }

    /**
     * Set a broadcast acknowledgement task for the write task. The broadcast
     * acknowledgement task will be ran by the scheduler when the current write task
     * is at the top of the task queue
     * 
     * @param bcastAckTask
     */
    public void setBcastAckTask(BcastAckTask bcastAckTask) {
        this.bcastAckTask = bcastAckTask;
        return;
    }

    /**
     * Broadcast the acks to all other workers including self for this message
     */
    public void bcastAcks() {
        this.bcastCount++;
        if (bcastAckTask != null) {
            (new Thread(bcastAckTask)).start();
        } else {
            Worker.logger.warning("No bcast task for this message");
            return;
        }
    }

    public void abortBcastAckTask() {
        this.bcastAckTask = null;
    }

    /**
     * Get how many times have broadcasted acks
     * 
     * @return the bcastCount
     */
    public int getBcastCount() {
        return bcastCount;
    }

    @Override
    public void run() {
        /* Write to the data store */
        // dataStore.put(writeReq.getKey(), writeReq.getVal());

        /* For debugging */
        // logger.info(String.format("<<<<<<<<<<<Deliver
        // Message[%d][%d]:key=%s,val=%s>>>>>>>>>>>", localClock, id,
        // writeReq.getKey(), writeReq.getVal()));
    }

    /**
     * Get the task id (i.e. logictime + id)
     */
    @Override
    public String getTaskId() {
        return this.ts.genKey();
    }

    /**
     * Compare the timestamp
    */
    @Override
    public int compareTo(TaskEntry<T> other) {
        return this.ts.minus(((WriteTask<T>) other).ts);
    }
}