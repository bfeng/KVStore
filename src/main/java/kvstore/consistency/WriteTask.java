package kvstore.consistency;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import kvstore.common.WriteReq;

public class WriteTask extends TaskEntry {
    private static final Logger logger = Logger.getLogger(WriteTask.class.getName());
    private WriteReq writeReq;
    private Map<String, String> dataStore;
    private BcastAckTask bcastAckTask;
    private int bcastCount;

    /**
     * A runable class for implementing writing to the data store
     * 
     * @param globalClock the clock of the current woker which creates the task
     * @param localClock  the clock when issues the write task
     * @param id          the id of the woker which creates the task
     * @param writeReq    the write reqest sent by the master
     * @param dataStore   the reference to the data store of the current worker
     */
    public WriteTask(int localClock, int id, WriteReq writeReq, Map<String, String> dataStore) {
        super(localClock, id);
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
            logger.warning("No bcast task for this message");
            return;
        }
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
        dataStore.put(writeReq.getKey(), writeReq.getVal());

        /* For debugging */
        logger.info(String.format("<<<<<<<<<<<Deliver Message[%d][%d]:key=%s,val=%s>>>>>>>>>>>", localClock, id,
                writeReq.getKey(), writeReq.getVal()));
    }
}