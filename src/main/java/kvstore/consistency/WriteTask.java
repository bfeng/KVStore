package kvstore.consistency;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import kvstore.common.WriteReq;

public class WriteTask extends taskEntry {
    private static final Logger logger = Logger.getLogger(WriteTask.class.getName());
    private WriteReq writeReq;
    private Map<String, String> dataStore;
    private BcastAckTask bcastAckTask;
    private int bcastCount;

    public WriteTask(AtomicInteger globalClock, int localClock, int id, WriteReq writeReq,
            Map<String, String> dataStore) {
        super(globalClock, localClock, id);
        this.writeReq = writeReq;
        this.dataStore = dataStore;
        this.bcastAckTask = null;
        this.bcastCount = 0;
    }

    public void setBcastAckTask(BcastAckTask bcastAckTask) {
        this.bcastAckTask = bcastAckTask;
        return;
    }

    /**
     * Brodcast the acks for this message
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
     * @return the bcastCount
     */
    public int getBcastCount() {
        return bcastCount;
    }

    @Override
    public void run() {
        globalClock.incrementAndGet(); /* Update the clock for having wrote to the data store */
        dataStore.put(writeReq.getKey(), writeReq.getVal());
        // logger.info(String.format("<<<<<<<<<<<Deliver Message[%d][%d]: key=%s,val=%s>>>>>>>>>>>", localClock, id,
        //         writeReq.getKey(), writeReq.getVal()));
    }
}