package kvstore.consistency;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import kvstore.common.WriteReq;

public class WriteTask extends taskEntry {
    private static final Logger logger = Logger.getLogger(WriteTask.class.getName());
    private WriteReq writeReq;
    private Map<String, String> dataStore;

    public WriteTask(AtomicInteger globalClock, int localClock, int id, int acksNum, WriteReq writeReq,
            Map<String, String> dataStore) {
        super(globalClock, localClock, id, acksNum);
        this.writeReq = writeReq;
        this.dataStore = dataStore;
    }

    /**
     * Check if allow running by checking if received all replies
     */
    public boolean ifAllowDeliver() {
        if (this.acksNum == 0)
            return true;
        return false;
    }

    public int getAckNum() {
        return acksNum;
    }

    @Override
    public void run() {
        dataStore.put(writeReq.getKey(), writeReq.getVal());
        logger.info(String.format("<<<<<<<<<<<Worker[%d][%d]: write ,key=%s,val=%s>>>>>>>>>>>", id, localClock,
                writeReq.getKey(), writeReq.getVal()));
    }
}