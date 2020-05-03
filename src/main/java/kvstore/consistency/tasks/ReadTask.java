package kvstore.consistency.tasks;

import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.servers.ReadReqBcast;
import kvstore.servers.Worker;

import java.util.Map;

public class ReadTask<T extends Timestamp> extends TaskEntry<T> {
    public ReadReqBcast readReqBcast;
    private Map<String, String> dataStore;
    private BcastAckTask bcastAckTask;
    private int bcastCount;
    public int localClock;
    public int id;

    public ReadTask(T ts, ReadReqBcast readReqBcast, Map<String, String> dataStore) {
        super(ts);
        this.readReqBcast = readReqBcast;
        this.dataStore = dataStore;
        this.bcastAckTask = null;
        this.bcastCount = 0;
    }

    public void setBcastAckTask(BcastAckTask bcastAckTask) {
        this.bcastAckTask = bcastAckTask;
    }

    public void bcastAcks() {
        this.bcastCount++;
        if (bcastAckTask != null) {
            (new Thread(bcastAckTask)).start();
        } else {
            Worker.logger.warning("No bcast task for this message");
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
        return this.ts.minus(other.ts);
    }

    @Override
    public void run() {

    }
}
