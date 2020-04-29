package kvstore.consistency.schedulers;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.FileHandler;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.tasks.WriteTask;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.servers.Worker;

/**
 * All incoming operations are enqueue into a priority queue sorted by a
 * customized comparator. The scheduler keeps taking a task from the priority
 * queue starting the task if allowed.
 */
public class SequentialScheduler extends Scheduler {
    private ConcurrentHashMap<String, Boolean[]> acksMap;
    private int ackLimit;
    public int globalClock; /* A scheduler would maintain a logic clock */
    FileHandler fh;

    /**
     * The ackMaps contains all happened acknowledgement for each message. The value
     * of the map is a boolean array. The index of the array corresponding the id of
     * workers. For example, "1.0" : [fasle, fasle, true] means the message "1.0"
     * doesn't receive acknowledgement from the worker 0, 1 but 2.
     *
     * @param ackLimit The number of acknowledgement required for starting a task,
     *                 i.e., delivering a message
     * @throws IOException
     * @throws SecurityException
     */
    public SequentialScheduler(int ackLimit)
            throws SecurityException, IOException {
        /* A hashmap contains all happened acknowledgement */
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(1024);
        this.ackLimit = ackLimit;
        this.globalClock = 0;
    }

    /**
     * Grab a task from the priority queue, running when received all acks
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Thread.sleep(new Random().nextInt(2) * 1000); /* Random dealy. Only for the
                // testing purpose */

                /* Taking a task from the queue. Block when the queue is empty */
                WriteTask task = (WriteTask) tasksQ.take();

                /* For debugging */
                // logger.info(String.format("<<<Run Task %s: Message[%d][%d]>>>",
                // task.getClass().getName(),
                // task.localClock, task.id));

                /* Here it only allows broadcasting once for each message */
                if (task.getBcastCount() == 0)
                    task.bcastAcks();

                /* Deliver the message when all replies received */
                if (ifAllowDeliver(task)) {
                    task.abortBcastAckTask();
                    deleteAckArr(task.getTaskId());

                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                    Worker.logger.info(String.format("<<<Message[%d][%d] Delivered!>>>", ((ScalarTimestamp)(task.ts)).localClock, ((ScalarTimestamp)(task.ts)).id));

                } else {

                    tasksQ.put(task); /* Put back the task for rescheduling */

                    // /* For debugging */
                    // logger.info(String.format("<<<Message[%d][%d] Blocked! Current ack
                    // array:%s>>>", task.localClock,
                    // task.id, Arrays.toString(this.acksMap.get(task.toString()))));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Add a new task and also create the corresponding ackMap item for this message
     * The taskQ and acksMap are safe in the multi-thread environment
     */
    public TaskEntry addTask(TaskEntry newTask) {
        tasksQ.put(newTask); /* Put the taks to the priority queue */
        if (!this.acksMap.containsKey(newTask.getTaskId())) {
            Boolean[] ackArr = new Boolean[ackLimit];
            Arrays.fill(ackArr, false);
            this.acksMap.put(newTask.getTaskId(), ackArr);
        }
        return newTask;
    }

    /**
     * Check if all replies for this message is received
     */
    public synchronized boolean ifAllowDeliver(TaskEntry task) {
        String key = task.getTaskId();
        if (!this.acksMap.containsKey(key) || Arrays.asList(this.acksMap.get(key)).contains(false))
            return false;
        return true;
    }

    /**
     * Update the ack for the specified message represented by the key. This method
     * must be synchronized because it can be accessed by multiple threads
     * 
     * @param ackReq an acknowledgement request
     * @return
     */
    public synchronized Boolean[] updateAck(Timestamp ts, int sender) {
        String key = ts.genKey();
        if (!this.acksMap.containsKey(key)) {
            Boolean[] ackArr = new Boolean[ackLimit];
            Arrays.fill(ackArr, false);
            this.acksMap.put(key, ackArr);
            this.acksMap.get(key)[sender] = true;
        } else {
            this.acksMap.get(key)[sender] = true;
        }
        return this.acksMap.get(key);
    }

    public synchronized void deleteAckArr(String key) {
        this.acksMap.remove(key);
    }

    @Override
    public synchronized int incrementAndGetTimeStamp() {
        this.globalClock++;
        return globalClock;
    }

    @Override
    public synchronized int updateAndIncrementTimeStamp(int SenderTimeStamp) {
        this.globalClock = Math.max(this.globalClock, SenderTimeStamp);
        this.globalClock++;
        return globalClock;
    }

}