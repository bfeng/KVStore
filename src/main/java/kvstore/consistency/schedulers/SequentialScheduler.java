package kvstore.consistency.schedulers;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.tasks.WriteTask;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.servers.Worker;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * All incoming operations are enqueue into a priority queue sorted by a
 * customized comparator. The scheduler keeps taking a task from the priority
 * queue starting the task if allowed.
 */
public class SequentialScheduler extends Scheduler<ScalarTimestamp> {
    protected PriorityBlockingQueue<TaskEntry<ScalarTimestamp>> tasksQ;
    private final ConcurrentHashMap<String, Boolean[]> acksMap;
    private final int ackLimit;

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
    public SequentialScheduler(ScalarTimestamp ts, int ackLimit) throws SecurityException, IOException {
        super(ts);
        /* A hashmap contains all happened acknowledgement */
        this.tasksQ = new PriorityBlockingQueue<TaskEntry<ScalarTimestamp>>(1024);
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(1024);
        this.ackLimit = ackLimit;
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
                WriteTask<ScalarTimestamp> task = (WriteTask<ScalarTimestamp>) tasksQ.take();

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
                    Worker.logger
                            .info(String.format("<<<Message[%d][%d] Delivered!>>> | Mode: Sequential", task.ts.localClock, task.ts.id));

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
     */
    @Override
    public synchronized TaskEntry<ScalarTimestamp> addTask(TaskEntry<ScalarTimestamp> newTask) {
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
    @Override
    public synchronized boolean ifAllowDeliver(TaskEntry<ScalarTimestamp> task) {
        String key = task.getTaskId();
        return this.acksMap.containsKey(key) && !Arrays.asList(this.acksMap.get(key)).contains(false);
    }

    /**
     * Update the ack for the specified message represented by the key. This method
     * must be synchronized because it can be accessed by multiple threads
     *
     * @param ackReq an acknowledgement request
     * @return
     */
    public synchronized Boolean[] updateAck(ScalarTimestamp ts, int sender) {
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
    public synchronized ScalarTimestamp incrementAndGetTimeStamp() {
        (this.globalTs).localClock++;
        int localClock = this.globalTs.localClock;
        int id = this.globalTs.id;
        return (new ScalarTimestamp(localClock, id));
    }

    @Override
    public synchronized ScalarTimestamp updateAndIncrementTimeStamp(int SenderTimeStamp) {
        this.globalTs.localClock = Math.max(this.globalTs.localClock, SenderTimeStamp);
        this.globalTs.localClock++;

        int localClock = this.globalTs.localClock;
        int id = this.globalTs.id;
        return (new ScalarTimestamp(localClock, id));
    }

    @Override
    public ScalarTimestamp getCurrentTimestamp() {
        return null;
    }

}