package kvstore.consistency;

import kvstore.servers.AckReq;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import kvstore.servers.AckReq;

/**
 * All incoming operations are enqueue into a priority queue sorted by a
 * customized comparator. The scheduler keeps taking a task from the priority
 * queue starting the task if allowed.
 */
public class SequentialScheduler extends Scheduler {
    private static final Logger logger = Logger.getLogger(SequentialScheduler.class.getName());
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
    public SequentialScheduler(int ackLimit, int workerId, Comparator<TaskEntry> sortBy)
            throws SecurityException, IOException {
        super(sortBy);
        /* A hashmap contains all happened acknowledgement */
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(1024);
        this.ackLimit = ackLimit;
        this.globalClock = 0;

        /* Configure the logger to outpu the log into files */
        File logDir = new File("./logs/");
        if (!logDir.exists())
            logDir.mkdir();
        fh = new FileHandler("logs/worker_" + workerId + ".log");
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        SequentialScheduler.logger.addHandler(fh);

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
                seqWriteTask task = (seqWriteTask) tasksQ.take();

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
                    deleteAckArr(task.toString());

                    logger.info(String.format("Queue: %d, Map %d", tasksQ.size(), acksMap.size()));

                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                    logger.info(String.format("<<<Message[%d][%d] Delivered!>>>", task.localClock, task.id));

                } else {

                    tasksQ.put(task); /* Put back the task for rescheduling */

                    /* For debugging */
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
        if (!this.acksMap.containsKey(newTask.toString())) {
            Boolean[] ackArr = new Boolean[ackLimit];
            Arrays.fill(ackArr, false);
            this.acksMap.put(newTask.toString(), ackArr);
        }
        return newTask;
    }

    /**
     * Check if all replies for this message is received
     */
    public synchronized boolean ifAllowDeliver(TaskEntry task) {
        String key = task.toString();
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
    public synchronized Boolean[] updateAck(AckReq ackReq) {
        String key = ackReq.getClock() + "." + ackReq.getId();
        if (!this.acksMap.containsKey(key)) {
            Boolean[] ackArr = new Boolean[ackLimit];
            Arrays.fill(ackArr, false);
            this.acksMap.put(key, ackArr);
            this.acksMap.get(key)[ackReq.getSender()] = true;
        } else {
            this.acksMap.get(key)[ackReq.getSender()] = true;
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