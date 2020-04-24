package kvstore.consistency;

import kvstore.servers.AckReq;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import kvstore.servers.AckReq;

/**
 * All incoming operations are enqueue into a priority queue sorted by a
 * customized comparator. The scheduler keeps taking a task from the priority
 * queue starting the task if allowed.
 */
public class Scheduler implements Runnable {
    private PriorityBlockingQueue<TaskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    private ConcurrentHashMap<String, Boolean[]> acksMap;
    private int ackLimit;
    FileHandler fh;

    /**
     * The ackMaps contains all happened acknowledgement for each message. The value
     * of the map is a boolean array. The index of the array corresponding the id of
     * workers. For example, "1.0" : [fasle, fasle, true] means the message "1.0"
     * doesn't receive acknowledgement from the worker 0, 1 but 2.
     *
     * @param ackLimit The number of acknowledgement required for start a task,
     *                 i.e., delivering a message
     * @throws IOException
     * @throws SecurityException
     */
    public Scheduler(int ackLimit, int workerId) throws SecurityException, IOException {
        /* The initial capacity is set to 16 */
        this.tasksQ = new PriorityBlockingQueue<TaskEntry>(16, new sortByTime());
        /* A hashmap contains all happened acknowledgement */
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(16);
        this.ackLimit = ackLimit;

        /* Configure the logger */
        File logDir = new File("./logs/");
        if (!logDir.exists())
            logDir.mkdir();
        fh = new FileHandler("logs/worker_" + workerId + ".log");
        SimpleFormatter formatter = new SimpleFormatter();
        fh.setFormatter(formatter);
        Scheduler.logger.addHandler(fh);

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
                if (isAcked(task)) {
                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                    logger.info(String.format("<<<Message[%d][%d] Delivered!>>>", task.localClock, task.id));
                } else {

                    tasksQ.put(task); /* Put back the task for rescheduling */

                    /* For debugging */
                    // logger.info(String.format("<<<Message[%d][%d] Blocked! Current ack array:%s>>>", task.localClock,
                    //         task.id, Arrays.toString(this.acksMap.get(task.toString()))));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Add a new task and also create the corresponding ackMap item for this message
     */
    public TaskEntry addTask(TaskEntry newTask) throws InterruptedException {
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
    public synchronized boolean isAcked(TaskEntry task) {
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

    /**
     * A comparator for implementing the total order. The scalar time is used. The
     * id is for the tie-breaking when the clocks are equal
     */
    public static class sortByTime implements Comparator<TaskEntry> {

        @Override
        public int compare(TaskEntry o1, TaskEntry o2) {
            if (o1.localClock != o2.localClock) {
                return o1.localClock - o2.localClock;
            } else {
                return o1.id - o2.id;
            }
        }

    }

}