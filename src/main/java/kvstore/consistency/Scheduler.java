package kvstore.consistency;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

import kvstore.servers.AckReq;


/**
 * All incoming operations are enqueue into a priority queue sorted by a
 * customized comparator. The scheduler keeps taking a task from the priority
 * queue starting the task if allowed.
 * 
 */
public class Scheduler implements Runnable {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    private ConcurrentHashMap<String, Boolean[]> acksMap;
    private int ackLimit;

    /**
     * The number of acknowledgement required for start a task, i.e., delivering a
     * message.
     * 
     * The ackMaps contains all happened acknowledgement for each message. The value
     * of the map is a boolean array. The index of the array corresponding the id of
     * workers. For example, "1.0" : [fasle, fasle, true] means the message "1.0"
     * doesn't receive acknowledgement from the worker 0, 1 but 2.
     * 
     * @param ackLimit
     */
    public Scheduler(int ackLimit) {
        /* The initial capacity is set to 16 */
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
        /* A hashmap contains all happened acknowledgement */
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(16);
        this.ackLimit = ackLimit;
    }

    /**
     * Grab a task from the priority queue, running when received all acks
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(new Random().nextInt(3) * 1000); /* Random dealy. Only for the testing purpose */

                /* Taking a task from the queue. Block when the queue is empty */
                WriteTask task = (WriteTask) tasksQ.take();

                /* For debugging */
                // logger.info(String.format("<<<Run Task %s: Message[%d][%d]>>>",
                // task.getClass().getName(),
                // task.localClock, task.id));

                /* Deliver the message when all replies received */
                if (isAcked(task)) {
                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                    logger.info(String.format("<<<Message[%d][%d] Delivered!>>>", task.localClock, task.id));
                } else {
                    /* Here it only allows broadcasting once for each message */
                    if (task.getBcastCount() == 0)
                        task.bcastAcks();
                    tasksQ.put(task); /* Put back the task for rescheduling */

                    /* For debugging */
                    // logger.info(String.format("<<<Message[%d][%d] Blocked! Current ack array:
                    // %s>>>", task.localClock,
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
    public taskEntry addTask(taskEntry newTask) throws InterruptedException {
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
     * 
     * @param key
     */

    public boolean isAcked(taskEntry task) {
        String key = task.toString();
        if (!this.acksMap.containsKey(key) || Arrays.asList(this.acksMap.get(key)).contains(false))
            return false;
        return true;
    }

    /**
     * Update the ack for the specified message represented by the key
     * 
     * @param ackReq an acknowledgement request
     * @return
     */
    public Boolean[] updateAck(AckReq ackReq) {
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
    public static class sortByTime implements Comparator<taskEntry> {

        @Override
        public int compare(taskEntry o1, taskEntry o2) {
            if (o1.localClock != o2.localClock) {
                return o1.localClock - o2.localClock;
            } else {
                return o1.id - o2.id;
            }
        }

    }

}