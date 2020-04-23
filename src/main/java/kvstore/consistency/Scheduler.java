package kvstore.consistency;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

import kvstore.servers.AckReq;

public class Scheduler implements Runnable {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    private ConcurrentHashMap<String, Boolean[]> acksMap;
    private int ackLimit;

    public Scheduler(int ackLimit) {
        /* The initial capacity is set to 16 */
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
        this.acksMap = new ConcurrentHashMap<String, Boolean[]>(16);
        this.ackLimit = ackLimit;
    }

    /**
     * Grab a task from the priority queue and release its lock
     * 
     * @TODO: when to deliver a message?
     */
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(new Random().nextInt(3) * 1000);/* Only for the testing purpose */

                WriteTask task = (WriteTask) tasksQ.take();
                // logger.info(String.format("<<<Run Task %s: Message[%d][%d]>>>", task.getClass().getName(),
                //         task.localClock, task.id));
                if (isAcked(task)) {
                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                    logger.info(String.format("<<<Message[%d][%d] Delivered!>>>", task.localClock, task.id));
                } else {
                    if (task.getBcastCount() == 0)
                        task.bcastAcks();
                    tasksQ.put(task);
                    // logger.info(String.format("<<<Message[%d][%d] Blocked! Current ack array: %s>>>", task.localClock,
                    //         task.id, Arrays.toString(this.acksMap.get(task.toString()))));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

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
     * Update the ack number for the specified message represented by the key
     * 
     * @param key
     * @return the latest number of received acks
     */

    public boolean isAcked(taskEntry task) {
        String key = task.toString();
        if (Arrays.asList(this.acksMap.get(key)).contains(false))
            return false;
        return true;
    }

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
     * A comparator for implementing the total order, i.e., the scalar time
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