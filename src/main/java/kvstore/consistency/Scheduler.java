package kvstore.consistency;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

/**
 * The scheduler block all rpc calls that are then enqueued into a priority
 * queue. The queue maintains the taskEntry instance which has a semaphore, i.e,
 * the priority queue maitains a queue of semaphores. A seperated thread keep
 * taking a task from the priority queue and resume. Proceed upon the task is
 * finished
 */
public class Scheduler implements Runnable {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(Scheduler.class.getName());
    private ConcurrentHashMap<String, Integer> acksMap;

    public Scheduler(int initSize) {
        /* The initial capacity is set to 16 */
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
        this.acksMap = new ConcurrentHashMap<String, Integer>(16);
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
                Thread.sleep(2 * 1000);
                taskEntry task = tasksQ.take();
                logger.info(String.format("New %s: Clock %d, Id %d, Need Ack %d", task.getClass().getName(),
                        task.localClock, task.id, task.acksNum));
                if (task.acksNum == 0) {
                    logger.info("Received all acks, allow proceeding");
                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();
                } else {
                    tasksQ.put(task);
                    logger.warning("Cannot proceeding");
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }

    public taskEntry addTask(taskEntry newTask) throws InterruptedException {
        tasksQ.put(newTask); /* Put the taks to the priority queue */
        if (!this.acksMap.containsKey(newTask.toString())) {
            this.acksMap.put(newTask.toString(), 0);
        }
        return newTask;
    }

    /**
     * Update the ack number for the specified message represented by the key
     * 
     * @param key
     * @return the latest number of received acks
     */
    public int updateAck(String key) {
        if (!this.acksMap.containsKey(key)) {
            this.acksMap.put(key, 1);
        } else {
            this.acksMap.put(key, this.acksMap.get(key) + 1);
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