package kvstore.consistency;

import java.util.Comparator;
import java.util.Random;
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

    public Scheduler(int initSize) {
        /* The initial capacity is set to 16 */
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
    }

    /**
     * Grab a task from the priority queue and release its lock
     */
    @Override
    public void run() {
        while (true) {
            try {
                /* Wait a random duration to let buffer grow */
                Random rand = new Random();
                Thread.sleep(rand.nextInt(3) * 1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            /* Grab a task from the queue. Blocked when no item available */
            taskEntry t = new taskEntry();
            try {
                t = this.tasksQ.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            /* Wait untill the thread is acquired */
            while (!t.sem.hasQueuedThreads()) {
            }
            logger.info(String.format("New write operation"));
            t.sem.release();

            /* Proceed untill the current task is finished */
            try {
                t.finisLatch.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public taskEntry addTask() throws InterruptedException {
        taskEntry newTask = new taskEntry();
        this.tasksQ.put(newTask);
        return newTask;
    }

    public static class taskEntry {
        public int logicTime;
        public int id;
        public Semaphore sem;
        public final CountDownLatch finisLatch;

        public taskEntry() {
            this.logicTime = -1;
            this.id = -1;
            this.sem = new Semaphore(0);
            this.finisLatch = new CountDownLatch(1);
        }
    }

    /**
     * A comparator for implementing the total order, i.e., the scalar time
     */
    public static class sortByTime implements Comparator<taskEntry> {

        @Override
        public int compare(taskEntry o1, taskEntry o2) {
            if (o1.logicTime != o2.logicTime) {
                return o1.logicTime - o2.logicTime;
            } else {
                return o1.id - o2.id;
            }
        }

    }

}