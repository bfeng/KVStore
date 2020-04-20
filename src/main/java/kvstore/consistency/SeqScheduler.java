package kvstore.consistency;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class SeqScheduler implements Runnable {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(SeqScheduler.class.getName());

    public SeqScheduler(int initSize) {
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
    }

    /**
     * @TODO: Take next when the current thread finished
     * @TODO: When to release the next?
     */
    @Override
    public void run() {
        while (true) {
            try {
                Random rand = new Random();
                Thread.sleep(rand.nextInt(3) * 1000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            // @TODO: What if it didn't acquire when take?
            taskEntry t = new taskEntry();
            try {
                t = this.tasksQ.take();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            /* Wait untill the semaphore is acquired */
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