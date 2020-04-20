package kvstore.consistency;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class SeqScheduler implements Runnable {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(SeqScheduler.class.getName());
    private Semaphore sem;

    public SeqScheduler(int initSize) {
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
        this.sem = new Semaphore(1);
    }

    @Override
    public void run() {
        while (true) {
            Random rand = new Random();
            try {
                Thread.sleep(rand.nextInt(3) * 1000);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            taskEntry t = new taskEntry(-1, -1);
            try {
                t = this.tasksQ.take();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            logger.info(String.format("New operation %d, %d", t.logicTime, t.id));
            t.sem.release();
        }
    }

    public taskEntry addTask(int logicTime, int id) throws InterruptedException {
        taskEntry newTask = new taskEntry(logicTime, id);
        this.tasksQ.put(newTask);
        return newTask;
    }

    public static class taskEntry {
        public int logicTime;
        public int id;
        public Semaphore sem;

        public taskEntry(int logicTime, int id) {
            this.logicTime = logicTime;
            this.id = id;
            sem = new Semaphore(0);
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