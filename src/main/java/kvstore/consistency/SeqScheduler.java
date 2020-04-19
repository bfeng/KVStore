package kvstore.consistency;

import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

public class SeqScheduler {
    private PriorityBlockingQueue<taskEntry> tasksQ;
    private static final Logger logger = Logger.getLogger(SeqScheduler.class.getName());

    public SeqScheduler(int initSize) {
        this.tasksQ = new PriorityBlockingQueue<taskEntry>(16, new sortByTime());
    }

    public void seqWait(int logicTime, int id) throws InterruptedException {
        taskEntry task = new taskEntry(logicTime, id);
        this.tasksQ.put(task);

        Random rand = new Random();

        int e = rand.nextInt(1);
        Thread.sleep(e * 1000);

        taskEntry topTask = this.tasksQ.peek();

        /* If the current message is not the at the topmost */
        if (topTask.logicTime != logicTime || topTask.id != id) {
            logger.info("{{{{{{{{{{{{{" + "Block " + logicTime + "," + id + "}}}}}}}}}}}}}");
            task.taskWait();
            return;
        }
        this.tasksQ.take();
        return;
    }

    public void seqResume() throws InterruptedException {
        if (this.tasksQ.isEmpty())
            return;
        // Thread.sleep(1000);
        taskEntry topTask = this.tasksQ.take();
        logger.info("<<<<<<<<<" + "Trying to Resume " + topTask.logicTime + "," + topTask.id + ">>>>>>>>>");
        topTask.taskResume();
    }

    public static class taskEntry {
        public int logicTime;
        public int id;
        private ArrayBlockingQueue<Integer> q;

        public taskEntry(int logicTime, int id) {
            this.logicTime = logicTime;
            this.id = id;
            q = new ArrayBlockingQueue<Integer>(1);
        }

        public void taskWait() throws InterruptedException {
            q.take();
        }

        public void taskResume() throws InterruptedException {
            q.put(1);
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