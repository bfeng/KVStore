package kvstore.consistency.schedulers;

import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.tasks.BcastWriteTask;
import kvstore.consistency.tasks.WriteTask;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.Worker;

public class CausalScheduler extends Scheduler<VectorTimestamp> {
    private int workerId;
    private LinkedBlockingDeque<TaskEntry<VectorTimestamp>> tasksQ;

    public CausalScheduler(VectorTimestamp ts, int worker_num, int workerId) {
        super(ts);
        this.tasksQ = new LinkedBlockingDeque<TaskEntry<VectorTimestamp>>(1024);
        this.workerId = workerId;
        Worker.logger.info(String.format("%s", this.globalTs.value.toString()));
    }

    @Override
    public void run() {
        try {

            // Iterator<TaskEntry<VectorTimestamp>> itr = this.tasksQ.iterator();
            // while (itr.hasNext()) {
            // TaskEntry<VectorTimestamp> tmp = itr.next();
            // Worker.logger.info(String.format("%s, %s", tmp.ts.value.toString(),
            // tmp.getClass().getName()));
            // }
            while (true) {
                // Thread.sleep(1 * 1000);
                TaskEntry<VectorTimestamp> task = this.tasksQ.take();
                if (ifAllowDeliver(task)) {
                    if (task instanceof BcastWriteTask) {
                        /* Set the timstamp when multicast the write task */
                        // Worker.logger.info(String.format("Current Vts before increment: %s",
                        // this.globalTs.value.toString()));
                        int senderId = ((BcastWriteTask<VectorTimestamp>) task).workerId;

                        ((BcastWriteTask<VectorTimestamp>) (task)).setTimestamp(incrementAndGetTimeStamp());
                        Worker.logger.info(String.format("<<<Message%s Delivered!>>> at %s from worker %d",
                                task.ts.value.toString(), this.getCurrentTimestamp().value.toString(), senderId));
                        // Worker.logger.info(String.format("Current Vts: %s",
                        // this.globalTs.value.toString()));
                    } else if (task instanceof WriteTask) {
                        int senderId = ((WriteTask<VectorTimestamp>) task).writeReqBcast.getSender();

                        int old = this.globalTs.value.get(senderId);
                        this.globalTs.value.set(senderId, old + 1);
                        Worker.logger.info(String.format("<<<Message%s Delivered!>>> at %s from worker %d",
                                task.ts.value.toString(), this.getCurrentTimestamp().value.toString(), senderId));
                        // Worker.logger.info(String.format("Allow delivering %s",
                        // task.ts.value.toString()));

                        // Worker.logger.info(String.format("Current Vts: %s",
                        // this.globalTs.value.toString()));
                    }

                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();

                } else {
                    Worker.logger.info(String.format("Message%s Blocked!", task.ts.value.toString()));
                    this.tasksQ.put(task);
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Allow only when the task is the next expected (e.g., [1 0 0 0] is the next
     * expected for [0 0 0 0])
     */
    @Override
    public synchronized boolean ifAllowDeliver(TaskEntry<VectorTimestamp> task) {
        /* The bacstWriteTask is not restricted */
        if (task instanceof BcastWriteTask) {
            return true;
        } else {
            // Worker.logger.info(
            // String.format("Comparing %s and %s", this.globalTs.value.toString(),
            // task.ts.value.toString()));
        }
        return checkConditions(task.ts, ((WriteTask<VectorTimestamp>) task).writeReqBcast.getSender());
    }

    /**
     * [1 1 0 1] [2 1 0 1]
     */
    private boolean checkConditions(VectorTimestamp expectedVts, int senderId) {
        VectorTimestamp currentVts = this.globalTs;
        boolean isExpectedFromSender = (expectedVts.value.get(senderId) == currentVts.value.get(senderId) + 1);

        boolean LargerThanAllOthers = true;
        for (int i = 0; i < expectedVts.value.size(); i++) {
            if (i != senderId) {
                if (currentVts.value.get(i) - expectedVts.value.get(i) < 0) {
                    LargerThanAllOthers = false;
                    break;
                }
            }
        }
        return isExpectedFromSender && LargerThanAllOthers;
    }

    @Override
    public synchronized TaskEntry<VectorTimestamp> addTask(TaskEntry<VectorTimestamp> taskEntry) {
        // if (!(taskEntry instanceof BcastWriteTask)) {
        // Worker.logger.info(String.format("Add a write task: %s",
        // taskEntry.ts.value.toString()));
        // }
        try {
            this.tasksQ.put(taskEntry);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public synchronized VectorTimestamp incrementAndGetTimeStamp() {
        int old = this.globalTs.value.get(this.workerId);
        this.globalTs.value.set(this.workerId, old + 1);
        VectorTimestamp newVts = new VectorTimestamp(this.workerId);
        newVts.value = new Vector<Integer>(this.globalTs.value);
        return newVts;
    }

    @Override
    public synchronized VectorTimestamp updateAndIncrementTimeStamp(int SenderTimeStamp) {
        return null;
    }

    @Override
    public VectorTimestamp getCurrentTimestamp() {
        VectorTimestamp vts = new VectorTimestamp(0);
        vts.value = new Vector<Integer>(this.globalTs.value);
        return vts;
    }

}