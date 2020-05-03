package kvstore.consistency.schedulers;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.tasks.BcastWriteTask;
import kvstore.consistency.tasks.WriteTask;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.Worker;

import java.util.Vector;
import java.util.concurrent.LinkedBlockingDeque;

public class EventualScheduler extends Scheduler<VectorTimestamp> {
    private final int workerId;
    private final LinkedBlockingDeque<TaskEntry<VectorTimestamp>> tasksQ;

    public EventualScheduler(VectorTimestamp ts, int workerId) {
        super(ts);
        this.tasksQ = new LinkedBlockingDeque<>(1024);
        this.workerId = workerId;
        Worker.logger.info(String.format("%s", this.globalTs.value.toString()));
    }

    @Override
    public void run() {
        try {
            do {
                // Thread.sleep(1 * 1000);
                TaskEntry<VectorTimestamp> task = this.tasksQ.take();
                if (ifAllowDeliver(task)) {
                    if (task instanceof BcastWriteTask) {
                        /* Set the timstamp when multicast the write task */
                        int senderId = ((BcastWriteTask<VectorTimestamp>) task).workerId;

                        ((BcastWriteTask<VectorTimestamp>) (task)).setTimestamp(incrementAndGetTimeStamp());
                        Worker.logger.info(String.format("<<<Message%s[%d] Delivered!>>> at %s[%d] | Mode: Causal",
                                task.ts.value.toString(), senderId, this.getCurrentTimestamp().value.toString(), workerId));
                    } else if (task instanceof WriteTask) {
                        int senderId = ((WriteTask<VectorTimestamp>) task).writeReqBcast.getSender();

                        int old = this.globalTs.value.get(senderId);
                        this.globalTs.value.set(senderId, old + 1);
                        Worker.logger.info(String.format("<<<Message%s[%d] Delivered!>>> at %s[%d] | Mode: Causal",
                                task.ts.value.toString(), senderId, this.getCurrentTimestamp().value.toString(), workerId));
                    }

                    Thread taskThread = new Thread(task);
                    taskThread.start();
                    taskThread.join();

                } else {
                    Worker.logger.info(String.format("Message%s Blocked!", task.ts.value.toString()));
                    this.tasksQ.put(task);
                }
            } while (true);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Allow any task is the next expected
     */
    @Override
    public synchronized boolean ifAllowDeliver(TaskEntry<VectorTimestamp> task) {
        return true;
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
