package kvstore.consistency.schedulers;

import java.util.Iterator;
import java.util.Vector;
import java.util.stream.IntStream;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.Worker;

public class CausalScheduler extends Scheduler<VectorTimestamp> {
    private int workerId;

    public CausalScheduler(VectorTimestamp ts, int worker_num, int workerId) {
        super(ts);
        this.workerId = workerId;
        Worker.logger.info(String.format("%s", this.globalTs.value.toString()));
    }

    @Override
    public void run() {
        try {
            // Thread.sleep(10 * 1000);
            // Iterator<TaskEntry<VectorTimestamp>> itr = this.tasksQ.iterator();
            // while (itr.hasNext()) {
            // Worker.logger.info(String.format("%s", tasksQ.take().ts.value.toString()));
            // }
            while (true) {
                TaskEntry<VectorTimestamp> task = this.tasksQ.take();
                if (ifAllowDeliver(task)) {

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
        // int thisSum = this.globalTs.value.stream().mapToInt(Integer::intValue).sum();
        // int thatSum = task.ts.value.stream().mapToInt(Integer::intValue).sum();
        // if (thatSum - thisSum == 1) {
        //     return true;
        // }
        return false;
    }

    @Override
    public synchronized TaskEntry<VectorTimestamp> addTask(TaskEntry<VectorTimestamp> taskEntry) {
        Worker.logger.info(String.format("Add the write task: %s", taskEntry.ts.value.toString()));
        this.tasksQ.put(taskEntry);
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

}