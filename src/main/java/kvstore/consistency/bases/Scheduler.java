package kvstore.consistency.bases;

import java.util.concurrent.PriorityBlockingQueue;

public abstract class Scheduler<T extends Timestamp> implements Runnable {
    protected T globalTs;
    public Scheduler(T ts) {
        this.globalTs = ts;
    }

    /**
     * Check if allow deliver the task
     */
    abstract public boolean ifAllowDeliver(TaskEntry<T> task);

    /**
     * Add a task to the scheduler
     * 
     * @throws InterruptedException
     */
    abstract public TaskEntry<T> addTask(TaskEntry<T> taskEntry);

    abstract public T getCurrentTimestamp();

    /**
     * Increment the time stamp owned by the scheduler
     * 
     * @return the updated timestap
     */
    abstract public T incrementAndGetTimeStamp();

    /**
     * Update the time stamp with a incoming sender
     */
    abstract public T updateAndIncrementTimeStamp(int SenderTimeStamp);

}