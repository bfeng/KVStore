package kvstore.consistency.bases;

public abstract class TaskEntry implements Runnable {
    public Timestamp ts;

    abstract public String getTaskId();

    abstract public int minus(TaskEntry taskEntry);
}