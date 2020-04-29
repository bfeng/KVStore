package kvstore.consistency.bases;

public abstract class TaskEntry implements Runnable, Comparable<TaskEntry> {
    public Timestamp ts;

    public TaskEntry(Timestamp ts) {
        this.ts = ts;
    }

    abstract public String getTaskId();
}