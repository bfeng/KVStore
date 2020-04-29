package kvstore.consistency.bases;

public abstract class TaskEntry<T extends Timestamp> implements Runnable, Comparable<TaskEntry<T>> {
    public T ts;

    public TaskEntry(T ts) {
        this.ts = ts;
    }

    abstract public String getTaskId();
}