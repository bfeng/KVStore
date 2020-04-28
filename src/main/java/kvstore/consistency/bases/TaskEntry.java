package kvstore.consistency.bases;

public abstract class TaskEntry implements Runnable {

    abstract public String getTaskId();

    abstract public int minus(TaskEntry taskEntry);
}