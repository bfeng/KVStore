package kvstore.consistency.bases;

public abstract class TaskEntry implements Runnable {
    public int localClock;
    public int id;

    public TaskEntry(int localClock, int id) {
        this.localClock = localClock;
        this.id = id;
    }

    abstract public String getTaskId();

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(localClock).append(".").append(id);
        return strBuilder.toString();
    }
}