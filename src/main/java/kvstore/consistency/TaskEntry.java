package kvstore.consistency;

public abstract class TaskEntry implements Runnable {
    int localClock;
    int id;

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