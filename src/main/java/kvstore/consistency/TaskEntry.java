package kvstore.consistency;

public abstract class TaskEntry implements Runnable {
    int localClock;
    int id;

    @Override
    public void run() {
    }

    public TaskEntry(int localClock, int id) {
        this.localClock = localClock;
        this.id = id;
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(localClock).append(".").append(id);
        return strBuilder.toString();
    }
}