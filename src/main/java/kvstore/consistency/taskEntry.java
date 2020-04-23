package kvstore.consistency;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class taskEntry implements Runnable {
    AtomicInteger globalClock;
    int localClock;
    int id;

    @Override
    public void run() {
    }

    public taskEntry(AtomicInteger globalClock, int localClock, int id) {
        this.globalClock = globalClock;
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