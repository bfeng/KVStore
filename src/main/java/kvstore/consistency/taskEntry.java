package kvstore.consistency;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects.ToStringHelper;

public abstract class taskEntry implements Runnable {
    AtomicInteger globalClock;
    int localClock;
    int id;
    int acksNum;

    abstract boolean ifAllowDeliver();

    @Override
    public void run() {
    }

    public taskEntry(AtomicInteger globalClock, int localClock, int id, int acksNum) {
        this.globalClock = globalClock;
        this.localClock = localClock;
        this.id = id;
        this.acksNum = acksNum;
    }

    abstract int getAckNum();

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(localClock).append(".").append(id);
        return strBuilder.toString();
    }
}