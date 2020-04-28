package kvstore.consistency;

public class ScalarTimestamp extends Timestamp {
    private final int localClock;
    private final int id;

    public ScalarTimestamp(int localClock, int id) {
        this.localClock = localClock;
        this.id = id;
    }

    /**
     * @return the localClock
     */
    public int getLocalClock() {
        return localClock;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }
}