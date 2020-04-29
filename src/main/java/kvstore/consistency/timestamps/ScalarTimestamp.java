package kvstore.consistency.timestamps;

import kvstore.consistency.bases.Timestamp;

public class ScalarTimestamp extends Timestamp {
    public int localClock;
    public int id;

    public ScalarTimestamp(int localClock, int id) {
        this.localClock = localClock;
        this.id = id;
    }

    @Override
    public String genKey() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(this.localClock).append(".").append(this.id);
        return strBuilder.toString();
    }

    @Override
    public int minus(Timestamp timestamp) {
        ScalarTimestamp scalarTimestamp = (ScalarTimestamp) timestamp;
        if (this.localClock != scalarTimestamp.localClock) {
            return this.localClock - scalarTimestamp.localClock;
        } else {
            return this.id - scalarTimestamp.id;
        }
    }

}