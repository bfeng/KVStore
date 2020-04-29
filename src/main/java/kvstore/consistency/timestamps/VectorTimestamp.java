package kvstore.consistency.timestamps;

import java.util.Vector;

import kvstore.consistency.bases.Timestamp;

public class VectorTimestamp extends Timestamp {
    public Vector<Integer> ts;

    @Override
    public String genKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int minus(Timestamp timestamp) {
        // TODO Auto-generated method stub
        return 0;
    }

}