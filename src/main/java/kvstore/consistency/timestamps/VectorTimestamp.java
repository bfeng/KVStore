package kvstore.consistency.timestamps;

import kvstore.consistency.bases.Timestamp;

import java.util.Vector;

public class VectorTimestamp extends Timestamp {
    public Vector<Integer> value;
    private int num;

    public VectorTimestamp(int num) {
        this.num = num;
        this.value = new Vector<Integer>(num);
        for (int i = 0; i < num; i++) {
            this.value.add(0);
        }
    }

    @Override
    public String genKey() {
        return null;
    }

    /**
     * Minus two vector timstamp
     * [0 0 0 0]  [0 0 0 0]
     * <p>
     * [1 0 0 0]  [1 0 0 0]
     * [1 1 0 0]  [1 1 0 0]
     * [1 0 0 1]  [1 0 0 1]
     * <p>
     * o1 [1 0 0 0] o2 [0 0 0 0] which is at the top of the queue?
     * o1 - o2 = 1
     */
    @Override
    public int minus(Timestamp timestamp) {
        VectorTimestamp other = (VectorTimestamp) timestamp;
        int output = 0;
        for (int i = 0; i < this.value.size(); i++) {
            output += this.value.get(i) - other.value.get(i);
        }
        return output;
    }


}