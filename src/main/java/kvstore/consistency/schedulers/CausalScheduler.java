package kvstore.consistency.schedulers;

import java.util.Vector;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.Worker;

public class CausalScheduler extends Scheduler<VectorTimestamp> {

    public CausalScheduler(VectorTimestamp vts, int worker_num) {
        super(vts);
        initTimeStamp(worker_num);
        Worker.logger.info(String.format("%s", ((VectorTimestamp) (this.globalTs)).toString()));
    }

    private void initTimeStamp(int num) {
        // ((VectorTimestamp(this.globalTs)).ts = new Vector<>(num);
        // for (int i = 0; i < num; i++) {
        // this.timeStamp.add(0);
        // }
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean ifAllowDeliver(TaskEntry task) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public TaskEntry addTask(TaskEntry taskEntry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VectorTimestamp incrementAndGetTimeStamp() {
        // TODO Auto-generated method stub
        return this.globalTs;
    }

    @Override
    public VectorTimestamp updateAndIncrementTimeStamp(int SenderTimeStamp) {
        // TODO Auto-generated method stub
        return this.globalTs;
    }

}