package kvstore.consistency.schedulers;

import java.util.Vector;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.consistency.bases.Timestamp;
import kvstore.consistency.timestamps.ScalarTimestamp;
import kvstore.consistency.timestamps.VectorTimestamp;
import kvstore.servers.Worker;

public class CausalScheduler extends Scheduler {
    private Vector<Integer> timeStamp;

    public CausalScheduler(VectorTimestamp vts ,int worker_num) {
        super(vts);
        initTimeStamp(worker_num);
        Worker.logger.info(String.format("%s", this.timeStamp.toString()));

    }

    private void initTimeStamp(int num) {
        this.timeStamp = new Vector<>(num);
        for (int i = 0; i < num; i++) {
            this.timeStamp.add(0);
        }
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
    public Timestamp incrementAndGetTimeStamp() {
        // TODO Auto-generated method stub
        return this.globalTs;
    }

    @Override
    public Timestamp updateAndIncrementTimeStamp(int SenderTimeStamp) {
        // TODO Auto-generated method stub
        return this.globalTs;
    }

}