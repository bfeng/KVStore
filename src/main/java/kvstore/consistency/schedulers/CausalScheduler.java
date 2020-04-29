package kvstore.consistency.schedulers;

import java.util.Vector;

import kvstore.consistency.bases.Scheduler;
import kvstore.consistency.bases.TaskEntry;
import kvstore.servers.Worker;

public class CausalScheduler extends Scheduler {
    private Vector<Vector<Integer>> timeStamp;

    public CausalScheduler(int worker_size) {
        initTimeStamp(worker_size);
        Worker.logger.info(String.format("%s", this.timeStamp.toString()));

    }

    private void initTimeStamp(int size) {
        this.timeStamp = new Vector<>(size);
        for (int i = 0; i < size; i++) {
            Vector<Integer> r = new Vector<Integer>(size);
            for (int j = 0; j < size; j++) {
                r.add(0);
            }
            this.timeStamp.add(r);
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
    public int incrementAndGetTimeStamp() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int updateAndIncrementTimeStamp(int SenderTimeStamp) {
        // TODO Auto-generated method stub
        return 0;
    }

}