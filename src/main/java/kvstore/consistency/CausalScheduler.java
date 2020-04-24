package kvstore.consistency;

import java.util.Comparator;

public class CausalScheduler extends Scheduler {

    public CausalScheduler(Comparator<TaskEntry> sortBy) {
        super(sortBy);
        // TODO Auto-generated constructor stub
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