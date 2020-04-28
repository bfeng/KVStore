package kvstore.consistency.comparators;

import java.util.Comparator;

import kvstore.consistency.bases.TaskEntry;

public class sortByScalarTime implements Comparator<TaskEntry> {

    @Override
    public int compare(TaskEntry o1, TaskEntry o2) {
        if (o1.localClock != o2.localClock) {
            return o1.localClock - o2.localClock;
        } else {
            return o1.id - o2.id;
        }
    }

}