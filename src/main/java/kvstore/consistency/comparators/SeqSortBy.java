package kvstore.consistency.comparators;

import java.util.Comparator;

import kvstore.consistency.bases.TaskEntry;

public class SeqSortBy implements Comparator<TaskEntry> {

    @Override
    public int compare(TaskEntry o1, TaskEntry o2) {
        return o1.minus(o2);
    }

}