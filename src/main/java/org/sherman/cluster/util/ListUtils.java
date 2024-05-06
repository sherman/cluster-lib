package org.sherman.cluster.util;

import java.util.ArrayList;
import java.util.List;

public class ListUtils {
    private ListUtils() {
    }

    public static List<Integer> getRemoved(List<Integer> oldList, List<Integer> newList) {
        List<Integer> removed = new ArrayList<>();
        for (int element : oldList) {
            if (!newList.contains(element)) {
                removed.add(element);
            }
        }
        return removed;
    }

    public static List<Integer> getAdded(List<Integer> oldList, List<Integer> newList) {
        List<Integer> added = new ArrayList<>();
        for (int element : newList) {
            if (!oldList.contains(element)) {
                added.add(element);
            }
        }
        return added;
    }

}
