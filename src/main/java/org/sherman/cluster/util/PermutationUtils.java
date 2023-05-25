package org.sherman.cluster.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PermutationUtils {
    private static final Logger logger = LoggerFactory.getLogger(PermutationUtils.class);

    private PermutationUtils() {
    }

    public static Set<List<Integer>> getPermutations(List<Integer> allShards, int shardsNumber) {
        List<Integer> shards = new ArrayList<>();
        shards.addAll(allShards);
        Set<List<Integer>> result = new HashSet<>();
        collectPermutations(shards, new ArrayList<>(), result, shardsNumber);
        return result;
    }

    private static void collectPermutations(
        List<Integer> source,
        List<Integer> selected,
        Set<List<Integer>> result,
        int shardsNumber
    ) {
        logger.info("{} {}", source, selected);
        if (selected.size() == shardsNumber) {
            List<Integer> temp = new ArrayList<>(selected);
            result.add(temp);
        } else {
            for (int i = 0; i < source.size(); i++) {
                int shard = source.remove(i);
                selected.add(shard);

                collectPermutations(source, selected, result, shardsNumber);

                source.add(i, shard);
                selected.remove(selected.size() - 1);
            }
        }
    }
}
