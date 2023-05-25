package org.sherman.cluster.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.sherman.cluster.util.PermutationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaDistributionServiceImpl implements ReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaDistributionServiceImpl.class);

    @Override
    public Map<Integer, List<Integer>> distribute(ReplicaDistribution parameters) {
        int nums = (int) Math.ceil((double) (parameters.getShards().size() * parameters.getReplicas()) / parameters.getNodes().size());

        Set<List<Integer>> permutations = PermutationUtils.getPermutations(parameters.getShards(), nums);

        logger.info("[{}]", permutations);

        List<List<Integer>> perms = new ArrayList<>();
        perms.addAll(permutations);

        Map<Integer, List<Integer>> result = new HashMap<>();
        Map<Integer, Integer> stat = new HashMap<>();

        for (int node : parameters.getNodes()) {
            result.put(node, new ArrayList<>());
        }

        List<List<Integer>> valid = new ArrayList<>();

        for (int i = 0; i < perms.size(); i++) {
            List<Integer> permutation = perms.get(i);
            boolean skip = false;
            for (int shard : permutation) {
                if (stat.getOrDefault(shard, 0) + 1 > parameters.getReplicas()) {
                    skip = true;
                    break;
                }
            }

            if (!skip) {
                valid.add(permutation);
                for (int shard : permutation) {
                    int counter = stat.getOrDefault(shard, 0);
                    stat.put(shard, counter + 1);
                }
            }
        }

        Iterator<List<Integer>> iter = valid.iterator();
        for (int node : parameters.getNodes()) {
            if (!iter.hasNext()) {
                iter = valid.iterator();
            }
            result.get(node).addAll(iter.next());
        }

        return result;
    }
}
