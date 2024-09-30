package org.sherman.cluster.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.sherman.cluster.util.PermutationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaDistributionServiceImpl implements ReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaDistributionServiceImpl.class);

    @Override
    public Map<Integer, List<Integer>> distribute(ReplicaDistribution parameters) {
        var nums = (int) Math.ceil((double) (parameters.getShards().size() * parameters.getReplicas()) / parameters.getNodes().size());

        var permutations = PermutationUtils.getPermutations(parameters.getShards(), nums);

        logger.info("[{}]", permutations);

        var perms = new ArrayList<List<Integer>>();
        perms.addAll(permutations);

        var result = new HashMap<Integer, List<Integer>>();
        var stat = new HashMap<Integer, Integer>();

        for (var node : parameters.getNodes()) {
            result.put(node, new ArrayList<>());
        }

        var valid = new ArrayList<List<Integer>>();

        for (var i = 0; i < perms.size(); i++) {
            var permutation = perms.get(i);
            var skip = false;
            for (var shard : permutation) {
                if (stat.getOrDefault(shard, 0) + 1 > parameters.getReplicas()) {
                    skip = true;
                    break;
                }
            }

            if (!skip) {
                valid.add(permutation);
                for (var shard : permutation) {
                    var counter = stat.getOrDefault(shard, 0);
                    stat.put(shard, counter + 1);
                }
            }
        }

        var iter = valid.iterator();
        for (var node : parameters.getNodes()) {
            if (!iter.hasNext()) {
                iter = valid.iterator();
            }
            result.get(node).addAll(iter.next());
        }

        return result;
    }
}
