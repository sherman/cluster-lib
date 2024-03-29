package org.sherman.cluster.service;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.sherman.cluster.util.PermutationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaDistributionServiceImplV2 implements ReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaDistributionServiceImplV2.class);

    @Override
    public Map<Integer, List<Integer>> distribute(ReplicaDistribution parameters) {
        Deque<Integer> nodes = new ArrayDeque<>(parameters.getNodes());
        int total = 0;
        Map<Integer, Integer> shardsToReplicas = new HashMap<>();
        Iterator<Integer> nodeIterator = nodes.iterator();

        Map<Integer, List<Integer>> result = new HashMap<>();

        int maxNumberOfReplicas = parameters.getShards().size() * parameters.getReplicas();
        while (total < maxNumberOfReplicas) {
            for (int shard : parameters.getShards()) {
                // get appropriate node
                while (true) {
                    int node;
                    if (nodeIterator.hasNext()) {
                        node = nodeIterator.next();
                    } else {
                        nodeIterator = nodes.iterator();
                        node = nodeIterator.next();
                    }

                    List<Integer> shards = result.computeIfAbsent(node, ignored -> new ArrayList<>());
                    if (!shards.contains(shard)) {
                        shards.add(shard);
                        break;
                    }
                }

                int replicas = shardsToReplicas.getOrDefault(shard, 0);
                shardsToReplicas.put(shard, replicas + 1);
                total++;
                Preconditions.checkArgument(shardsToReplicas.get(shard) <= parameters.getReplicas());
            }
        }

        logger.info("Result: [{}]", result);

        return result;
    }
}
