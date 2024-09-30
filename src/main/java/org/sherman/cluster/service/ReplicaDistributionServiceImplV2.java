package org.sherman.cluster.service;

import com.google.common.base.Preconditions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.sherman.cluster.domain.ReplicaDistribution;
import org.sherman.cluster.util.ListUtils;
import org.sherman.cluster.util.PermutationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributes shards with the required number of replicas between a cluster of nodes.
 * Trying to minimize the number of rebalanced shards when a set of nodes is changed.
 */
public class ReplicaDistributionServiceImplV2 implements ReplicaDistributionService {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaDistributionServiceImplV2.class);

    @Override
    public Map<Integer, List<Integer>> distribute(ReplicaDistribution parameters) {
        List<Integer> actual = new ArrayList<>();
        if (!parameters.getPrevNodes().isEmpty()) {
            // handle removed nodes
            var removed = ListUtils.getRemoved(parameters.getPrevNodes(), parameters.getNodes());
            var moved = new HashSet<Integer>();
            var newList = new ArrayDeque<>(parameters.getNodes());
            for (var node : parameters.getPrevNodes()) {
                if (removed.contains(node)) {
                    while (!newList.isEmpty()) {
                        var lastNode = newList.removeLast();
                        if (!moved.contains(lastNode)) {
                            actual.add(lastNode);
                            moved.add(lastNode);
                            break;
                        }
                    }
                } else {
                    if (!moved.contains(node)) {
                        actual.add(node);
                    }
                }
            }

            // handle added nodes
            for (var node : parameters.getNodes()) {
                if (!actual.contains(node)) {
                    actual.add(node);
                }
            }
        } else {
            actual = parameters.getNodes();
        }

        logger.info("Actual list: [{}]", actual);

        var nodes = new ArrayDeque<>(actual);
        var total = 0;
        var shardsToReplicas = new HashMap<Integer, Integer>();
        var nodeIterator = nodes.iterator();

        var result = new HashMap<Integer, List<Integer>>();

        var maxNumberOfReplicas = parameters.getShards().size() * parameters.getReplicas();
        while (total < maxNumberOfReplicas) {
            for (var shard : parameters.getShards()) {
                // get appropriate node
                while (true) {
                    var node = 0;
                    if (nodeIterator.hasNext()) {
                        node = nodeIterator.next();
                    } else {
                        nodeIterator = nodes.iterator();
                        node = nodeIterator.next();
                    }

                    var shards = result.computeIfAbsent(node, ignored -> new ArrayList<>());
                    if (!shards.contains(shard)) {
                        shards.add(shard);
                        break;
                    }
                }

                var replicas = shardsToReplicas.getOrDefault(shard, 0);
                shardsToReplicas.put(shard, replicas + 1);
                total++;
                Preconditions.checkArgument(shardsToReplicas.get(shard) <= parameters.getReplicas());
            }
        }

        logger.info("Result: [{}]", result);

        return result;
    }
}
