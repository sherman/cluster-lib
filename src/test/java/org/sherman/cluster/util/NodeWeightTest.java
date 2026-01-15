package org.sherman.cluster.util;

import java.util.HashMap;
import java.util.Map;
import org.sherman.cluster.domain.SearchClusterStatistics;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchNodeStatistics;
import org.sherman.cluster.domain.SearchShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class NodeWeightTest {
    private static final Logger logger = LoggerFactory.getLogger(NodeWeightTest.class);

    @Test(dataProvider = "data")
    void getWeight(Map<Integer, Map<String, Integer>> statistics) {
        var nodeWeight = new NodeWeight(0.45f, 0.55f);

        var clusterStatistics = new SearchClusterStatistics();
        var nodesStatistics = new HashMap<SearchNode, SearchNodeStatistics>();
        for (var statsPerNode : statistics.entrySet()) {
            var node = new SearchNode("node" + statsPerNode.getKey());
            clusterStatistics.addNode(node);
            nodesStatistics.put(node, clusterStatistics.getNodeStatistics(node));
        }

        var shardId = 1;
        for (var statsPerNode : statistics.entrySet()) {
            var nodeKey = statsPerNode.getKey();
            var indicesAndShards = statsPerNode.getValue();
            var nodeStatistics = nodesStatistics.get(new SearchNode("node" + nodeKey));
            for (var indexStatistics : indicesAndShards.entrySet()) {
                for (var k = 0; k < indexStatistics.getValue(); k++) {
                    nodeStatistics.addShard(new SearchShard(indexStatistics.getKey(), shardId));
                    shardId++;
                }
            }
        }

        // TODO: add asserts here
        for (var index : clusterStatistics.getIndices()) {
            logger.info("Index: [{}]", index);
            for (var nodeAndStatistics : nodesStatistics.entrySet()) {
                var weight = nodeWeight.getWeight(nodeAndStatistics.getValue(), clusterStatistics, nodeAndStatistics.getKey(), index);
                logger.info("Weight: [{}] for node: [{}]", weight, nodeAndStatistics.getKey().getId());
            }
            logger.info("=================================");
        }
    }

    @DataProvider
    public static Object[][] data() {
        return new Object[][] {
            {Map.of(1, Map.of())},
            {Map.of(1, Map.of("index1", 1))},
            {Map.of(1, Map.of("index1", 2))},
            {Map.of(1, Map.of("index1", 1, "index2", 1, "index3", 1))},
            {Map.of(1, Map.of("index1", 3, "index2", 3, "index3", 3))},
            {
                Map.of(
                    1, Map.of("index1", 3, "index2", 3, "index3", 3),
                    2, Map.of("index3", 3, "index4", 3, "index5", 3),
                    3, Map.of("index6", 3)
                )
            },
            {
                Map.of(
                    1, Map.of("index1", 3, "index2", 2, "index3", 3),
                    2, Map.of("index3", 3, "index4", 3, "index5", 3)
                )
            },
            {
                Map.of(
                    1, Map.of("index1", 1, "index2", 1),
                    2, Map.of("index3", 1, "index4", 2),
                    3, Map.of("index5", 1, "index6", 3),
                    4, Map.of("index7", 1, "index8", 2)
                )
            },
            {
                Map.of(
                    1, Map.of("index1", 1),
                    2, Map.of("index2", 1),
                    3, Map.of("index3", 1),
                    4, Map.of("index4", 2)
                )
            }
        };
    }
}
