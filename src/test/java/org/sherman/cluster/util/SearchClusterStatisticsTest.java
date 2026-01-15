package org.sherman.cluster.util;

import org.sherman.cluster.domain.SearchClusterStatistics;
import org.sherman.cluster.domain.SearchNode;
import org.sherman.cluster.domain.SearchShard;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SearchClusterStatisticsTest {
    @Test
    public void getAverageShardsByIndex() {
        var statistics = new SearchClusterStatistics();
        Assert.assertEquals(0.0f, statistics.getAverageShardsByIndex("test"));

        // add node
        var node0 = new SearchNode("node0");
        statistics.addNode(node0);
        Assert.assertEquals(0.0f, statistics.getAverageShardsByIndex("test"));
        statistics.getNodeStatistics(node0).addShard(new SearchShard("test", 1));
        statistics.getNodeStatistics(node0).addShard(new SearchShard("test", 2));
        Assert.assertEquals(2.0f, statistics.getAverageShardsByIndex("test"));

        // add one more node
        var node1 = new SearchNode("node1");
        statistics.addNode(node1);
        Assert.assertEquals(1.0f, statistics.getAverageShardsByIndex("test"));
    }

    @Test
    public void getAverageShards() {
        var statistics = new SearchClusterStatistics();
        Assert.assertEquals(0.0f, statistics.getAverageShardsByIndex("test"));

        // add node
        var node0 = new SearchNode("node0");
        statistics.addNode(node0);
        Assert.assertEquals(0.0f, statistics.getAverageShardsPerNode());
        statistics.getNodeStatistics(node0).addShard(new SearchShard("test", 1));
        statistics.getNodeStatistics(node0).addShard(new SearchShard("test", 2));
        Assert.assertEquals(2.0f, statistics.getAverageShardsPerNode());

        // add one more node
        var node1 = new SearchNode("node1");
        statistics.addNode(node1);
        Assert.assertEquals(1.0f, statistics.getAverageShardsPerNode());

        // add one more node
        var node2 = new SearchNode("node2");
        statistics.addNode(node2);
        Assert.assertEquals(0.6666667f, statistics.getAverageShardsPerNode());
    }
}
