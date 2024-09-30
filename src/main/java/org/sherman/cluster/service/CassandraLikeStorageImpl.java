package org.sherman.cluster.service;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;
import org.sherman.cluster.util.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Denis M. Gabaydulin
 * @since 08.03.19
 */
@SuppressWarnings("UnstableApiUsage")
public class CassandraLikeStorageImpl implements CassandraLikeStorage<Long> {
    private static final Logger log = LoggerFactory.getLogger(CassandraLikeStorageImpl.class);

    private final Range<Long> closedRange;
    private final ServerStorage serverStorage;

    private Map<ServerNode, RangeSet<Long>> nodesToRanges = new HashMap<>();
    private RangeMap<Long, ServerNode> rangesToServerNodes = TreeRangeMap.create();
    private RangeMap<Long, Set<String>> rangesToElements = TreeRangeMap.create();

    private final int initialTokens;

    public CassandraLikeStorageImpl(ServerStorage serverStorage, Range<Long> closedRange, int initialTokens) {
        this.serverStorage = serverStorage;
        this.closedRange = closedRange;

        this.initialTokens = initialTokens;

        init();
    }

    private void init() {
        var serversWithTokens = new TreeSet<ServerNodeWithToken>();

        for (var serverNode : serverStorage.getServers()) {
            Set<Long> tokens = generateTokens();

            log.info("Tokens: [{}]", tokens);

            tokens.forEach(t -> serversWithTokens.add(new ServerNodeWithToken(serverNode, t)));

            nodesToRanges.put(serverNode, TreeRangeSet.create());
        }

        log.info("Servers with tokens: [{}]", serversWithTokens);

        Long prevToken = null;
        for (var serverNodeWithToken : serversWithTokens) {
            if (prevToken == null) {
                prevToken = closedRange.lowerEndpoint();
            }

            var range = Range.closed(prevToken, serverNodeWithToken.token);

            nodesToRanges.get(serverNodeWithToken.serverNode).add(range);
            rangesToServerNodes.put(range, serverNodeWithToken.serverNode);
            rangesToElements.put(range, new HashSet<>());

            prevToken = serverNodeWithToken.token + 1;
        }

        var serverNode = serverStorage.getRandom();
        var range = Range.closed(prevToken, closedRange.upperEndpoint());
        nodesToRanges.get(serverNode).add(range);
        rangesToServerNodes.put(range, serverNode);
        rangesToElements.put(range, new HashSet<>());

        log.info("{}", nodesToRanges);
    }

    @Override
    public void putKey(@NotNull String key) {
        //log.info("{}", key);
        rangesToElements.get(Tokens.getTokenByKey(key)).add(key);
    }

    @NotNull
    @Override
    public ServerNode getServerNodeByKey(@NotNull String key) {
        return rangesToServerNodes.get(Tokens.getTokenByKey(key));
    }

    @Override
    public Map<ServerNode, Integer> getDistribution() {
        return rangesToServerNodes.asMapOfRanges().entrySet().stream()
            .map(e -> Map.entry(e.getValue(), rangesToElements.getEntry(e.getKey().lowerEndpoint()).getValue().size()))
            .collect(Collectors.groupingBy((Function<Map.Entry<ServerNode, Integer>, ServerNode>) Map.Entry::getKey))
            .entrySet().stream()
            .map((Function<Map.Entry<ServerNode, List<Map.Entry<ServerNode, Integer>>>, Map.Entry<ServerNode, Integer>>) entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().stream().map(Map.Entry::getValue).reduce(0, Integer::sum)
                ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * In case of re-balancing (e.g. add new node in a cluster) we should split all ranges by random points to move some data to a new node.
     */
    @Override
    public void addServer(ServerNode serverNode) {
        // generate tokens for a new server
        var tokens = generateTokens();
        log.info("Tokens: [{}]", tokens);
        var serversWithTokens = new TreeSet<ServerNodeWithToken>();
        tokens.forEach(t -> serversWithTokens.add(new ServerNodeWithToken(serverNode, t)));

        var newNodesToRanges = new HashMap<ServerNode, RangeSet<Long>>();
        var newRangesToServerNodes = TreeRangeMap.<Long, ServerNode>create();
        var newRangesToElements = TreeRangeMap.<Long, Set<String>>create();
        Map<Range<Long>, List<Range<Long>>> replacement = new HashMap<>();

        long sum1 = 0;
        long sum2 = 0;

        for (var token : tokens) {
            for (var node : nodesToRanges.keySet()) {
                for (var range : nodesToRanges.get(node).asRanges()) {
                    if (range.contains(token)) {
                        if (range.lowerEndpoint().equals(range.upperEndpoint())) {
                            log.info("Can't split a single point range");
                            continue;
                        }

                        if (replacement.containsKey(token)) {
                            log.info("Can't split range, was already split");
                            continue;
                        }

                        Range<Long> r1;
                        Range<Long> r2;
                        // split range
                        if (token.equals(range.lowerEndpoint())) {
                            r1 = Range.closed(range.lowerEndpoint(), range.lowerEndpoint());
                            r2 = Range.closed(Math.min(range.lowerEndpoint() + 1, range.upperEndpoint()), range.upperEndpoint());
                        } else if (token.equals(range.upperEndpoint())) {
                            r1 = Range.closed(range.lowerEndpoint(), Math.max(range.lowerEndpoint(), range.upperEndpoint() - 1));
                            r2 = Range.closed(range.upperEndpoint(), range.upperEndpoint());
                        } else {
                            r1 = Range.closed(range.lowerEndpoint(), token);
                            r2 = Range.closed(token + 1, range.upperEndpoint());
                        }

                        sum1 += r1.upperEndpoint() - r1.lowerEndpoint();
                        sum2 += r2.upperEndpoint() - r2.lowerEndpoint();

                        log.info("Range [{}] split to [{}] and [{}]", range, r1, r2);

                        replacement.put(range, ImmutableList.of(r1, r2));
                    }
                }
            }
        }

        log.info("Sum1: {}, sum2: {}", sum1, sum2);

        // add new node
        newNodesToRanges.put(serverNode, TreeRangeSet.create());

        // actual replace
        for (var node : nodesToRanges.keySet()) {
            newNodesToRanges.putIfAbsent(node, TreeRangeSet.create());
            for (var range : nodesToRanges.get(node).asRanges()) {
                if (replacement.containsKey(range)) {
                    var r1 = replacement.get(range).get(0);
                    var r2 = replacement.get(range).get(1);

                    var rMax = getMax(r1, r2);
                    var rMin = getMin(r1, r2);

                    if (getRangeLength(newNodesToRanges.get(node)) > getRangeLength(newNodesToRanges.get(serverNode))) {
                        addRange(serverNode, newNodesToRanges, newRangesToServerNodes, newRangesToElements, node, rMin, rMax);
                    } else {
                        addRange(serverNode, newNodesToRanges, newRangesToServerNodes, newRangesToElements, node, rMax, rMin);
                    }
                } else {
                    newNodesToRanges.get(node).add(range);
                    newRangesToServerNodes.put(range, node);
                    newRangesToElements.put(range, new HashSet<>());
                }
            }
        }
        // add new server node
        nodesToRanges = newNodesToRanges;
        rangesToServerNodes = newRangesToServerNodes;
        rangesToElements = newRangesToElements;

        log.info("{}", nodesToRanges);
    }

    private void addRange(
        ServerNode serverNode,
        Map<ServerNode, RangeSet<Long>> newNodesToRanges,
        RangeMap<Long, ServerNode> newRangesToServerNodes,
        RangeMap<Long, Set<String>> newRangesToElements,
        ServerNode node, Range<Long> rMax,
        Range<Long> rMin
    ) {
        newNodesToRanges.get(node).add(rMax);
        newRangesToServerNodes.put(rMax, node);
        newRangesToElements.put(rMax, new HashSet<>());

        newNodesToRanges.get(serverNode).add(rMin);
        newRangesToServerNodes.put(rMin, serverNode);
        newRangesToElements.put(rMin, new HashSet<>());
    }

    @Override
    public Map<ServerNode, Long> getRangeDistribution() {
        return nodesToRanges.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> getRangeLength(e.getValue())
            ));
    }

    private long getRangeLength(RangeSet<Long> ranges) {
        return ranges.asRanges().stream()
            .map(r -> r.upperEndpoint() - r.lowerEndpoint())
            .mapToLong(v -> v)
            .sum();
    }

    private Range<Long> getMax(Range<Long> r1, Range<Long> r2) {
        long d1 = r1.upperEndpoint() - r1.lowerEndpoint();
        long d2 = r2.upperEndpoint() - r2.lowerEndpoint();
        if (d1 > d2) {
            return r1;
        } else {
            return r2;
        }
    }

    private Range<Long> getMin(Range<Long> r1, Range<Long> r2) {
        long d1 = r1.upperEndpoint() - r1.lowerEndpoint();
        long d2 = r2.upperEndpoint() - r2.lowerEndpoint();
        if (d1 < d2) {
            return r1;
        } else {
            return r2;
        }
    }

    private SortedSet<Long> generateTokens() {
        var tokens = new TreeSet<Long>();
        for (int i = 0; i < initialTokens; i++) {
            tokens.add(ThreadLocalRandom.current().nextLong(closedRange.lowerEndpoint(), closedRange.upperEndpoint()));
        }
        return tokens;
    }

    private static class ServerNodeWithToken implements Comparable<ServerNodeWithToken> {
        private final ServerNode serverNode;
        private final long token;

        private ServerNodeWithToken(ServerNode serverNode, long token) {
            this.serverNode = serverNode;
            this.token = token;
        }

        @Override
        public int compareTo(@NotNull ServerNodeWithToken o) {
            return Comparator
                .comparing((Function<ServerNodeWithToken, Long>) serverNodeWithToken -> serverNodeWithToken.token)
                .compare(this, o);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ServerNodeWithToken that = (ServerNodeWithToken) o;
            return token == that.token &&
                Objects.equal(serverNode, that.serverNode);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(serverNode, token);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("serverNode", serverNode)
                .add("token", token)
                .toString();
        }
    }
}
