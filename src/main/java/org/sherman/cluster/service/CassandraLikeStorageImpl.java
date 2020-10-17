package org.sherman.cluster.service;

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

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.TreeRangeSet;
import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;
import org.sherman.cluster.util.Tokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Denis M. Gabaydulin
 * @since 08.03.19
 */
public class CassandraLikeStorageImpl implements CassandraLikeStorage<Long> {
    private static final Logger log = LoggerFactory.getLogger(CassandraLikeStorageImpl.class);

    private final Range<Long> closedRange;
    private final ServerStorage serverStorage;

    private final Map<ServerNode, RangeSet<Long>> nodesToRanges = new HashMap<>();
    private final RangeMap<Long, ServerNode> rangesToServerNodes = TreeRangeMap.create();
    private final RangeMap<Long, Set<String>> rangesToElements = TreeRangeMap.create();

    private final int initialTokens;

    public CassandraLikeStorageImpl(ServerStorage serverStorage, Range<Long> closedRange, int initialTokens) {
        this.serverStorage = serverStorage;
        this.closedRange = closedRange;

        this.initialTokens = initialTokens;

        init();
    }

    private void init() {
        SortedSet<ServerNodeWithToken> serversWithTokens = new TreeSet<>();

        for (ServerNode serverNode : serverStorage.getServers()) {
            Set<Long> tokens = generateTokens();

            tokens.forEach(t -> serversWithTokens.add(new ServerNodeWithToken(serverNode, t)));

            nodesToRanges.put(serverNode, TreeRangeSet.create());
        }

        Long prevToken = null;
        for (ServerNodeWithToken serverNodeWithToken : serversWithTokens) {
            if (prevToken == null) {
                prevToken = closedRange.lowerEndpoint();
            }

            Range<Long> range = Range.closed(prevToken, serverNodeWithToken.token);

            nodesToRanges.get(serverNodeWithToken.serverNode).add(range);
            rangesToServerNodes.put(range, serverNodeWithToken.serverNode);
            rangesToElements.put(range, new HashSet<>());

            prevToken = serverNodeWithToken.token + 1;
        }

        ServerNode serverNode = serverStorage.getRandom();
        Range<Long> range = Range.closed(prevToken, Long.MAX_VALUE);
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
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getValue(), rangesToElements.getEntry(e.getKey().lowerEndpoint()).getValue().size()))
            .collect(Collectors.groupingBy((Function<AbstractMap.SimpleImmutableEntry<ServerNode, Integer>, ServerNode>) AbstractMap.SimpleImmutableEntry::getKey))
            .entrySet().stream()
            .map((Function<Map.Entry<ServerNode, List<AbstractMap.SimpleImmutableEntry<ServerNode, Integer>>>, Map.Entry<ServerNode, Integer>>) entry -> new AbstractMap.SimpleImmutableEntry<>(
                entry.getKey(),
                entry.getValue().stream().map(AbstractMap.SimpleImmutableEntry::getValue).reduce(0, (a, b) -> a + b)
            ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private SortedSet<Long> generateTokens() {
        SortedSet<Long> tokens = new TreeSet<>();
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
