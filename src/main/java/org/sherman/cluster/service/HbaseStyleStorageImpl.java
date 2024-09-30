package org.sherman.cluster.service;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Denis M. Gabaydulin
 * @since 04.03.19
 */
@SuppressWarnings("UnstableApiUsage")
public class HbaseStyleStorageImpl implements HbaseStyleStorage<Long> {
    private static final Logger log = LoggerFactory.getLogger(HbaseStyleStorageImpl.class);

    private final Range<Long> closedRange;
    private final ServerStorage serverStorage;
    private final RangeMap<Long, ServerNode> rangesToServerNodes = TreeRangeMap.create();
    private final long maxElements;
    private final RangeMap<Long, Set<Long>> rangesToElements = TreeRangeMap.create();

    public HbaseStyleStorageImpl(ServerStorage serverStorage, Range<Long> closedRange, long maxElements) {
        Preconditions.checkArgument(closedRange.lowerEndpoint() >= 0, "Range must be positive!");

        this.serverStorage = serverStorage;
        this.closedRange = closedRange;
        this.maxElements = maxElements;

        init();
    }

    private void init() {
        var bound = closedRange.upperEndpoint() / serverStorage.getServers().size();

        log.info("Size: {}", bound);

        var init = closedRange.lowerEndpoint();

        for (var i = 0; i < serverStorage.getServers().size(); i++) {
            var serverNode = serverStorage.getServers().get(i);
            var range = Range.closed(init, i == serverStorage.getServers().size() - 1 ? closedRange.upperEndpoint() : init + bound);
            rangesToServerNodes.put(range, serverNode);
            rangesToElements.put(range, new HashSet<>());

            log.info("{} {}", range.lowerEndpoint(), range.upperEndpoint());

            init = init + bound + 1;
        }
    }

    private void splitRange(Range<Long> range) {
        Preconditions.checkArgument(range.upperEndpoint() - range.lowerEndpoint() > 2, "Can't split region, too small!");

        // split region
        var split = (range.upperEndpoint() + range.lowerEndpoint()) / 2;
        var left = Range.closed(range.lowerEndpoint(), split);
        var right = Range.closed(split + 1, range.upperEndpoint());

        // split elements
        var leftKeys = rangesToElements.get(range.lowerEndpoint()).stream()
            .filter(left::contains)
            .collect(Collectors.toSet());

        var rightKeys = rangesToElements.get(range.lowerEndpoint()).stream()
            .filter(right::contains)
            .collect(Collectors.toSet());

        // update elements
        rangesToElements.remove(range);
        rangesToElements.put(left, leftKeys);
        rangesToElements.put(right, rightKeys);

        // update ranges
        var prevNode = rangesToServerNodes.get(range.lowerEndpoint());
        rangesToServerNodes.remove(range);
        rangesToServerNodes.put(left, prevNode); // keep a half of elements on the prev node

        // getting a random node != prev
        ServerNode node;
        do {
            node = serverStorage.getRandom();
        } while (prevNode.equals(node));

        rangesToServerNodes.put(right, node);
    }

    /**
     * When a range contains more than the maximum number of elements, we should split it into a couple of new ranges.
     */
    @Override
    public void putKey(@NotNull Long key) {
        var keys = rangesToElements.get(key);
        if (keys.size() >= maxElements) {
            splitRange(rangesToElements.getEntry(key).getKey());
        }

        rangesToElements.get(key).add(key);
    }

    @NotNull
    @Override
    public ServerNode getServerNodeByKey(@NotNull Long key) {
        return rangesToServerNodes.get(key);
    }

    @Override
    public Map<ServerNode, Integer> getDistribution() {
        return rangesToServerNodes.asMapOfRanges().entrySet().stream()
            .map(e -> Map.entry(e.getValue(), rangesToElements.getEntry(e.getKey().lowerEndpoint()).getValue().size()))
            .collect(Collectors.groupingBy((Function<Map.Entry<ServerNode, Integer>, ServerNode>) Map.Entry::getKey))
            .entrySet().stream()
            .map((Function<Entry<ServerNode, List<Map.Entry<ServerNode, Integer>>>, Entry<ServerNode, Integer>>) entry ->
                Map.entry(
                    entry.getKey(),
                    entry.getValue().stream().map(Map.Entry::getValue).reduce(0, Integer::sum)
                ))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
}
