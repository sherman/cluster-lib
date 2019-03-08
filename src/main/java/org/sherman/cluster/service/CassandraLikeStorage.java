package org.sherman.cluster.service;

import org.jetbrains.annotations.NotNull;
import org.sherman.cluster.domain.ServerNode;

import java.util.Map;

/**
 * @author Denis M. Gabaydulin
 * @since 08.03.19
 */
public interface CassandraLikeStorage<T extends Comparable<T>> {
    void putKey(@NotNull String key);

    @NotNull
    ServerNode getServerNodeByKey(@NotNull String key);

    Map<ServerNode, Integer> getDistribution();
}
