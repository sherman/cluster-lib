package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class SearchShard {
    private final String index;
    private final int id;

    public SearchShard(String index, int id) {
        this.index = index;
        this.id = id;
    }

    public String getIndex() {
        return index;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchShard that = (SearchShard) o;
        return id == that.id && Objects.equal(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("index", index)
            .add("id", id)
            .toString();
    }
}
