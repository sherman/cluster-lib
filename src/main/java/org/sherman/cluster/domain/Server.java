package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class Server {
    private final int id;
    private final LamportClock clock = new LamportClock(1);

    public Server(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Server client = (Server) o;
        return id == client.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .toString();
    }

    public long tick(long ts) {
        return clock.tick(ts);
    }

    public int getId() {
        return id;
    }
}
