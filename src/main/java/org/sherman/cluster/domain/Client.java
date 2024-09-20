package org.sherman.cluster.domain;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class Client {
    private final int id;
    private int sessionId; // tie breaker
    private final LamportClock clock = new LamportClock(1);

    public Client(int sessionId, int id) {
        this.sessionId = sessionId;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void restart() {
        clock.restart();
        sessionId++;
    }

    public void updateTs(long newTs) {
        clock.tick(newTs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Client client = (Client) o;
        return id == client.id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("sessionId", sessionId)
            .add("id", id)
            .toString();
    }

    public long inc() {
        return clock.inc();
    }

    public int getSessionId() {
        return sessionId;
    }
}
