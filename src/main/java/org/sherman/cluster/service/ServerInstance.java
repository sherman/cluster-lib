package org.sherman.cluster.service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.sherman.cluster.domain.Client;
import org.sherman.cluster.domain.Server;
import org.sherman.cluster.domain.StateMessage;
import org.sherman.cluster.domain.VersionedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerInstance {
    private static final Logger logger = LoggerFactory.getLogger(ServerInstance.class);

    private final Server server;
    private final ConcurrentMap<Integer, CopyOnWriteArrayList<VersionedData>> messages = new ConcurrentHashMap<>();

    public ServerInstance(Server server) {
        this.server = server;
    }

    public void receiveData(Client client, StateMessage message) {
        var newTs = server.tick(message.getTs());
        logger.info("Ts: [{}] for server: [{}], msg: [{}]", newTs, server.getId(), client + ":" + message);
        var clientMessages = messages.computeIfAbsent(client.getId(), ignored -> new CopyOnWriteArrayList<>());
        clientMessages.add(new VersionedData(message.getSessionId(), newTs, message.getData()));
        client.updateTs(newTs);
    }

    public List<VersionedData> getMessagesByClient(int clientId) {
        return messages.get(clientId);
    }
}
