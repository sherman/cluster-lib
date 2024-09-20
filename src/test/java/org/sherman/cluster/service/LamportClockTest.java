package org.sherman.cluster.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.sherman.cluster.domain.Client;
import org.sherman.cluster.domain.Server;
import org.sherman.cluster.domain.StateMessage;
import org.sherman.cluster.domain.VersionedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class LamportClockTest {
    private static final Logger logger = LoggerFactory.getLogger(LamportClockTest.class);

    @Test
    public void case1() {
        ServerInstance serverInstance1 = new ServerInstance(new Server(1));
        ServerInstance serverInstance2 = new ServerInstance(new Server(2));
        Client client1 = new Client(1, 1);
        Client client2 = new Client(1, 2);

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg11"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg12"));
        serverInstance1.receiveData(client2, new StateMessage(client1.getSessionId(), client2.inc(), "msg21"));
        serverInstance2.receiveData(client2, new StateMessage(client1.getSessionId(), client2.inc(), "msg22"));

        printTotalOrder(List.of(client1, client1), List.of(serverInstance1, serverInstance2));
    }

    @Test
    public void case2() {
        ServerInstance serverInstance1 = new ServerInstance(new Server(1));
        ServerInstance serverInstance2 = new ServerInstance(new Server(2));
        Client client1 = new Client(1, 1);

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg11"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg12"));

        client1.restart();

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg13"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg14"));

        printTotalOrder(List.of(client1), List.of(serverInstance1, serverInstance2));
    }

    @Test
    public void case3() {
        ServerInstance serverInstance1 = new ServerInstance(new Server(1));
        ServerInstance serverInstance2 = new ServerInstance(new Server(2));
        ServerInstance serverInstance3 = new ServerInstance(new Server(3));
        Client client1 = new Client(1, 1);

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg11"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg12"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg13"));

        client1.restart();

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg14"));

        client1.restart();

        serverInstance1.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg15"));

        client1.restart();

        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg16"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg17"));
        serverInstance2.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg18"));
        serverInstance3.receiveData(client1, new StateMessage(client1.getSessionId(), client1.inc(), "msg19"));

        printTotalOrder(List.of(client1), List.of(serverInstance1, serverInstance2, serverInstance3));
    }

    private static void printTotalOrder(List<Client> clients, List<ServerInstance> servers) {
        for (Client client : clients) {
            logger.info("==========================================");
            List<VersionedData> messages = new ArrayList<>();
            for (ServerInstance serverInstance : servers) {
                messages.addAll(serverInstance.getMessagesByClient(client.getId()));
            }

            List<VersionedData> sorted = messages.stream().sorted(
                (o1, o2) -> Comparator.comparing(VersionedData::getSessionId)
                    .thenComparing(VersionedData::getTs)
                    .compare(o1, o2)
            ).toList();

            for (VersionedData message : sorted) {
                logger.info("[{}]", message);
            }
        }
    }
}
