package distributed.systems;

import distributed.systems.cluster.management.OnElectionCallback;
import distributed.systems.cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {

    private final ServiceRegistry serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {

        try {
            serviceRegistry.unRegisterFromCluster();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
        serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {;

        try {
            String currentServerAddress =
                    String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);

            serviceRegistry.registerToCluster(currentServerAddress);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
