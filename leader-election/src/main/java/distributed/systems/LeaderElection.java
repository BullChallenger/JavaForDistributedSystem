package distributed.systems;

import org.apache.zookeeper.ZooKeeper;

public class LeaderElection {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    private ZooKeeper zooKeeper;

    public static void main(String[] args) {
        System.out.println("Hello world!");
    }

    public void connectToZookeeper() {

    }
}