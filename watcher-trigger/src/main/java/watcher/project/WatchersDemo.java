package watcher.project;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class WatchersDemo implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    private static final int SESSION_TIMEOUT = 3000;

    private static final String TARGET_ZNODE = "/target_znode";

    private ZooKeeper zooKeeper;

    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        WatchersDemo watchersDemo = new WatchersDemo();

        watchersDemo.connectToZookeeper();
        watchersDemo.watchTargetZnode();
        watchersDemo.run();
        watchersDemo.close();
        System.out.println("응용 프로그램을 종료합니다.");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws InterruptedException, KeeperException { // 특정 이벤트에 대한 워처 등록

        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);

        if (stat == null) {
            return;
        }

        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data : " + new String(data) + ", children : " + children);
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("주키퍼 서버와 성공적으로 연결되었습니다.");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("주키퍼와의 연결이 해제되었습니다.");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + "가 삭제되었습니다.");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + "가 생성되었습니다.");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + "의 데이터가 변경되었습니다");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + "의 자식 노드가 변경되었습니다.");
                break;
        }

        try {
            watchTargetZnode(); // 변경이 발생 되었을 때, 최신 데이터를 화면에 출력하기 위함, 일회성 이벤트이기에 다시 등록
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }
}