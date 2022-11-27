package distributed.systems;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    private static final int SESSION_TIMEOUT = 3000;

    private static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;

    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("응용 프로그램을 종료합니다.");
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // z노드 이름에 붙힐 접두사, z노드에 입력할 데이터, 접근 제어 목록, z노드 생성 모드 (EPHEMERAL_SEQUENTIAL = 주키퍼와의 연결이 끊어지면 해당 z노드 삭제)

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children =  zooKeeper.getChildren(ELECTION_NAMESPACE, false); // 특정 Z노드 밑에 있는 자식 Z노드들의 이름 목록을 반환

        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnodeName)) { // 리더로 선출된 경우
            System.out.println("내가 리더다!");
            return;
        }

        System.out.println("나는 리더가 아니다..., " + smallestChild + " 가 리더다.");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
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
                    System.out.println("주키퍼와의 연결이 해제되었습니다.");
                    zooKeeper.notifyAll();
                }
        }
    }
}