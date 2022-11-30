package distributed.systems.cluster.management;

import distributed.systems.cluster.management.OnElectionCallback;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;

    private String currentZnodeName;

    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        // z노드 이름에 붙힐 접두사, z노드에 입력할 데이터, 접근 제어 목록, z노드 생성 모드 (EPHEMERAL_SEQUENTIAL = 주키퍼와의 연결이 끊어지면 해당 z노드 삭제)

        System.out.println("znode name" + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        String predecessorZnodeName = "";
        Stat predecessorStat = null;

        while (predecessorStat == null) { // 리더로 선출되지 않은 상태이거나, 기존 Z노드 중에 실패를 감지할 노드를 뽑을 때까지 반복
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false); // 특정 Z노드 밑에 있는 자식 Z노드들의 이름 목록을 반환

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) { // 리더로 선출된 경우
                System.out.println("내가 리더다!");
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("나는 리더가 아니다..., " + smallestChild + "가 리더다.");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        onElectionCallback.onWorker();

        System.out.println("Watching Znode " + predecessorZnodeName);
        System.out.println();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
        }
    }
}