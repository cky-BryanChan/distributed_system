package app_kvEcs.ECS_Server;

/**
 * Created by bryanchan on 3/3/17.
 */

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.zookeeper.*;
import java.io.IOException;

public class zkConnector {

    private ZooKeeper zk;
    private java.util.concurrent.CountDownLatch connSignal = new java.util.concurrent.CountDownLatch(1);
    public ZooKeeper connect(String host) throws IOException, IllegalStateException, InterruptedException {
        new LogSetup("logs/zookeep.log", Level.WARN);
        zk = new ZooKeeper(host, 2000, new Watcher(){
            public void process(WatchedEvent event){
                if (event.getState() == Event.KeeperState.SyncConnected){
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
        return zk;
    }

    public void close() throws InterruptedException{
        zk.close();
    }

}
