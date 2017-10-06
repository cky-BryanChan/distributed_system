package app_kvEcs.ECS_Server;

import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by bryanchan on 3/24/17.
 */
public class ECS_MAIN {
    public static ECS_Server ECS;

    public static void main(String arg[]) throws IOException, InterruptedException, KeeperException {
        FileReader fr = new FileReader("ecs.config");
        BufferedReader br = new BufferedReader(fr);
        ECS = new ECS_Server(br);
        fr.close();
        ECS.run();
    }

}
