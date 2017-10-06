package testing;

import app_kvEcs.ECS_Server.ECS_Server;
import junit.framework.TestCase;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ECS_ServerTest extends TestCase{
    private static ECS_Server ecs_server;



    static {
        try {
            FileReader ffr = new FileReader("ecs.config");
            ecs_server = new ECS_Server(new BufferedReader(ffr));
            ffr.close();
        } catch (IOException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testInitTwice() throws InterruptedException, IOException, KeeperException {
        FileReader ffr = new FileReader("ecs.config");

        ecs_server = new ECS_Server(new BufferedReader(ffr));
        String cacheSize = "100", stratgy = "FIFO";

        boolean once = ecs_server.initNode(1,cacheSize,stratgy);
        boolean twice = ecs_server.initNode(1,cacheSize,stratgy);
        assertTrue(once && !twice);
        ecs_server.shutDownNode();
    }

    public void testPostInitList() throws IOException, KeeperException, InterruptedException {
        FileReader ffr = new FileReader("ecs.config");
        BufferedReader reader = new BufferedReader(ffr);
        int lines = 0;
        while (reader.readLine() != null) lines++;
        reader.close();

        ffr = new FileReader("ecs.config");
        ecs_server = new ECS_Server(new BufferedReader(ffr));
        String cacheSize = "100", stratgy = "FIFO";
        boolean once = ecs_server.initNode(2,cacheSize,stratgy);
        ArrayList<String> active = ecs_server.getActive();
        ArrayList<String> idle = ecs_server.getIdle();
        assertTrue( active.size() == 2 );
        assertTrue( idle.size() == (lines - 2) );
        ffr.close();
        ecs_server.shutDownNode();
    }


    public void testAddNode() throws IOException, KeeperException, InterruptedException {
        FileReader ffr = new FileReader("ecs.config");

        ecs_server = new ECS_Server(new BufferedReader(ffr));
        String cacheSize = "100", stratgy = "FIFO";
        boolean once = ecs_server.initNode(3,cacheSize,stratgy);
        int active_old = ecs_server.getActive().size();
        ecs_server.startNode();
        ecs_server.addNode(cacheSize,stratgy);
        int active_new = ecs_server.getActive().size();
        assertTrue( active_old + 1 == active_new );
        ffr.close();
        ecs_server.shutDownNode();
    }


    public void testRemoveNode() throws IOException, KeeperException, InterruptedException {
        FileReader ffr = new FileReader("ecs.config");

        ecs_server = new ECS_Server(new BufferedReader(ffr));
        String cacheSize = "100", stratgy = "FIFO";
        boolean once = ecs_server.initNode(3,cacheSize,stratgy);
        int active_old = ecs_server.getActive().size();
        ecs_server.startNode();
        ecs_server.removeNode(1);
        int active_new = ecs_server.getActive().size();
        assertTrue( active_old - 1 == active_new );
        ffr.close();
        ecs_server.shutDownNode();
    }
}
