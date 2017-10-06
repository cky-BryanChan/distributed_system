package testing;

import app_kvEcs.ECS_Server.ECS_Server;
import client.Client;
import client.KVStore;
import common.messages.KVMessage;
import org.apache.zookeeper.KeeperException;
import org.junit.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Created by apple on 2017/3/26.
 */
public class dataRecoveryTest {
    private Client client;
    public static ECS_Server ECS;

    @BeforeClass
    public static void setUpClass() {
        try {
            FileReader fr = new FileReader("ecs.config");
            BufferedReader br = new BufferedReader(fr);
            ECS = new ECS_Server(br);
            ECS.initNode(5,"50","FIFO");
            Thread.sleep(1000);
            ECS.startNode();
            fr.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @AfterClass
    public static void tearDownClass() throws InterruptedException, IOException, KeeperException {
        ECS.shutDownNode();
    }


    @Before
    public void setUp() throws IOException {
        client = new Client("localhost", 5000);
        client.connect();
    }

    @After
    public void tearDown() throws Exception {
        client.disconnect();
    }

    @Test
    public void testDataRecovery() {
        String key = "testDataRecovery";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            ECS.removeNode(1);
            Thread.sleep(1000);
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testDataMove() {
        String key = "333";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            ECS.addNode("50", "FIFO");
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testDataMove2() {
        String key = "33333";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            ECS.addNode("50", "FIFO");
            ECS.addNode("50", "FIFO");
            ECS.addNode("50", "FIFO");
            ECS.addNode("50", "FIFO");
            ECS.addNode("50", "FIFO");
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testPut() {
        String key = "testPut";
        String value = "Put";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = client.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS);
    }

    @Test
    public void testUpdate() {
        String key = "testUpdate";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, initialValue);
            response = client.put(key, updatedValue);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    @Test
    public void testDelete() {
        String key = "testDelete";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            response = client.put(key, "null");

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "TestGet";
        String value = "get";
        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("get"));
    }
}
