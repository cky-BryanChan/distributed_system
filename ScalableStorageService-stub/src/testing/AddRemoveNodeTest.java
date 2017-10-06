package testing;

import app_kvClient.KVClient;
import app_kvEcs.ECS_Server.ECS_Server;
import app_kvServer.KVServer;
import app_kvServer.model.Metadata;
import client.Client;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by apple on 2017/3/5.
 */
public class AddRemoveNodeTest {
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
    public void testPut() {
        String key = "putfoo";
        String value = "putbar";
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
        String key = "updateTestValue";
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
        String key = "deleteTestValue";
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
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            client.put(key, value);
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = client.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.GET_ERROR);
    }

    @Test
    public void testAddNode() throws InterruptedException, IOException, KeeperException {
        ECS.addNode("50", "FIFO");
        try {
            client.put("errrr", "weeee");
            assertEquals("weeee", client.get("errrr").getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteNode() throws InterruptedException, IOException, KeeperException {
        ECS.removeNode(1);
        try {
            client.connect();
            client.put("errrr", "weeee");
            assertEquals("weeee", client.get("errrr").getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}

