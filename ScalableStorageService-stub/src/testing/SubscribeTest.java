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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SubscribeTest {
    private KVClient kvClient;
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
    public void setUp() throws Exception {
        kvClient = new KVClient();
        kvClient.handleCommand("connect 0.0.0.0 5000");
    }

    @After
    public void tearDown() throws Exception {
        kvClient.handleCommand("disconnect");
    }

    @Test
    public void testSub() {
        Exception ex = null;

        try {
            kvClient.handleCommand("put foo bar");
            Thread.sleep(1000);
            kvClient.handleCommand("sub foo");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(kvClient.getScribers().size() == 1);
    }

    @Test
    public void testMultiSub() {
        Exception ex = null;

        try {
            kvClient.handleCommand("put foo bar");
            Thread.sleep(1000);
            kvClient.handleCommand("sub foo");
            Thread.sleep(1000);
            kvClient.handleCommand("put test test");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test");
            Thread.sleep(1000);
            kvClient.handleCommand("put test2 test2");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test2");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(kvClient.getScribers().size() == 3);
    }

    @Test
    public void testUnsub() {
        Exception ex = null;

        try {
            kvClient.handleCommand("put foo bar");
            Thread.sleep(1000);
            kvClient.handleCommand("sub foo");
            Thread.sleep(1000);
            kvClient.handleCommand("put test test");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test");
            Thread.sleep(1000);
            kvClient.handleCommand("put test2 test2");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test2");
            Thread.sleep(1000);
            kvClient.handleCommand("unsub test");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(kvClient.getScribers().size() == 2);
    }

    @Test
    public void testUnsubAll() {
        Exception ex = null;

        try {
            kvClient.handleCommand("put foo bar");
            Thread.sleep(1000);
            kvClient.handleCommand("sub foo");
            Thread.sleep(1000);
            kvClient.handleCommand("put test test");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test");
            Thread.sleep(1000);
            kvClient.handleCommand("put test2 test2");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test2");
            Thread.sleep(1000);
            kvClient.handleCommand("unsub all");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(kvClient.getScribers().size() == 0);
    }

    @Test
    public void testSubKeyDelete() {
        Exception ex = null;

        try {
            kvClient.handleCommand("put foo bar");
            Thread.sleep(1000);
            kvClient.handleCommand("sub foo");
            Thread.sleep(1000);
            kvClient.handleCommand("put test test");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test");
            Thread.sleep(1000);
            kvClient.handleCommand("put test2 test2");
            Thread.sleep(1000);
            kvClient.handleCommand("sub test2");
            Thread.sleep(1000);
            kvClient.handleCommand("put foo");
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(kvClient.getScribers().size() == 2);
    }
}
