package testing;

import app_kvEcs.ECS_Server.ECS_Server;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class InteractionTest {

	private KVStore kvClient;
    public static ECS_Server ECS;

    @BeforeClass
	public static void setUpClass() {
        try {
            FileReader fr = new FileReader("ecs.config");
            BufferedReader br = new BufferedReader(fr);
            ECS = new ECS_Server(br);
            ECS.initNode(3,"50","FIFO");
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
        kvClient = new KVStore("localhost", 5000);
        kvClient.connect();
    }

	@After
	public void tearDown() throws Exception {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

        System.out.printf("\nreceived status "+response.getStatus());
        assertTrue(ex == null);
		assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testNotResponsible() {
		String key = "0.0.0.0:5001";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		System.out.printf("\nreceived status "+response.getStatus());
		assertTrue(ex == null );
        assertTrue(response.getStatus()== StatusType.SERVER_NOT_RESPONSIBLE);
	}


	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "Disconnectfoo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updatefoos";
		String initialValue = "initial";
		String updatedValue = "updated";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);

		} catch (Exception e) {
			ex = e;
		}

		System.out.printf("\nreceived status "+response.getStatus());
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deletefoos";
		String value = "toDelete";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");

		} catch (Exception e) {
			ex = e;
		}

		System.out.printf("\nreceived status "+response.getStatus());
		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
//
	@Test
	public void testGet() {
		String key = "putfoo";
		String value = "bar1";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}

		assertTrue(ex == null && response.getValue().equals(value));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "foo2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		System.out.printf("\nreceived status "+response.getStatus());
		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}


	


}
