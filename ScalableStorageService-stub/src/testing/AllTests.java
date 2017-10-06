package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			//use a default of 128 keys, FIFO replacement
			new KVServer(50000).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(AddRemoveNodeTest.class);
        clientSuite.addTestSuite(UITest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(CacheTest.class);
		clientSuite.addTestSuite(DiskStorageTest.class);
        clientSuite.addTestSuite(MetadataTest.class);
		clientSuite.addTestSuite(CacheTest.class);
		//clientSuite.addTestSuite(ECS_ServerTest.class);
		return clientSuite;
	}
	
}
