package testing;

import app_kvClient.KVClient;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.ClassRule;
import java.io.*;
import java.lang.*;
import java.util.Random;

public class UITest extends TestCase {
    private KVClient kvClient;
    private ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private int port = 1025 + new Random(1).nextInt(2000);

    public void setUp() {
        System.out.printf("setUp");
        kvClient = new KVClient();
        System.setOut(new PrintStream(outContent));
        kvClient.handleCommand("connect localhost 50000");
    }

    public void tearDown() {
        kvClient.handleCommand("disconnect");
    }

    /**
     *test if print error message when number of parameters is not correct for the put command
     */
    @Test
    public void testPutParameters() {
        kvClient.handleCommand("put");

        assertTrue(outContent.toString().contains("KVClient> Error! Invalid number of parameters!"));
    }

    /**
     *test if print error message when number of parameters is not correct for the get command
     */
    @Test
    public void testGetParameters() {
        kvClient.handleCommand("get foo bar");

        assertTrue(outContent.toString().contains("KVClient> Error! Invalid number of parameters!"));
    }

    /**
     *test if print error message when key exceeds maximum length of the key for the put command
     */
    @Test
    public void testPutKey() {
        kvClient.handleCommand("put fooooooooooooooooooooo bar");

        assertTrue(outContent.toString().contains("KVClient> Error! Maximum length for key is 20 chars;" +
                " Maximum length for value is 120*1024 chars!"));
    }

    /**
     *test if print error message when key exceeds maximum length of the key for the get command
     */
    @Test
    public void testGetKey() {
        kvClient.handleCommand("get fooooooooooooooooooooo");

        assertTrue(outContent.toString().contains("KVClient> Error! Maximum length for key is 20 chars!"));
    }

    /**
     *test if print error message when server is disconnected
     */
    @Test
    public void testDisconnect() {
        tearDown();
        kvClient.handleCommand("put foo bar");

        assertTrue(outContent.toString().contains("KVClient> Error! Not connected!"));
    }
}