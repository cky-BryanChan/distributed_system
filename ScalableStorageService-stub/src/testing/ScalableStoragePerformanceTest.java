package testing;

import app_kvEcs.ECS_Server.ECS_Server;
import app_kvServer.CacheStorage;
import app_kvServer.KVServer;
import app_kvServer.model.Metadata;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.net.SyslogAppender;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Only used for performance evaluation, will be executed independently
 * Should not be added to AllTests.java testsuite
 * Each test will reset cacheStorage
 * use ThreadLocalRandom.current().nextInt(min, max + 1) to genearate random get key
 * All results showed in microsecond (ms) 1e-6 of second
 */
public class ScalableStoragePerformanceTest extends TestCase {
    private KVStore[] kvClient;
    public static ECS_Server ECS;
    private HashMap<String, String> data;

    public void setUp() {
        File folder = new File("/Users/Apple/Desktop/maildir");
        try {
            System.out.println("create dataset");
            data = DataSet.storeDataSet(folder);
            System.out.println("dataset created");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private CacheStorage cache;

    public void tearDown(){
        if (cache != null){
            cache.reset();
        }
    }

    @Test
    public void testFIFO(){

        final String strategy = "FIFO";
        testDifferentClient(strategy);
    }

    @Test
    public void testLFU(){
        final String strategy = "LFU";
        testDifferentClient(strategy);
    }

    @Test
    public void testLRU(){
        final String strategy = "LRU";
        testDifferentClient(strategy);
    }

    public void testDifferentClient(String strategy){
        testDifferentServer(strategy, 1);
        testDifferentServer(strategy, 5);
        testDifferentServer(strategy, 20);
        testDifferentServer(strategy, 50);
        testDifferentServer(strategy, 100);
    }

    public void testDifferentServer(String strategy, int client){
        //testDifferentCache(strategy, client, 1);
        //testDifferentCache(strategy, client, 5);
        //testDifferentCache(strategy, client, 20);
        testDifferentCache(strategy, client, 50);
        //testDifferentCache(strategy, client, 100);

    }

    public void testDifferentCache(String strategy, int client, int server){
        testStrategy(strategy,client, server, 10000, 100);
        //testStrategy(strategy,client, server, 10000, 1000);
    }

    public void testStrategy(String strategy, int client, int server, int dataNum, int cacheSize){
        try {
            ECS = null;
            FileReader fr = new FileReader("ecs.config");
            BufferedReader br = new BufferedReader(fr);
            ECS = new ECS_Server(br);
            ECS.initNode(server, Integer.toString(cacheSize), strategy);
            // MUST s
            Thread.sleep(1000);
            ECS.startNode();
            kvClient = new KVStore[client];
            for(int i=0; i<client; i++) {
                kvClient[i] = new KVStore("localhost", 5000);
                kvClient[i].connect();
            }
            fr.close();

        } catch (Exception e) {
            System.out.println(e);
            //e.printStackTrace();
        }

        try {
            new LogSetup("logs/testing/PerformanceTest.log", Level.ERROR);
      //      CacheStorage.init(strategy, cacheSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //cache = CacheStorage.getInstance();
        //System.out.println("cache size: " + cacheSize);

        long start_time, end_time, total;
        start_time = System.nanoTime();
        for(int i=0; i<client; i++) {
            int stop = 0;
            Iterator<?> it = data.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
                if (stop == dataNum) break;
                stop++;
                try {
                    kvClient[i].put(pairs.getKey(), pairs.getValue());

                    assertEquals(pairs.getValue(), kvClient[i].get(pairs.getKey()).getValue());
                } catch (Exception e) {
                    //e.printStackTrace();
                }
            }
        }

        end_time = System.nanoTime();
        total = (end_time-start_time)/1000;

        System.out.println(strategy +": client#:"+ client + " server#:"+server+ " data#:"+dataNum+ " cache:"+cacheSize);
        System.out.println("Took: "+ Long.toString(total)+"ms");

        try{
            for(int i=0; i<client; i++) {
                kvClient[i].disconnect();
            }
            ECS.shutDownNode();
            Thread.sleep(500);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}

