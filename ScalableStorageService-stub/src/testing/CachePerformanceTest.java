package testing;

import app_kvServer.CacheStorage;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.net.SyslogAppender;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Only used for performance evaluation, will be executed independently
 * Should not be added to AllTests.java testsuite
 * Each test will reset cacheStorage
 * use ThreadLocalRandom.current().nextInt(min, max + 1) to genearate random get key
 * All results showed in microsecond (ms) 1e-6 of second
 */
public class CachePerformanceTest extends TestCase {


    private CacheStorage cache;

    public void tearDown(){
        if (cache != null){
            cache.reset();
        }
    }

    @Test
    public void testFIFO(){

        final String strategy = "FIFO";
        testStrategy(strategy);
    }

    @Test
    public void testLFU(){
        final String strategy = "LFU";
        testStrategy(strategy);
    }

    @Test
    public void testLRU(){
        final String strategy = "LRU";
        testStrategy(strategy);
    }

    public void testStrategy(String strategy){
        int num_of_insert , num_of_query;
        long start_time, end_time, total;
        HashMap<String,String> putPairs = new HashMap<>();
        List<String> getList = new ArrayList<>();

        final int cache_size = 1000;
        try {
            new LogSetup("logs/testing/PerformanceTest.log", Level.ERROR);
            CacheStorage.init(strategy,cache_size);
        } catch (IOException e) {
            e.printStackTrace();
        }
        cache = CacheStorage.getInstance();
        System.out.println("cache siez: " + cache_size );


        //100%/0%
        num_of_insert = 1000;
        num_of_query = 0;
        putPairs = addtoPutPair(1000,putPairs);

        start_time = System.nanoTime();
        for (Map.Entry<String,String> entry: putPairs.entrySet()){
            cache.store(entry.getKey(),entry.getValue());
        }
        end_time = System.nanoTime();
        total = (end_time-start_time)/1000;
        System.out.println(strategy +": put "+ Integer.toString((num_of_insert)) + " pairs, get "+Integer.toString(num_of_query)+ " pairs");
        System.out.println("Took: "+ Long.toString(total)+"ms");

        // 80%/20%
        num_of_insert = 800;
        num_of_query = 200;
        putPairs.clear();
        getList.clear();
        cache.purgeAll();
        putPairs = addtoPutPair(num_of_insert,putPairs);
        getList = addtoGetList(num_of_query,getList,1,num_of_insert);

        start_time = System.nanoTime();
        for (Map.Entry<String,String> entry: putPairs.entrySet()){
            cache.store(entry.getKey(),entry.getValue());
        }
        //Note that there is no performance penalty for using the for-each loop, even for arrays.
        //In fact, it may offer a slight performance advantage over an ordinary for loop in some circumstances
        for (String query:getList){
            cache.retrieve(query);
        }
        end_time = System.nanoTime();
        total = (end_time-start_time)/1000;
        System.out.println(strategy +": put "+ Integer.toString((num_of_insert)) + " pairs, get "+Integer.toString(num_of_query)+ " pairs");
        System.out.println("Took: "+ Long.toString(total)+"ms");

        // 50%/50%
        num_of_insert = 500;
        num_of_query = 500;
        putPairs.clear();
        getList.clear();
        cache.purgeAll();
        putPairs = addtoPutPair(num_of_insert,putPairs);
        getList = addtoGetList(num_of_query,getList,1,num_of_insert);

        start_time = System.nanoTime();
        for (Map.Entry<String,String> entry: putPairs.entrySet()){
            cache.store(entry.getKey(),entry.getValue());
        }
        for (String query:getList){
            cache.retrieve(query);
        }
        end_time = System.nanoTime();
        total = (end_time-start_time)/1000;
        System.out.println(strategy +": put "+ Integer.toString((num_of_insert)) + " pairs, get "+Integer.toString(num_of_query)+ " pairs");
        System.out.println("Took: "+ Long.toString(total)+"ms");
    }

    public HashMap<String,String> addtoPutPair (int num_of_insert, HashMap<String,String> putPairs){
        int counter = 1;
        while (counter <= num_of_insert){
            String key = Integer.toString(counter);
            //avoid using String Concatenation like (a+b), StringBuilder instead, and we should count that part of time
            String val = Integer.toString(counter+1);
            counter ++;
            putPairs.put(key,val);
        }
        return putPairs;
    }

    public List<String> addtoGetList(int num_of_query, List<String> getList, int min, int max){
        int counter = 1;
        while(counter <= num_of_query){
            int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
            String key = Integer.toString(randomNum);
            getList.add(key);
            counter ++;
        }
        return getList;
    }

}

