package app_kvServer;

import app_kvServer.model.Range;
import app_kvServer.model.Record;
import common.messages.KVMessage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *  in memory data maintenance
 *  defined as singleton class
 *  since there will only be 1 cache for 1 server no matter how many client connections
 */
public class CacheStorage {

    private static Logger logger = Logger.getLogger(CacheStorage.class);

    private static CacheStorage singleton = null;
    private String strategy;
    private int recordsLimit;

    private ConcurrentHashMap<String,Record> cache;

    private CacheStorage(String strategy, int recordsLimit) {
        this.strategy = strategy;
        this.recordsLimit = recordsLimit;
        this.cache = new ConcurrentHashMap<>(recordsLimit);
    }

    /**
     * Must be called after CacheStorage.init()
     * @return singleton instate of cache
     */
    public static CacheStorage getInstance(){
        if (singleton == null){
            throw new AssertionError("CacheStorage has to been init before use");
        }
        return singleton;
    }

    /**
     * Initialization of CacheStorage, should only be called once
     * @param strategy  'FIFO', 'LRU' or 'LFU'
     * @param recordsLimit number of records can be stored into cache
     * @return singleton instance of CacheStorage
     */
    public synchronized static CacheStorage init(String strategy, int recordsLimit){
        if (singleton != null){
            throw new AssertionError("CacheStorage has already been init before!");
        }
        singleton = new CacheStorage(strategy,recordsLimit);
        logger.info("CacheStorage has been created");
        return singleton;
    }

    /**
     * Only used by PerformanceTest, this API should not be visible/used for any other purpose
     */
    public static void reset(){
        singleton = null;
    }


    /**
     * Assume unique port number
     * @param key
     * @param port
     * @param host
     * @return
     */
    public boolean subscribe(String key, int port, String host){
        if (cache.containsKey(key)){
            Record record = cache.get(key);
            HashMap<Integer,String> subscriber = record.getSubscribers();
            assert subscriber != null;
                subscriber.put(port,host);
                record.setSubscribers(subscriber);
        } else {
            return false;
        }
        return true;
    }

    public boolean unsubscribe(String key, int port, String host){
        if (cache.containsKey(key)) {
            Record record = cache.get(key);
            HashMap<Integer, String> subscriber = record.getSubscribers();
            assert subscriber != null;
            if (subscriber.containsKey(port) && subscriber.get(port).equals(host)) {
                subscriber.remove(port);
                record.setSubscribers(subscriber);
                return true;
            }
        }
        return false;
    }

    public boolean worthBroadcast(String key){
        return cache.containsKey(key) && cache.get(key).getSubscribers().size() > 0;
    }

    public HashMap<Integer, String> getSubscriberfromCache(String key){
        return cache.get(key).getSubscribers();
    }
    /**
     * Store into cache,
     *  if below limit, insert for unseen key, update for existing key
     *  else, sort cache with respect to replacement strategy, will evict the tail of list to persistent storage
     *        then insert the new record to cache
     *
     * @param key
     * @param value
     * @return
     */
    public KVMessage.StatusType store(String key, String value){
        // if below limit
        if (cache.size() < recordsLimit || cache.containsKey(key)){
            long currTime = System.currentTimeMillis();
            Record record;
            if (!cache.containsKey(key)){  //insert
                record = new Record(currTime,1,currTime,value,key);
                cache.put(key,record);
                return KVMessage.StatusType.PUT_SUCCESS;
            }
            else{ //update
                record = cache.get(key);
                record.setFrequency(record.getFrequency()+1);
                record.setLastAccess(currTime);
                record.setValue(value);
                return KVMessage.StatusType.PUT_UPDATE;
            }
        }
        // if above limit, need replace cache entry (eliminate tail)
        // FIFO sort by time stamp DESC,
        // LFU sort by frequency DESC
        // LRU sort by lastAccess DESC
        else{
            List<Record> records;
            if (strategy.equals(Constant.FIFO)){
                records = new ArrayList<>(cache.values());
                Collections.sort(records, new Comparator<Record>() {
                    @Override
                    public int compare(Record o1, Record o2) {
                        return (int) (o2.getTimeStamp() - o1.getTimeStamp());
                    }
                });
            }
            else if (strategy.equals(Constant.LFU)){
                records = new ArrayList<>(cache.values());
                Collections.sort(records, new Comparator<Record>() {
                    @Override
                    public int compare(Record o1, Record o2) {
                        return o2.getFrequency() - o1.getFrequency();
                    }
                });
            }
            else { //LRU
                records = new ArrayList<>(cache.values());
                Collections.sort(records, new Comparator<Record>() {
                    @Override
                    public int compare(Record o1, Record o2) {
                        return (int) (o2.getLastAccess() - o1.getLastAccess());
                    }
                });
            }

            // now the tail of list is victim to be evicted, need to remove from our cahce
            Record victimRecord = records.get(records.size()-1);
            String victimKey = victimRecord.getReverseKey();
            cache.remove(victimKey);
            DiskStorage.getInstance().set(victimKey,victimRecord.getValue(),0);

            // Insert latest to cache
            long currTime = System.currentTimeMillis();
            Record record = new Record(currTime,1,currTime,value,key);
            cache.put(key,record);
        }
        return KVMessage.StatusType.PUT_SUCCESS;
    }

    /**
     * cache retrieval
     *  if found in cache, simply return and increment frequency and update last access
     *  else look up in persistent storage, if not found, return with error
     * @param key
     * @return
     */
    public String retrieve(String key){
        // records exist in our cache
        if (cache.containsKey(key)){
            Record target = cache.get(key);
            target.setFrequency(target.getFrequency() + 1);
            target.setLastAccess(System.currentTimeMillis());
            return target.getValue();
        }
        else{
            String diskResp = DiskStorage.getInstance().get(key,0);
            if (diskResp.equals(Constant.KEY_NOT_FOUND) || diskResp.equals("null")){
                return Constant.retrieveErrorMsg;
            } else {
                // on found, put back to cache
                store(key,diskResp);
                return diskResp;
            }
        }
    }

    public HashMap<Integer,String> purge(String key){
        if (cache.containsKey(key)){
            HashMap<Integer,String> subscriberList = cache.get(key).getSubscribers();
            cache.remove(key);
            logger.debug(String.format("Key %s has been removed from cacheStorage",key));
            assert subscriberList != null;
            return subscriberList;
        }
        else{
            DiskStorage.getInstance().set(key,"null",0);
            return null;
        }
    }

    public void purgeAll(){
        cache.clear();
    }

    public void addAll(ConcurrentHashMap<String, String> m){
        Iterator<Map.Entry<String,String>> it = m.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String,String> pair = it.next();
            String key = pair.getKey(), value = pair.getValue();
            long currTime = System.currentTimeMillis();
            Record record = new Record(currTime,1,currTime,value,key);
            cache.put(key,record);
        }
    }

    /**
     * Filter the data within range,
     * add to new HashMap <String,String> fromCache, metadata no need
     * combine with fromDisk and can be moved
     * assume all the keys in cache is already hashed to md5
     */
    public ConcurrentHashMap<String, String> preMove( Range range){
        ConcurrentHashMap<String, String> fromCache = new ConcurrentHashMap<>(recordsLimit);
        for (Map.Entry<String,Record> pair: cache.entrySet()){
            if (range.belongs(pair.getKey())){
                fromCache.put(pair.getKey(),pair.getValue().getValue());
            }
        }
        return fromCache;
    }

    /**
     * Once the moveData has been completed, delete the intersection of 2 hashmap
     * @param fromCache returned by preMove
     * @return
     */
    public boolean deletePartial(ConcurrentHashMap<String,String> fromCache){
        Iterator iter_fromCache = fromCache.keySet().iterator();
        while (iter_fromCache.hasNext()){
            String k = (String) iter_fromCache.next();
            purge(k);
        }
        return true;
    }

    public ConcurrentHashMap<String,String> getAll() {
        ConcurrentHashMap<String, String> fromCache = new ConcurrentHashMap<>(recordsLimit);
        for (Map.Entry<String, Record> pair : cache.entrySet()) {
            fromCache.put(pair.getKey(), pair.getValue().getValue());

        }
        return fromCache;
    }
}
