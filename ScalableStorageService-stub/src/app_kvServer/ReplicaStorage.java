package app_kvServer;

import common.messages.AdmMessage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ReplicaStorage {

    private static Logger logger = Logger.getLogger(ReplicaStorage.class);

    private String strategy;
    private int limit;
    private int replicaNum;

    private ConcurrentHashMap<String, String> cache_rep;
    // possible private DiskStorage

    public ReplicaStorage(String strategy, int limit, int replicaNum){
        this.limit = limit;
        this.strategy = strategy;
        this.replicaNum = replicaNum;
        this.cache_rep = new ConcurrentHashMap<>(limit);
        logger.info("ReplicaStorage has been created :" + replicaNum);
    }

    public AdmMessage.StatusType store(String key, String value){

        if (value.equals("null") || value.equals("")) {
            boolean deleteResp = purge(key);
            return deleteResp ? AdmMessage.StatusType.DELETE_SUCCESS:
                    AdmMessage.StatusType.DELETE_ERROR;
        }

        if (cache_rep.size() < limit || cache_rep.containsKey(key) ){
            if(!cache_rep.containsKey(key)){
                cache_rep.put(key,value);
                return AdmMessage.StatusType.PUT_SUCCESS;
            } else { //update
                cache_rep.replace(key,value);
                return AdmMessage.StatusType.PUT_UPDATE;
            }
        }
        else { // write to disk
            // since it's replica, we actually do not record metadata and just evict randomly
            List<String> keyList = new ArrayList<>(cache_rep.keySet());
            int victimIdx = ThreadLocalRandom.current().nextInt(0,keyList.size());
            String victimKey = keyList.get(victimIdx);
            // evict victim to disk and remove from cache_rep
            DiskStorage.getInstance().set(victimKey,cache_rep.get(victimKey),replicaNum);

            cache_rep.remove(victimKey);
            cache_rep.put(key,value);
        }
        return AdmMessage.StatusType.PUT_SUCCESS;
    }

    public String retrieve(String key){
        if (cache_rep.containsKey(key)){
            return cache_rep.get(key);
        } else{
            String diskResp = DiskStorage.getInstance().get(key,replicaNum);
            if (diskResp.equals(Constant.KEY_NOT_FOUND) || diskResp.equals("null")){
                return Constant.retrieveErrorMsg;
            } else {
                // on found, put back to cache
                store(key,diskResp);
                return diskResp;
            }
        }
    }

    public boolean purge(String key){
        if (cache_rep.containsKey(key)){
            cache_rep.remove(key);
            logger.debug(String.format("Key %s has been removed from replicaStorage %d",key,replicaNum));
            return true;
        } else {
            DiskStorage.getInstance().set(key,"null",replicaNum);
            return false;
        }
    }

    public void purgeAll(){
        cache_rep.clear();
    }

    public ConcurrentHashMap<String, String> getAll(){
        return cache_rep;
    }

    public void setAll(ConcurrentHashMap<String,String> src){
        cache_rep.putAll(src);
    }

    public int len(){
        return cache_rep.size();
    }

}
