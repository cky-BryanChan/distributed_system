package app_kvServer.model;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Metadata {


    private Map<String, String> meta;
    private static MessageDigest md5;

    public Metadata(Map<String, String> meta) {
        this.meta = meta;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "meta=" + meta.toString() +
                '}';
    }
    /**
     * Store map entry of metadata (serverId:hashed) to Collection
     * sort ASC for successor and DESC for predecessor, return next serverId in hashing ring
     * @param myServerId   host:port
     * @param isSuccessor  isSuccessor = false means find Predecessor
     * @return  serverId for Successor or Predecessor
     */
    private String findNeighbor(String myServerId, final boolean isSuccessor){
        Map.Entry<String,String> myEntry = null;
        List<Map.Entry<String,String>> greater = new ArrayList<>();
        greater.addAll(meta.entrySet());
        // Asc based on hashed value
        Collections.sort(greater, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                if (isSuccessor)
                    return o1.getValue().compareTo(o2.getValue());
                else
                    return o2.getValue().compareTo(o1.getValue());
            }
        });
        for (Map.Entry<String,String> pairs: meta.entrySet()){
            if (pairs.getKey().equals(myServerId)){
                myEntry = pairs;
            }
        }
        assert myEntry!=null;

        int targetIndex ;
        if (greater.indexOf(myEntry) == greater.size() -1 )
            targetIndex = 0;
        else
            targetIndex = greater.indexOf(myEntry) + 1;
        return greater.get(targetIndex).getKey();
    }

    public String findServer(String key){
        Range range;
        for (Map.Entry<String, String> entry : meta.entrySet()) {
            range = new Range(meta.get(findPredecessor(entry.getKey())),meta.get(entry.getKey()));
            if(range.belongs(hashMD5(key)))
                return entry.getKey();
        }
        return null;
    }

    public static String hashMD5(String key){
        if ( md5 == null ) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return (new HexBinaryAdapter()).marshal(md5.digest(key.getBytes()));
    }

    public static String getCheckKey(List<String> my_list, String key){
        String port = "";
        for (String temp : my_list){
            String[] tokens = temp.split(":");
            if (tokens[0].equals(key)) {
                port = tokens[1];
                break;
            }
        }
        return key+":"+port;
    }

    public String findSuccessor(String myServerId){
        return findNeighbor(myServerId,true);
    }
    public String findPredecessor(String myServerId){
        return findNeighbor(myServerId,false);
    }
    public Map<String,String> getMap(){
        return this.meta;
    }
}
