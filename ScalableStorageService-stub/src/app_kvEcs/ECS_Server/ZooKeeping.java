package app_kvEcs.ECS_Server;

/**
 * Created by bryanchan on 3/3/17.
 */


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ZooKeeping {
    private static ZooKeeper zk;
    private static zkConnector zkc;

    public ZooKeeping() throws IOException, InterruptedException {
        zkc = new zkConnector();
        zk = zkc.connect("localhost");
    }

    // create a new server node
    public static void create_znode(String path, byte[]data) throws KeeperException, InterruptedException {
        Stat st =  zk.exists(path,null);
        if (st != null) {
            zk.delete(path,zk.exists(path,true).getVersion());
        }
        zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // remove an existing server
    public static void delete_znode(String path) throws KeeperException, InterruptedException {
        zk.delete(path,zk.exists(path,true).getVersion());
    }

    // get server data
    public static byte[] read_znode(String path) throws KeeperException, InterruptedException {
        return zk.getData(path,true,zk.exists(path,true));
    }

    // update server data
    public static void update_znode(String path,byte[] data) throws KeeperException, InterruptedException {
        zk.setData(path,data,zk.exists(path,true).getVersion());
    }

    // construct server data
    public static byte[] contruct_data(String ip,
                                       String port,
                                       String cache_size,
                                       String strategy,
                                       String hash_value,
                                       String range_start,
                                       String range_end)
    {
        JSONObject json = new JSONObject();
        json.put("ip",ip);
        json.put("port",port);
        json.put("hash_value",hash_value);
        json.put("range_start",range_start);
        json.put("range_end",range_end);
        json.put("cache_size",cache_size);
        json.put("strategy",strategy);
        json.put("serverID",ip+":"+port);

        return json.toString().getBytes();
    }

    //get znode data
    public static HashMap<String, String> get_znode_data (String path) throws KeeperException, InterruptedException {
        byte[] data = read_znode(path);
        String str = new String(data, StandardCharsets.UTF_8);
        JSONObject obj = new JSONObject(str);
        HashMap<String,String> result = new HashMap<>();

        String ip = obj.has("ip") ? obj.getString("ip") : null;
        String port = obj.has("port") ? obj.getString("port") : null;
        String hash_value = obj.has("hash_value") ? obj.getString("hash_value") : null;
        String range_end = obj.has("range_end") ? obj.getString("range_end") : null;
        String range_start = obj.has("range_start") ? obj.getString("range_start") : null;
        String cache_size = obj.has("cache_size") ? obj.getString("cache_size") : null;
        String strategy = obj.has("strategy") ? obj.getString("strategy") : null;

        result.put("ip",ip);
        result.put("port",port);
        result.put("hash_value",hash_value);
        result.put("range_start",range_start);
        result.put("range_end",range_end);
        result.put("cache_size",cache_size);
        result.put("strategy",strategy);
        result.put("serverId",ip+":"+port);
        return  result;
    }


}
