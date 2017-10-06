package app_kvServer;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;

import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface ECSmanagedServer {

    /**
     * Called by external app_kvEcs to control the KVServe
     * @param metadata   consistent hashing start/end range
     * @param cacheSize  limit of our hashtable record
     * @param strategy   cache eviction strategy, FIFO,LRU,LFU
     */
    public boolean initKVServer(Metadata metadata, String cacheSize, String strategy);

    /**
     * Called by external app_kvEcs to start the server
     */
    public boolean startServer();
    /**
     * Called by external app_kvEcs to stop the server
     */
    public boolean stopServer();
    /**
     * Called by app_kvEcs to close the socket and exit the KVServer completely
     */
    public void shutdownServer();
    /**
     * called by app_kvEcs to lock the KVServer for any write/PUT operation
     * @return lockStatus
     */
    public boolean lockWrite();
     /**
     * Called by app_kvEcs to unlock the KVServer, used combined with lockWrite()
     * @return unlockStatus
     */
    public boolean unlockWrite();
    /**
     *  Server must also able to receive & update with such transferred data
     **/
    public boolean moveData(ConcurrentHashMap<String, String> fromCache, String destServer);

    public void updateMetadata(Map<String,String> metadata);

    // Helper function used as variables in interface
    public boolean initedByECS();
    public boolean startedByECS();
    public boolean lockedByECS();

    public void setEcsServer(Socket clientSocket);
    public Socket getEcsServer();
    public Range getPrimaryRange();
    public Range getReplica2Range();
    public Range getReplica1Range();
    public Metadata getMetadata();

    public ReplicaStorage getReplica(int replicaNum);
    public void setReplica(ReplicaStorage r, int replicaNum);
}
