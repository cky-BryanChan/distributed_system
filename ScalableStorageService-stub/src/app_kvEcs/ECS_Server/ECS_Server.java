package app_kvEcs.ECS_Server;

/**
 * Created by bryanchan on 3/3/17.
 */

import app_kvServer.model.Range;
import common.messages.AdmMessage;
import common.messages.AdmMessageC;
import common.messages.TextMessage;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import app_kvServer.model.Metadata;

import static common.messages.AdmMessage.StatusType.*;


public class ECS_Server implements Runnable{
    private static ZooKeeping zk;
    private static ArrayList<String> active = new ArrayList<>();
    private static ArrayList<String> idle = new ArrayList<>();
    private static ConcurrentHashMap<String, Socket> primary_socket_list = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Socket> replica_socket_list = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String,String> socket_mapping = new ConcurrentHashMap<>();
    private static Metadata meta;
    private static Boolean initialized = false;
    private static String serverPath = "/Users/$USER/StorageServer/out/artifacts/ScalableStorageService_stub_jar/ScalableStorageService-stub.jar";
    public static Boolean flag_shutdown = false;

    public static void get_server_status(){
        System.out.println("active servers:");
        for(String znode:active){
            System.out.print(znode);
            System.out.print(",");
        }
        System.out.println("");
        System.out.println("idle servers:");
        for(String znode:idle){
            System.out.print(znode);
            System.out.print(",");
        }
        System.out.println("");
    }

    public ECS_Server(BufferedReader br) throws IOException, KeeperException, InterruptedException{
        String currentLine;
        String[] tokens;
        zk = new ZooKeeping();
        idle.clear();
        active.clear();
        while ((currentLine = br.readLine()) != null){
            tokens = currentLine.split("\\s+");
            String name = tokens[0];
            String ip = tokens[1];
            String port = tokens[2];
            String hash_value = get_hash(ip,port);
            create_znode("/"+name, ip,port,null,null,hash_value,null,null);
            idle.add(name);
        }

        br.close();
    }

    public static ArrayList<String> getActive(){return active;}

    public static ArrayList<String> getIdle(){return idle;}

    public static ConcurrentHashMap<String, Socket> get_replica_socket(){return replica_socket_list;}

    public static ConcurrentHashMap<String,String> get_socket_mapping(){return socket_mapping;}

    public static Metadata getMeta() {return meta;}

    public static void create_znode(String path, String ip, String port, String cache_size, String strategy, String hash_value, String range_start, String range_end) throws KeeperException, InterruptedException {
        byte[] znodeByte = zk.contruct_data(ip,port,cache_size,strategy,hash_value,range_start,range_end);
        zk.create_znode(path,znodeByte);
    }

    public static void computeActiveRange() throws KeeperException, InterruptedException {
        Map<String,String> pairs = new HashMap<>();

        for (String znode: active){
            String path = "/"+znode;
            HashMap<String, String> znodeMap = zk.get_znode_data(path);
            String hash_value = znodeMap.get("hash_value");
            String serverId = znodeMap.get("serverId");
            pairs.put(serverId, hash_value);
        }
        meta = new Metadata(pairs);

        for (String znode: active){
            String path = "/"+znode;
            HashMap<String, String> znodeMap = zk.get_znode_data(path);
            String ip = znodeMap.get("ip");
            String port = znodeMap.get("port");
            String cache_size = znodeMap.get("cache_size");
            String strategy = znodeMap.get("strategy");
            String hash_value = znodeMap.get("hash_value");
            String serverId = znodeMap.get("serverId");
            String PredecessorId = meta.findPredecessor(serverId);
            String range_start = meta.getMap().get(PredecessorId);
            String range_end = meta.getMap().get(serverId);

            byte[] znodeByte = zk.contruct_data(ip,port,cache_size,strategy,hash_value,range_start,range_end);
            zk.update_znode(path,znodeByte);
        }
    }

    public static String get_hash(String ip, String port){
        return Metadata.hashMD5(ip+":"+port);
    }

    public static void broadcast_activeServer(AdmMessage.StatusType status) throws KeeperException, InterruptedException {

        for (String znode: active){
            String path = "/"+znode;
            HashMap<String, String> znodeMap = zk.get_znode_data(path);
            String cache_size = znodeMap.get("cache_size");
            String strategy = znodeMap.get("strategy");
            Socket socket = primary_socket_list.get(znode);
            AdmMessageC msg = new AdmMessageC(status,cache_size,strategy,meta,null,null);
            System.out.println("sending"+ strategy + cache_size);
            Thread t = new Thread(new SendMsg(msg,socket));
            t.start();
        }
    }

    public static String get_affected_znode(String hashed) throws KeeperException, InterruptedException {
        for (String znode: active){
            String path = "/"+znode;
            HashMap<String, String> znodeMap = zk.get_znode_data(path);
            Range range = new Range(znodeMap.get("range_start"),znodeMap.get("range_end"));
            if (range.belongs(hashed))
                return znode;
        }
        return "null";
    }

    public static void handleCrash(String serverId) throws KeeperException, InterruptedException, IOException {
        String[] tokens;
        String ip;
        int port;

        // remove Node

        System.out.println("broken server: "+serverId);

        //remove znode from active
        String crashed_znode = socket_mapping.get(serverId);
        socket_mapping.remove(serverId);
        active.remove(crashed_znode);

        //broadcast writelock
        broadcast_activeServer(AdmMessage.StatusType.LOCKWRITE);

        //get A,C,D,E serverId
        String A = meta.findPredecessor(serverId);
        String C = meta.findSuccessor(serverId);
        String D = meta.findSuccessor(C);
        String E = meta.findSuccessor(D);

        System.out.println("A: "+A);
        System.out.println("C: "+C);
        System.out.println("D: "+D);
        System.out.println("E: "+E);
        //socket setup
        tokens = A.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        Socket socket_A = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_A));
        tokens = C.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        Socket socket_C = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_C));
        tokens = D.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        Socket socket_D = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_D));
        tokens = E.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        Socket socket_E = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_E));

        /***** handle C *****/
        System.out.println("\n\n\n Starting handleC");

        // Successor Remove Node
        AdmMessageC msg = new AdmMessageC(AdmMessage.StatusType.SUCCESSOR_RM_NODE,null,null);
        SendMsg.send_AdminMsg(socket_C, msg);

        // r2 Clear
        clearSocketReplica(2,socket_C);
        // r2 = Ar1
        getfromPutToTransfer(socket_A,socket_C,1,2);

        /***** handle D *****/
        System.out.println("\n\n\n Starting handleD");
        // r1 clear
        clearSocketReplica(1,socket_D);
        //Dr1 = Cp
        getfromPutToTransfer(socket_C,socket_D,0,1);
        // r2 clear
        clearSocketReplica(2,socket_D);
        //Dr2 = Cr1
        getfromPutToTransfer(socket_C,socket_D,1,2);

        /***** handle E *****/
        System.out.println("\n\n\n Starting handleE");
        // r2 clear
        clearSocketReplica(2,socket_D);
        //r2 = Dr1        msg = new AdmMessageC(R1_GET,null,null);
        getfromPutToTransfer(socket_D,socket_E,1,2);

        //recompute range
        computeActiveRange();
        broadcast_activeServer(AdmMessage.StatusType.UPDATE);

        //release write lock
        broadcast_activeServer(AdmMessage.StatusType.UNLOCKWRITE);


        // ADD NODE

        String znode = active.get(0);
        HashMap<String, String> znodeMap = zk.get_znode_data("/"+znode);
        String cache_size = znodeMap.get("cache_size");
        String strategy = znodeMap.get("strategy");

        addNode(cache_size,strategy);

        String newZnode = active.get(active.size()-1);
        znodeMap = zk.get_znode_data("/"+newZnode);
        String new_serverId = znodeMap.get("serverId");
        A = meta.findPredecessor(new_serverId);
        C = meta.findSuccessor(new_serverId);
        D = meta.findSuccessor(C);
        E = meta.findSuccessor(D);


        System.out.println("A: "+A);
        System.out.println("B: "+new_serverId);
        System.out.println("C: "+C);
        System.out.println("D: "+D);
        System.out.println("E: "+E);


        //socket setup
        tokens = A.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        socket_A = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_A));

        tokens = new_serverId.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        Socket socket_B = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_B));

        tokens = C.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        socket_C = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_C));

        tokens = D.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        socket_D = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_D));

        tokens = E.split(":");
        ip = tokens[0];
        port = Integer.parseInt(tokens[1]);
        socket_E = new Socket(ip,port);
        System.out.println(SendMsg.receiveMessage(socket_E));

        broadcast_activeServer(AdmMessage.StatusType.LOCKWRITE);

        /***** handle B *****/
        System.out.println("\n\n\n Starting handleB");
        // r1 clear
        clearSocketReplica(1,socket_B);
        // r1 = Ap
        getfromPutToTransfer(socket_A,socket_B,0,1);
        // r2 clear
        clearSocketReplica(2,socket_B);
        // r2 = Ar1
        getfromPutToTransfer(socket_A,socket_B,1,2);

        /***** handle C *****/
        System.out.println("\n\n\n Starting handleC");

        // r1 clear
        clearSocketReplica(1,socket_C);
        // r1 = Bp
        getfromPutToTransfer(socket_B,socket_C,0,1);
        // r2 clear
        clearSocketReplica(2,socket_C);
        // r2 = Br1
        getfromPutToTransfer(socket_B,socket_C,1,2);

        /***** handle D *****/
        System.out.println("\n\n\n Starting handleD");

        // r1 clear
        clearSocketReplica(1,socket_D);
        // r1 = Cp
        getfromPutToTransfer(socket_C,socket_D,0,1);
        // r2 clear
        clearSocketReplica(2,socket_D);
        // r2 = Cr1
        getfromPutToTransfer(socket_C,socket_D,1,2);
        /***** handle E *****/
        System.out.println("\n\n\n Starting handleE");

        // r2 clear
        clearSocketReplica(2,socket_E);
        // r2 = Dr1
        getfromPutToTransfer(socket_D,socket_E,1,2);


        broadcast_activeServer(AdmMessage.StatusType.UNLOCKWRITE);
    }

    public static void getfromPutToTransfer(Socket src, Socket dest, int srcReplica, int destReplica) throws IOException {
        AdmMessage.StatusType get = srcReplica == 1 ? R1_GET : R2_GET;
        AdmMessage.StatusType get_done = srcReplica == 1 ? R1_GET_DONE : R2_GET_DONE;
        get = srcReplica == 0 ? PRIMARY_GET : get;
        get_done = srcReplica == 0 ? PRIMARY_GET_DONE : get_done;

        AdmMessage.StatusType put = destReplica == 1 ? R1_PUT:R2_PUT;
        AdmMessage.StatusType put_done = destReplica == 1 ? R1_PUT_DONE : R2_PUT_DONE;

        System.out.println(src.getPort() + ":" + srcReplica + "," + dest.getPort() + ":" + destReplica);
        // initial get from source
        AdmMessageC msg = new AdmMessageC(get,null,null);
        SendMsg.send_AdminMsg(src,msg);

        // wait for consecutive replies
        TextMessage reply_msg = SendMsg.receiveMessage(src);
        AdmMessage admin_reply_msg = AdmMessageC.toAdmMessage(reply_msg);

        // ASSUMPTIONS: will be many get until get_done
        while(admin_reply_msg.getStatus() != get_done){
            //System.out.println();
            String key = admin_reply_msg.getKey();
            String value = admin_reply_msg.getValue();

            // put to destination
            msg = new AdmMessageC(put, key, value);
            SendMsg.send_AdminMsg(dest, msg);

            // next iterations, still get from src
            reply_msg = SendMsg.receiveMessage(src);
            admin_reply_msg = AdmMessageC.toAdmMessage(reply_msg);
        }
        // send put_done to destination
        msg = new AdmMessageC(put_done,null,null);
        SendMsg.send_AdminMsg(dest,msg);
    }

    public static void clearSocketReplica(int replicaNum, Socket soc) throws IOException {
        AdmMessage.StatusType r = replicaNum == 1 ? AdmMessage.StatusType.R1_CLEAR : R2_CLEAR;
        AdmMessageC msg = new AdmMessageC(r,null,null);
        SendMsg.send_AdminMsg(soc,msg);
    }

    //ecs client functions
    public static boolean initNode(int num_nodes, String cache_size,String strategy) throws KeeperException, InterruptedException, IOException {

        if (initialized == true){
            System.out.println("app_kvEcs already initialized!");
            return false;
        }

        if (num_nodes > idle.size()) {
            System.out.println("not enough idle server to allocate, change <#nodes>");
            return false;
        }

        //move idle to active
        for (int i = num_nodes; i > 0 ; i--){
            String server = idle.remove(0);
            active.add(server);
            HashMap<String, String> znode = zk.get_znode_data("/"+server);
            byte[] znodeData = zk.contruct_data(znode.get("ip"),
                    znode.get("port"),
                    cache_size,
                    strategy,
                    znode.get("hash_value"),
                    null,
                    null);
            zk.update_znode("/"+server,znodeData);
        }

        //compute range for active servers
        computeActiveRange();

        //initialize and run servers
        System.out.println("initialize and init servers");
        for (String znode: active){

            HashMap<String, String> znodeMap = zk.get_znode_data("/"+znode);
            String ip = znodeMap.get("ip");
            String port = znodeMap.get("port");
            String serverId = znodeMap.get("serverId");
            String cmd = "ssh -n "+ip+" nohup java -jar " +serverPath +" "+port+" &\n";
            Runtime run = Runtime.getRuntime();
            System.out.println(cmd);
            run.exec(cmd);
            Thread.sleep(1000);

            //construct primary socket
            Socket socket = new Socket(ip,Integer.parseInt(port));
            primary_socket_list.put(znode,socket);
            System.out.println(SendMsg.receiveMessage(socket).getMsg());

            //construct replica socket
            socket = new Socket(ip,Integer.parseInt(port));
            replica_socket_list.put(serverId,socket);
            System.out.println(SendMsg.receiveMessage(socket).getMsg());

            //add mapping
            socket_mapping.put(serverId,znode);

        }

        System.out.println("broadcast init");
        broadcast_activeServer(AdmMessage.StatusType.INIT);
        initialized = true;

        for(Map.Entry<String, Socket> entry : replica_socket_list.entrySet()){
            Socket socket = entry.getValue();
            Thread t = new Thread(new PrimaryConnection(socket,entry.getKey()));
            t.start();
        }

        System.out.println("init done !!! \n\n\n");
        return true;
    }

    public static void startNode() throws KeeperException, InterruptedException {
        if (initialized == false){
            System.out.println("please initialize server first");
            return;
        }
        broadcast_activeServer(AdmMessage.StatusType.START);
        Thread.sleep(1000);

        // run detection
       // new Thread(new FailDetection()).start();
    }

    public static void stopNode() throws KeeperException, InterruptedException {
        if (initialized == false){
            System.out.println("please initialize server first");
            return;
        }
        broadcast_activeServer(AdmMessage.StatusType.STOP);
    }

    public static void shutDownNode() throws IOException, KeeperException, InterruptedException {

        initialized = false;
        flag_shutdown = true;

        for(Map.Entry<String, Socket> entry : replica_socket_list.entrySet()){

            Socket socket = entry.getValue();
            replica_socket_list.remove(entry.getKey());
            socket.close();
        }



        broadcast_activeServer(AdmMessage.StatusType.SHUTDOWN);

        for (String znode :active){
            System.out.println("removing "+znode);
            primary_socket_list.remove(znode);
            idle.add(znode);
        }

        for (String znode :idle)
            zk.delete_znode("/"+znode);

        System.exit(1);
    }

    public static void addNode(String cache_size, String strategy) throws IOException, KeeperException, InterruptedException {
        if (initialized == false){
            System.out.println("please initialize server first");
            return;
        }

        if(idle.isEmpty()){
            System.out.println("no idle servers");
            return;
        }


        //get one from idle
        String znode = idle.remove(0);

        //znode setup
        String path = "/"+znode;
        HashMap<String, String> znodeMap = zk.get_znode_data(path);
        String new_ip = znodeMap.get("ip");
        String new_port = znodeMap.get("port");
        String new_hash_value = znodeMap.get("hash_value");
        String new_serverId = znodeMap.get("serverId");
        String affected_znode = get_affected_znode(new_hash_value);
        meta.getMap().put(new_serverId,new_hash_value);
        byte[] znodeByte = zk.contruct_data(new_ip, new_port, cache_size,strategy, new_hash_value, null, null);
        zk.update_znode(path,znodeByte);
        active.add(znode);


        //determine position in ring
        computeActiveRange();


        //initialize server with updated metadata
        znodeMap = zk.get_znode_data(path);
        String serverId = znodeMap.get("serverId");
        String range_start = znodeMap.get("range_start");
        String range_end = znodeMap.get("range_end");
        Range range = new Range(range_start,range_end);

        //SSH to new server
        String cmd = "ssh -n "+new_ip+" nohup java -jar " +serverPath+" "+new_port+" &";
        Runtime run = Runtime.getRuntime();
        run.exec(cmd);

        AdmMessageC msg = new AdmMessageC(AdmMessage.StatusType.INIT,cache_size,strategy,meta,null,null);
        Thread.sleep(2000);
        Socket socket = new Socket(new_ip,Integer.parseInt(new_port));
        primary_socket_list.put(znode,socket);
        System.out.println(SendMsg.receiveMessage(socket).getMsg());
        SendMsg.send_AdminMsg(socket,msg);

        socket = new Socket(new_ip,Integer.parseInt(new_port));
        replica_socket_list.put(serverId,socket);
        System.out.println(SendMsg.receiveMessage(socket).getMsg());
        msg = new AdmMessageC(AdmMessage.StatusType.ECS_SOCKET,null,null);
        SendMsg.send_AdminMsg(socket,msg);


        //start server
        msg = new AdmMessageC(AdmMessage.StatusType.START,cache_size,strategy,meta,null,null);
        SendMsg.send_AdminMsg(socket,msg);


        //set writeLock on affected server
        msg = new AdmMessageC(AdmMessage.StatusType.LOCKWRITE,null,null,meta,null,null);
        socket = primary_socket_list.get(affected_znode);
        SendMsg.send_AdminMsg(socket,msg);


        //move data from affected server to new server
        msg = new AdmMessageC(AdmMessage.StatusType.MOVEDATA,null,null,meta,new_serverId,range);
        SendMsg.send_AdminMsg(socket,msg);


        //broadcast update to all servers
        broadcast_activeServer(AdmMessage.StatusType.UPDATE);


        //release lock
        msg = new AdmMessageC(AdmMessage.StatusType.UNLOCKWRITE,null,null,meta,new_serverId,range);
        SendMsg.send_AdminMsg(socket,msg);

    }

    public static void removeNode(int index) throws KeeperException, InterruptedException, IOException {


        if (initialized == false){
            System.out.println("please initialize server first");
            return;
        }

        if(index < 0 || index > active.size()-1){
            System.out.println("please enter index 0-"+Integer.toString(active.size()-1));
            return;
        }

        String deleting_znode = active.remove(index);
        idle.add(deleting_znode);

        // znode remove and recalculate
        String path = "/"+deleting_znode;
        HashMap<String, String> znodeMap = zk.get_znode_data(path);
        String deleting_serverId = znodeMap.get("serverId");
        String deleting_hash_value = znodeMap.get("hash_value");
        String deleting_range_start = znodeMap.get("range_start");
        String deleting_range_end = znodeMap.get("range_end");
        Range deleting_range = new Range(deleting_range_start,deleting_range_end);
        meta.getMap().remove(deleting_serverId);
        computeActiveRange();
        String responsible_znode = get_affected_znode(deleting_hash_value);


        // set writeLock on deleting server
        AdmMessageC msg = new AdmMessageC(AdmMessage.StatusType.LOCKWRITE,null,null,meta,null,null);
        Socket socket = primary_socket_list.get(deleting_znode);
        SendMsg.send_AdminMsg(socket,msg);


        // send update to the responsible server
        path = "/"+responsible_znode;
        znodeMap = zk.get_znode_data(path);
        String responsible_serverId = znodeMap.get("serverId");
        msg = new AdmMessageC(AdmMessage.StatusType.UPDATE,null,null,meta,null,null);
        socket = primary_socket_list.get(responsible_znode);
        SendMsg.send_AdminMsg(socket,msg);


        // send moveData from deleting server to the responsible server
        msg = new AdmMessageC(AdmMessage.StatusType.MOVEDATA,null,null,meta,responsible_serverId,deleting_range);
        socket = primary_socket_list.get(deleting_znode);
        SendMsg.send_AdminMsg(socket,msg);


        // broadcast update to servers
        broadcast_activeServer(AdmMessage.StatusType.UPDATE);


        // shutdown deleting server
        msg = new AdmMessageC(AdmMessage.StatusType.SHUTDOWN,null,null,meta,null,null);
        socket = primary_socket_list.remove(deleting_znode);
        SendMsg.send_AdminMsg(socket,msg);
        socket.close();
        socket = replica_socket_list.remove(deleting_serverId);
        socket.close();

    }

    @Override
    public void run() {
        try {

            String[] tokens;
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));


            //client operation prompt
            while (true){
                System.out.print("ECS_SERVER >");
                tokens = stdin.readLine().trim().split("\\s+");

                if (tokens[0].equals("init")){

                    if (tokens.length != 4) {
                        System.out.println("init invalid arguments! <#node> <cache size> <strategy>");
                        continue;
                    }

                    int num_nodes = Integer.parseInt(tokens[1]);
                    initNode(num_nodes,tokens[2],tokens[3]);
                }
                else if(tokens[0].equals("start")){

                    startNode();

                }
                else if(tokens[0].equals("stop")){

                    stopNode();

                }
                else if(tokens[0].equals("shutDown")){

                    shutDownNode();
                    break;

                }
                else if(tokens[0].equals("addNode")){

                    if (tokens.length != 3){
                        System.out.println("addNode invalid arguments <cache size> <strategy>");
                        continue;
                    }
                    addNode(tokens[1],tokens[2]);

                }

                else if(tokens[0].equals("removeNode")){

                    if(tokens.length != 2){
                        System.out.println("removeNode invalid arguments <index>");
                    }
                    removeNode(Integer.parseInt(tokens[1]));

                }
                else if(tokens[0].equals("showServer")){
                    get_server_status();
                }

                else if(tokens[0].equals("test")){

                }
                else if(tokens[0].equals("help")){
                    System.out.println("Valid command:");
                    System.out.println("init <#node> <cache size> <strategy>");
                    System.out.println("start");
                    System.out.println("stop");
                    System.out.println("shutDown");
                    System.out.println("addNode <cache size> <strategy>");
                    System.out.println("removeNode <index>");
                    System.out.println("startServer");
                }
                else {
                    System.out.println("type help");
                }
            }



        }catch(Exception e){
            System.out.println(e);
        }
    }
}
