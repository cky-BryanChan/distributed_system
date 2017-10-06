package app_kvServer;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;
import common.messages.KVMessage;
import common.messages.KVMessageC;
import common.messages.TextMessage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVServer extends Thread implements ECSmanagedServer{

    private static Logger logger = Logger.getRootLogger();
    private int port;
    private ServerSocket serverSocket;
    private Socket ecsSocket;
    private boolean running;
    private String serverId;

    private Metadata meta;
    private Range primaryRange;
    private Range replica1Range;
    private Range replica2Range;
    private ReplicaStorage replica1;
    private ReplicaStorage replica2;

    private int cacheSize;
    private String strategy;

    private boolean initedByECS;
    private boolean startedByECS;
    private boolean lockedByECS;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public KVServer(int port) {
        this.port = port;
        this.running = false;
        this.initedByECS = false;
        this.startedByECS = false;
        this.lockedByECS = false;
    }

    // NEW FOR MILESTONE 2

    public boolean initKVServer(Metadata meta, String cacheSize, String strategy){
        // update both meta and primary/r1/r2 range
        updateMetadata(meta.getMap());

        this.cacheSize = Integer.parseInt(cacheSize);
        this.strategy = strategy;
        // init cache and disk
        CacheStorage.init(this.strategy,this.cacheSize);
        DiskStorage.initialization();
        this.initedByECS = true;
        logger.debug("Server has been initialized by app_kvEcs");
        logger.debug("initKVServer with " + meta.toString() + " cache Size: " + cacheSize + " strategy: " + strategy);
        return true;
    }

    public boolean startServer(){
        this.startedByECS = true;
        logger.debug("Server has been started by app_kvEcs");
        return (this.startedByECS);
    }

    public boolean stopServer(){
        this.startedByECS = false;
        return (!this.startedByECS);
    }
    // Thread have final method of stop

    public void shutdownServer(){
        logger.info("app_kvEcs has issued shut down instruction");
        try{
            serverSocket.close();
        } catch (IOException e){
            logger.error("Error! " +
                    "Unable to close socket on port " + port, e);
        }
        System.exit(1);
    }

    public boolean lockWrite(){
        lockedByECS = true;
        return true;
    }

    public boolean unlockWrite(){
        lockedByECS = false;
        return true;
    }

    public boolean moveData(ConcurrentHashMap<String, String> data, String destServer){
        String ipaddr = destServer.split(":")[0];
        int dest_port = Integer.parseInt(destServer.split(":")[1]);
        Socket toDestinationServer = null;
        try {
           toDestinationServer = new Socket(ipaddr,dest_port);
            //Connection established garbage message
            TextMessage rplymsg = recv_src_to_des_message(toDestinationServer);
            logger.debug("created src to dest socket, start transferring data");

        Iterator dataIt = data.keySet().iterator();
        while(dataIt.hasNext()){
            String k = (String) dataIt.next();
            String v = data.get(k);
            TextMessage newput = KVMessageC.toText(
                    KVMessage.StatusType.PUT_SERVER,
                    k,v,null
            );
            logger.info("moveData: "+newput.getMsg());
            send_src_to_dest_message(toDestinationServer,newput);
            TextMessage destReply = recv_src_to_des_message(toDestinationServer);
            KVMessageC destReplyC = KVMessageC.toKVmessage(destReply);
            if (destReplyC.getStatus() != KVMessage.StatusType.PUT_SUCCESS){
                logger.warn("Destination cannot take the data any more");
                return false;
            }
        }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void updateMetadata(Map<String,String> metadata){
        this.meta = new Metadata(metadata);
        String prev_serverId = meta.findPredecessor(this.serverId);
        String prime_hi = meta.getMap().get(this.serverId);
        String prime_lo = meta.getMap().get(prev_serverId);
        this.primaryRange = new Range(prime_lo,prime_hi);

        prev_serverId = meta.findPredecessor(prev_serverId);
        String r1_hi = prime_lo;
        String r1_lo = meta.getMap().get(prev_serverId);
        this.replica1Range = new Range(r1_lo,r1_hi);

        prev_serverId = meta.findPredecessor(prev_serverId);
        String r2_hi = r1_lo;
        String r2_lo = meta.getMap().get(prev_serverId);
        this.replica2Range = new Range(r2_lo,r2_hi);

        logger.debug("primary range: " + prime_lo + ":" + prime_hi);
        logger.debug("r1 range: " + r1_lo + ":" + r1_hi);
        logger.debug("r2 range: " + r2_lo + ":" + r2_hi);
    }

    @Override
    public boolean initedByECS() {
        return initedByECS;
    }

    @Override
    public boolean startedByECS() {
        return startedByECS;
    }

    @Override
    public boolean lockedByECS() {
        return lockedByECS;
    }

    @Override
    public void setEcsServer(Socket clientSocket){
        this.ecsSocket = clientSocket;
    }

    @Override
    public Socket getEcsServer() {
        return ecsSocket;
    }

    @Override
    public Range getPrimaryRange() {
        return primaryRange;
    }

    @Override
    public Range getReplica2Range() {
        return replica2Range;
    }

    @Override
    public Range getReplica1Range() {
        return replica1Range;
    }

    @Override
    public Metadata getMetadata() {
        return meta;
    }

    @Override
    public ReplicaStorage getReplica(int replicaNum) {
        return (replicaNum == 1) ? replica1 : replica2;
    }

    @Override
    public void setReplica(ReplicaStorage r, int replicaNum) {
        if (replicaNum == 1) replica1 = r;
        else if (replicaNum == 2) replica2 = r;
        else logger.error("Invalid replicaNum in setReplica "+ replicaNum);
    }


    // MILESTONE 1 CODE
    private boolean isRunning(){
        return this.running;
    }

    /**
     * Bind the port and log the info that server starts
     * @return binding success
     */
    private boolean initializeServer(){
        try{
            serverSocket = new ServerSocket(port);
            serverId = serverSocket.getInetAddress().getHostAddress() +":"+ serverSocket.getLocalPort();
            logger.info("Server listening on : " + serverId);
            return true;
        } catch (IOException e){
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException){
                logger.error("Port " + port + "is already bound!");
            }
            return false;
        }
    }

    public void run(){
        running = initializeServer();

        if (serverSocket != null) {
            while(isRunning()){
                try{
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client);
                    new Thread(connection).start();

                    connection.setECSmanagerServer(this);

                    logger.info("Connected to "
                            +client.getInetAddress().getHostName()
                            + "on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped");
    }


    public static void main(String[] args){
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if(args.length != 1) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: KVServer <port> !");
            } else {
                int port = Integer.parseInt(args[0]);
                new KVServer(port).start();
            }

        } catch (IOException e){
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe){
            System.out.println("Error! Invalid argument <port> Not a number!");
            System.out.println("Usage: KVServer <port> !");
            System.exit(1);

        }
    }

    public void send_src_to_dest_message(Socket toDest,TextMessage msg) throws IOException{
        OutputStream output = toDest.getOutputStream();
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND SRC 2 DEST\t<"
                + toDest.getInetAddress().getHostAddress() + ":"
                + toDest.getPort() + ">: '"
                + msg.getMsg() +"'");
    }

    private TextMessage recv_src_to_des_message(Socket fromDest) throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        InputStream input = fromDest.getInputStream();

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

			/* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }

        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

		/* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        long threadId = Thread.currentThread().getId();
        logger.info("Thread# " + threadId + " RECEIVE \t<"
                + fromDest.getInetAddress().getHostAddress() + ":"
                + fromDest.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }
}

