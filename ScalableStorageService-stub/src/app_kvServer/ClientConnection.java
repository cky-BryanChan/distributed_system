package app_kvServer;

import app_kvEcs.ECS_Server.SendMsg;
import app_kvServer.model.Metadata;
import common.messages.AdmMessage;
import common.messages.AdmMessage.StatusType;
import common.messages.AdmMessageC;
import common.messages.KVMessage;
import common.messages.KVMessageC;
import common.messages.TextMessage;
import common.messages.cryptography;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.Key;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static common.messages.AdmMessage.StatusType.UNKNOWN_MIN;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
@SuppressWarnings("Duplicates")
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    private CacheStorage cache = null;
    private ConcurrentHashMap<String, String> fromCache = null;
    private ECSmanagedServer ecSmanagedServer;
    private cryptography crypt = null;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket ) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            sendMessage(new TextMessage(
                    "Connection to MSRG Echo server established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort()));

                    TextMessage latestMsg = receiveMessage();
                    if (latestMsg == null || latestMsg.getMsg().length() == 0) {
                        logger.debug("Received empty message");
                        sendMessage(latestMsg);
                    } else {

                        while (isOpen) {
                            if (!cryptography.isEncryption(latestMsg)) {
                                // AdmMessage or 1st ClientMessage
                                if (AdmMessageC.isAdminType(latestMsg)) {
                                    //admin message
                                    handleAdmMessage(latestMsg);
                                } else if (crypt == null) {
                                    //setup client public key for server
                                    logger.debug("First message from cleint, needs to setup crypt");
                                    handleKVMessage(latestMsg);
                                } else {
                                    //get hacked
                                    logger.debug("Get Hacked");
                                    TextMessage msg = KVMessageC.toText(KVMessage.StatusType.UNKNOWN_REQUEST, null);
                                    sendMessage(msg);
                                }

                            } else { // already have encrypt
                                assert crypt != null;
                                // need to decrypt and handle KVMessage
                                if (ecSmanagedServer.initedByECS() && ecSmanagedServer.startedByECS()) {
                                    cache = CacheStorage.getInstance();
                                    assert latestMsg != null;
                                    String msg_str = crypt.decrypt(latestMsg.getMsg());
                                    TextMessage decryptedMsg = new TextMessage(msg_str);
                                    handleKVMessage(decryptedMsg);
                                } else {
                                    rejectKVMessage();
                                }
                            }
                            latestMsg = receiveMessage();
                        }

                    } //end of else


            } // end of try

        catch (IOException ioe) {
        isOpen = false;
            logger.error("Error! Connection could not be established!", ioe);

        } catch (Exception e) {
            logger.error("Error! decrypt Exception!", e);
        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }


    public int handleAdmMessage(TextMessage latestMsg) throws IOException{
        logger.debug("Received Admin Message");
        AdmMessage msg = AdmMessageC.toAdmMessage(latestMsg);
        StatusType status = UNKNOWN_MIN;
        switch (msg.getStatus()){
            case INIT:
                 status = ecSmanagedServer.initKVServer(
                        msg.getMetadata(),
                        msg.getCacheSize(),
                        msg.getStrategy()) ? StatusType.INIT_SUCCESS : StatusType.INIT_ERROR;
                cache = CacheStorage.getInstance();
                // Initiate new replica Cache
                logger.debug("Primary Cache Initialized");
                int limit = Integer.parseInt(msg.getCacheSize());
                ReplicaStorage r1,r2;
                    r1 = new ReplicaStorage(msg.getStrategy(),limit,1);
                    r2 = new ReplicaStorage(msg.getStrategy(),limit,2);
                ecSmanagedServer.setReplica(r1,1);
                ecSmanagedServer.setReplica(r2,2);
                logger.debug("2 Replica Cache Initialized");
                break;
            case START:
                status = ecSmanagedServer.startServer() ? StatusType.START_SUCCESS : StatusType.START_ERROR;
                break;
            case STOP:
                 status = ecSmanagedServer.stopServer() ? StatusType.STOP_SUCCESS : StatusType.STOP_ERROR;
                break;
            case SHUTDOWN:
                ecSmanagedServer.shutdownServer();
                break;
            case LOCKWRITE:
                status =   ecSmanagedServer.lockWrite() ? StatusType.LOCKWRITE_SUCCESS : StatusType.LOCKWRITE_ERROR;
                break;
            case UNLOCKWRITE:
                status = ecSmanagedServer.unlockWrite() ? StatusType.UNLOCKWRITE_SUCCESS : StatusType.UNLOCKWRITE_ERROR;
                break;
            case UPDATE:
                if (msg.getMetadata() == null || msg.getMetadata().getMap() == null ){
                   status = StatusType.UPDATE_ERROR;
                }else{
                    ecSmanagedServer.updateMetadata(msg.getMetadata().getMap());
                    status = StatusType.UPDATE_SUCCESS;
                }
                break;
            case ECS_SOCKET:
                logger.debug("Received the ECS_SOCKET, saved");
                ecSmanagedServer.setEcsServer(clientSocket);
                status = StatusType.ECS_SOCKET_RECEIVED;
                break;
            case REPLICA_PUT:
                String key = msg.getKey(), value = msg.getValue();
                logger.debug("Received REPLICA_PUT, (key:value) " + key + " : " + value );
                if (ecSmanagedServer.getReplica1Range().belongs(msg.getKey())){
                    status = ecSmanagedServer.getReplica(1).store(key,value);
                }
                else if (ecSmanagedServer.getReplica2Range().belongs(msg.getKey())){
                    status = ecSmanagedServer.getReplica(2).store(key,value);
                }
                else {
                    status = StatusType.NOT_REPLICA;
                    logger.error("NOT THE REPLICA");
                }
                break;
            case MOVEDATA:
                cache = CacheStorage.getInstance();
                if (msg.getServerId() == null || msg.getRange() == null ){
                    status = StatusType.MOVEDATA_ERROR;
                }
                else {
                    String destination = msg.getServerId();
                    logger.debug("Received app_kvEcs moveData, destination: "+destination + " range: " + msg.getRange().toString() );
                    fromCache = cache.preMove(msg.getRange());
                    logger.debug("created fromCache, size: " + fromCache.size());
                    // from Disk
                    boolean moveDataStatus = ecSmanagedServer.moveData(fromCache,destination);
                    // create socket and send
                    status = moveDataStatus ? StatusType.MOVEDATA_COMPLETED : StatusType.MOVEDATA_ERROR;
                    cache.deletePartial(fromCache);
                }
                break;
            case SUCCESSOR_RM_NODE:
                ReplicaStorage replica1 = ecSmanagedServer.getReplica(1);
                ReplicaStorage replica2 = ecSmanagedServer.getReplica(2);
                logger.warn("SUCCESSOR_RM_NODE: pre-rm, " + replica1.len() + " : " +replica2.len());
                cache = CacheStorage.getInstance();
                cache.addAll(replica1.getAll());
                logger.debug("Adding all replica1 to primary cache");
                replica1.purgeAll();
                replica1.setAll(replica2.getAll());
                logger.debug("Switching the r1 with r2");
                logger.warn("SUCCESSOR_RM_NODE: pre-rm, " + replica1.len() + " : " +replica2.len());
                status = StatusType.SUCCESSOR_RM_NODE_SUCCESS;
                break;
            case R1_CLEAR:
                ecSmanagedServer.getReplica(1).purgeAll();
                status = StatusType.R1_CLEAR_SUCCESS;
                break;
            case R2_CLEAR:
                ecSmanagedServer.getReplica(2).purgeAll();
                status = StatusType.R2_CLEAR_SUCCESS;
                break;
            case R1_GET:
                ConcurrentHashMap<String,String> map1 = ecSmanagedServer.getReplica(1).getAll();
                logger.debug("R1_GET, map1 size: "+map1.size());
                for (Map.Entry<String,String> pair:map1.entrySet()){
                    TextMessage each = AdmMessageC.toText(StatusType.R1_GET,pair.getKey(),pair.getValue());
                    sendMessage(each);
                    logger.info("R1_GET" + each.getMsg());
                }
                status = StatusType.R1_GET_DONE;
                logger.debug("R1_GET_DONE");
                break;
            case R2_GET:
                ConcurrentHashMap<String,String> map2 = ecSmanagedServer.getReplica(2).getAll();
                logger.debug("R2_GET, map2 size: "+map2.size());
                for (Map.Entry<String,String> pair:map2.entrySet()){
                    TextMessage each = AdmMessageC.toText(StatusType.R2_GET,pair.getKey(),pair.getValue());
                    sendMessage(each);
                    logger.info("R2_GET" + each.getMsg());
                }
                status = StatusType.R2_GET_DONE;
                logger.debug("R2_GET_DONE");
                break;
            case PRIMARY_GET:
                cache = CacheStorage.getInstance();
                ConcurrentHashMap<String,String> mapP = cache.getAll();
                logger.debug("PRIMARY_GET, mapP size: "+mapP.size());
                for (Map.Entry<String,String> pair:mapP.entrySet()){
                    TextMessage each = AdmMessageC.toText(StatusType.PRIMARY_GET,pair.getKey(),pair.getValue());
                    sendMessage(each);
                    logger.info("PRIMARY_GET" + each.getMsg());
                }
                status = StatusType.PRIMARY_GET_DONE;
                logger.debug("PRIMARY_GET_DONE");
                break;
            case R1_PUT:
                assert msg.getKey()!=null;
                assert msg.getValue()!=null;
                ecSmanagedServer.getReplica(1).store(msg.getKey(),msg.getValue());
                logger.info("R1_PUT" + msg.getKey() + msg.getValue());
                status = StatusType.DONOTSEND;
                break;
            case R1_PUT_DONE:
                status = StatusType.R1_PUT_DONE;
                break;
            case R2_PUT:
                assert msg.getKey()!=null;
                assert msg.getValue()!=null;
                ecSmanagedServer.getReplica(2).store(msg.getKey(),msg.getValue());
                logger.info("R2_PUT" + msg.getKey() + msg.getValue());
                status = StatusType.DONOTSEND;
                break;
            case R2_PUT_DONE:
                status = StatusType.R2_PUT_DONE;
                break;
        }
        if (status != StatusType.DONOTSEND){
            TextMessage admR = AdmMessageC.toText(status,null,null,null,null,null);
            sendMessage(admR);
        }
        return 0;
    }

    public int handleKVMessage(TextMessage latestMsg) throws IOException{
        KVMessageC parsedMsg = KVMessageC.toKVmessage(latestMsg);

        if (parsedMsg == null ) {
            TextMessage msg = KVMessageC.toText(KVMessage.StatusType.UNKNOWN_REQUEST,null,null,null);
            sendMessage(encryptTxtMsg(msg));
        }
        else if (parsedMsg.getStatus() == KVMessage.StatusType.UPDATE_CRYPTKEY){
            Key client_pub_key = parsedMsg.get_cryptKey();
            crypt = new cryptography(client_pub_key);
            TextMessage response = KVMessageC.toText(KVMessage.StatusType.UPDATE_CRYPTKEY_SUCCESS,null);
            sendMessage(response);
        }
        else {
        String key = parsedMsg.getKey();
        int cacheNum = whichRange(key);
        logger.debug("handleKVMessage, (key,cacheNum)" + key + " : " +  cacheNum);
        if (  cacheNum == -1 && (parsedMsg.getStatus() != KVMessage.StatusType.PUT_SERVER)) {
            notWithinRangeKVMessage(ecSmanagedServer.getMetadata());
        }
        else if (  cacheNum != 0 && (parsedMsg.getStatus() == KVMessage.StatusType.PUT)) {
            notWithinRangeKVMessage(ecSmanagedServer.getMetadata());
        }
        else if ( ecSmanagedServer.lockedByECS() && parsedMsg.getStatus() == KVMessage.StatusType.PUT ){
            writeLockedKVMessage();
        }
        else {
            // ===================  GET  ===================
            if (parsedMsg.getStatus() == KVMessage.StatusType.GET) {
                String cacheResp = null;
                if (cacheNum == 0)
                    cacheResp = cache.retrieve(key);
                else
                    cacheResp = ecSmanagedServer.getReplica(cacheNum).retrieve(key);
                if (cacheResp.equals(Constant.retrieveErrorMsg)) {
                    TextMessage response = KVMessageC.toText(
                            KVMessage.StatusType.GET_ERROR,
                            parsedMsg.getKey(),
                            "null",
                            null
                    );
                    sendMessage(encryptTxtMsg(response));
                } else {
                    TextMessage response = KVMessageC.toText(
                            KVMessage.StatusType.GET_SUCCESS,
                            parsedMsg.getKey(),
                            cacheResp,
                            null
                    );
                    sendMessage(encryptTxtMsg(response));
                }
                // ===================  PUT  ===================
            } else if (parsedMsg.getStatus() == KVMessage.StatusType.PUT ||
                    parsedMsg.getStatus() == KVMessage.StatusType.PUT_SERVER) { // SET
                TextMessage response = null;
                boolean isUpdate_Delete = false;
                HashMap<Integer,String> purgeResponse = null;

                if (parsedMsg.getValue().equals("null") || parsedMsg.getValue().equals("")) {
                    isUpdate_Delete = true;
                    purgeResponse = cache.purge(parsedMsg.getKey());
                    if (purgeResponse != null ) {
                        response = KVMessageC.toText(
                                KVMessage.StatusType.DELETE_SUCCESS,
                                "null",
                                "null",
                                null);
                        TextMessage msgToEcs = AdmMessageC.toText(
                                StatusType.REPLICA_PUT,
                                parsedMsg.getKey(),
                                parsedMsg.getValue());
                        SendMsg.sendMessage(ecSmanagedServer.getEcsServer(), msgToEcs);
                    } else {
                        response = KVMessageC.toText(
                                KVMessage.StatusType.DELETE_ERROR,
                                "null",
                                "null",
                                null);
                    }

                } else {
                    isUpdate_Delete = false;
                    KVMessage.StatusType cacheResponse = cache.store(parsedMsg.getKey(), parsedMsg.getValue());
                    response = KVMessageC.toText(
                            cacheResponse,
                            parsedMsg.getKey(),
                            parsedMsg.getValue(),
                            null);
                    TextMessage msgToEcs = AdmMessageC.toText(
                            StatusType.REPLICA_PUT,
                            parsedMsg.getKey(),
                            parsedMsg.getValue());
                    SendMsg.sendMessage(ecSmanagedServer.getEcsServer(), msgToEcs);
                    logger.debug("Sent AdmMessage to ECS" + msgToEcs.getMsg());
                    // send KVMessage back to client
                }

                sendMessage(encryptTxtMsg(response));
                    // Handle Subscriber Here  either, update || deleted but has > 0 subscriber
                if (cache.worthBroadcast(key) || (purgeResponse!=null && purgeResponse.size() > 0)){

                    logger.info("isUpdate_Delete" + isUpdate_Delete);
                    HashMap<Integer,String> subscriberList = isUpdate_Delete ? purgeResponse :
                            cache.getSubscriberfromCache(key);

                    logger.debug("Sending Subscriber BroadCast");
                    TextMessage msg = KVMessageC.toText(
                            KVMessage.StatusType.SUBSCRIBE_UPDATE,parsedMsg.getKey(),parsedMsg.getValue(),null);

                    for (Map.Entry<Integer,String> entry: subscriberList.entrySet()){
                        Socket to = new Socket(entry.getValue(),entry.getKey());
                        logger.info("Send broadcast message to: " + to.getInetAddress().getHostName()+":"+to.getPort());
                        logger.info("Send broadcast message msg: " + msg.getMsg());
                        SendMsg.sendMessage(to,msg);
                    }
                }

            } else if (parsedMsg.getStatus() == KVMessage.StatusType.SUBSCRIBE){
                String hashKey = parsedMsg.getKey(),
                        subscriberPort = parsedMsg.getValue(),
                        subscriberHost = clientSocket.getLocalAddress().getHostAddress();
                logger.info("Received SUBSCRIBE, " + hashKey + "  " + subscriberHost +":"+ subscriberPort);
                int src_port = Integer.parseInt(subscriberPort);
                boolean subBoolean = cache.subscribe(hashKey,src_port,subscriberHost);
                KVMessage.StatusType statusType = (subBoolean) ? KVMessage.StatusType.SUBSCRIBE_SUCCESS : KVMessage.StatusType.SUBSCRIBE_ERROR;
                TextMessage response = KVMessageC.toText(statusType,null);
                sendMessage(encryptTxtMsg(response));
            } else if (parsedMsg.getStatus() == KVMessage.StatusType.UNSUBSCRIBE){
                String hashKey = parsedMsg.getKey(),
                        subscriberPort = parsedMsg.getValue(),
                        subscriberHost = clientSocket.getLocalAddress().getHostAddress();
                logger.info("Received UNSUBSCRIBE, " + hashKey + "  " + subscriberHost +":"+ subscriberPort);
                int src_port = Integer.parseInt(subscriberPort);
                boolean subBoolean = cache.unsubscribe(hashKey,src_port,subscriberHost);
                KVMessage.StatusType statusType = (subBoolean) ? KVMessage.StatusType.UNSUBSCRIBE_SUCCESS : KVMessage.StatusType.UNSUBSCRIBE_ERROR;
                TextMessage response = KVMessageC.toText(statusType,null);
                sendMessage(encryptTxtMsg(response));
            }
            else {
                TextMessage msg = KVMessageC.toText(KVMessage.StatusType.UNKNOWN_REQUEST,null,null,null);
                sendMessage(encryptTxtMsg(msg));
                logger.debug("Unknown Request Received: " + parsedMsg.toString());
            }
          } // responsible
        } // parsedMsg not null
        return 0;
    }

    /**
     * get client request during the server has not been started
     * @return status code
     * @throws IOException
     */
    public int rejectKVMessage() throws IOException{
        TextMessage response = KVMessageC.toText(
                KVMessage.StatusType.SERVER_STOPPED,
                "null",
                "null",
                null
        );
        logger.info("Reject Message sent");
        sendMessage(encryptTxtMsg(response));
        return 0;
    }

    /**
     * get client request whose key in not within server responsible range
     * @param metadata server latest metadata
     * @return status code
     * @throws IOException
     */
    public int notWithinRangeKVMessage(Metadata metadata) throws IOException {
        TextMessage response = KVMessageC.toText(
                KVMessage.StatusType.SERVER_NOT_RESPONSIBLE,
                "null",
                "null",
               metadata
        );
        logger.info("Reject Message sent");
        sendMessage(encryptTxtMsg(response));
        return 0;
    }

    public void broadcastToSubscriber(String key, String value, HashMap<Integer,String> subscribers){
        Iterator<Map.Entry<Integer, String>> it = subscribers.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<Integer, String> entry = it.next();
            logger.info("broadcastTo: "+ entry.getValue() + entry.getKey() + "and create new socket");
            TextMessage encrypted = KVMessageC.toText(KVMessage.StatusType.SUBSCRIBE_UPDATE,key,value,null);

        }
    }

    private int whichRange(String key){
        if (ecSmanagedServer.getPrimaryRange().belongs(key)) return 0;
        else if (ecSmanagedServer.getReplica1Range().belongs(key)) return 1;
        else if (ecSmanagedServer.getReplica2Range().belongs(key)) return 2;
        else  return -1;
    }

    /**
     * get client request during server move data
     * @return status code
     * @throws IOException
     */
    public int writeLockedKVMessage() throws IOException {
        TextMessage response = KVMessageC.toText(
                KVMessage.StatusType.SERVER_WRITE_LOCK,
                "null",
                "null",
                null
        );
        logger.info("Server received PUT request during the lockWrite");
        sendMessage(encryptTxtMsg(response));
        return 0;
    }
    /**
     * Method sends a TextMessage using this socket.
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() +"'");
    }

    // remove sendEncryptMsg and receivedEncryptMsg
    private TextMessage encryptTxtMsg(TextMessage msg) {
        String encryptedText = null;
        try {
            encryptedText = crypt.encrypt(msg.getMsg());
        } catch (Exception e) {
            logger.error("encryptTxtMsg encrypt error" + e.getMessage());
            e.printStackTrace();
        }
        return new TextMessage(encryptedText);
    }

    private TextMessage decryptTxtMsg(TextMessage msg) {
        String msg_str = null;
        try {
            msg_str = crypt.decrypt(msg.getMsg());
        } catch (Exception e) {
            logger.error("decryptTxtMsg encrypt error" + e.getMessage());
            e.printStackTrace();
        }
        logger.info("Receive message:\t '" + msg_str + "'");
        return  new TextMessage(msg_str);
    }

    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

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
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }

    public void setECSmanagerServer(ECSmanagedServer managedServer){
        this.ecSmanagedServer = managedServer;
    }
}

