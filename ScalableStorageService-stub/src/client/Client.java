package client;

import app_kvClient.KVClient;
import common.messages.KVMessage;
import common.messages.KVMessageC;
import common.messages.TextMessage;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import app_kvServer.model.Metadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Key;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import common.messages.cryptography;

import javax.crypto.spec.SecretKeySpec;

public class Client extends Thread {

    private static final String PROMPT = "Client> ";
    private Logger logger = Logger.getRootLogger();
    private Set<ClientSocketListener> listeners;
    private boolean running;
    private cryptography crypt;

    private OutputStream output;
    private InputStream input;
    private KVStore kvstore;
    private String currServer;
    private Metadata meta;
    private Subscriber subscriber;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    public Client(String address, int port, Subscriber subscriber){
        kvstore = new KVStore(address, port);
        currServer = address+":"+port;
        this.subscriber = subscriber;
        kvstore.setSubscriber(this.subscriber);
    }
    public Client(String address, int port ){
        kvstore = new KVStore(address, port);
        currServer = address+":"+port;
    }

    public void run() {

    }

    public void setCryp(cryptography c){
        crypt = c;
    }

    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }

    /**
     * connect to the server
     * @throws UnknownHostException
     * @throws IOException
     */
    public void connect() {
        setRunning(true);
        try {
            kvstore.connect();
            kvstore.run();
        }catch (Exception e) {
            System.out.println(PROMPT + "SERVER_DOWN");
            this.currServer = meta.findSuccessor(this.currServer);
            String[] parts = this.currServer.split(":");
            kvstore = new KVStore(parts[0], Integer.parseInt(parts[1]));
            kvstore.setSubscriber(this.subscriber);
            connect();
        }
    }

    /**
     * disconnect to the server
     */
    public void disconnect() {
        setRunning(false);
        if(kvstore != null) {
            kvstore.disconnect();
            kvstore = null;
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }


    public void reconnectResponsible(String key) throws Exception {
        if(meta == null)
            return;

        String tempServer = meta.findServer(key);
        if(currServer!= tempServer) {
            disconnect();
            this.currServer=tempServer;
            String[] parts = currServer.split(":");
            kvstore = new KVStore(parts[0], Integer.parseInt(parts[1]));
            kvstore.setSubscriber(this.subscriber);
            connect();
        }

    }

    /* Only used for testing purporse */
    public void mockAsECS(Metadata meta, String size, String strategy){
        kvstore.mockAsECS(meta,size,strategy);
    }

    /**
     * save the key-value pair
     * @param key
     *            the key that identifies the given value.
     * @param value
     *            the value that is indexed by the given key.
     * @return message received from server
     * @throws Exception
     * 				message not successfully sent to server
     */
    public KVMessage put(String key, String value) throws Exception {
        if(isRunning()) {
            if(meta!=null)
                reconnectResponsible(key);
            KVMessage msg = kvstore.put(key, value);

            if(msg.getStatus()==KVMessage.StatusType.SERVER_WRITE_LOCK){
                System.out.println(PROMPT + "SERVER_WRITE_LOCK");
                return msg;
            }
            else if(msg.getStatus()==KVMessage.StatusType.SERVER_STOPPED) {
                System.out.println(PROMPT + "SERVER_STOPPED");
                return msg;
            }
            else if(msg.getStatus()==KVMessage.StatusType.DELETE_SUCCESS){
                System.out.println(PROMPT + "DELETE_AND_UNSUBSCRIBED " + key);
                if(this.subscriber == null)
                    return msg;
                List<String> my_list = this.subscriber.getScribers();
                String temp = Metadata.getCheckKey(my_list,key);
                my_list.remove(temp);
                this.subscriber.setScribers(my_list);
                return msg;
            }
            int i=0;
            while(i<30 && msg.getStatus()==KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                this.meta = msg.getMetaData();
                System.out.println(PROMPT + "SERVER_NOT_RESPONSIBLE");
                reconnectResponsible(key);
                msg = kvstore.put(key, value);
                i++;
            }
            return msg;
        }
        else{
            logger.error("not connected");
            throw new Exception("not connected");
        }
    }

    /**
     * get value from storage server for the given key
     * @param key
     *            the key that identifies the value.
     * @return message from the server
     * @throws Exception
     * 				message not successfully to server
     */
    public KVMessage get(String key) throws Exception {
        if(isRunning()) {
            if(meta!=null)
                reconnectResponsible(key);
            KVMessage msg = kvstore.get(key);

            if(msg == null)
                return  msg;

            if(msg.getStatus()==KVMessage.StatusType.SERVER_STOPPED) {
                System.out.println(PROMPT + "SERVER_STOPPED");
                return msg;
            }

            if(msg.getStatus()==KVMessage.StatusType.VALUE_NOT_MATCHED) {
                System.out.println(PROMPT + "VALUE_NOT_MATCHED");
                return msg;
            }

            int i=0;
            while(i<30 && msg.getStatus()==KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                this.meta = msg.getMetaData();
                reconnectResponsible(key);
                System.out.println(PROMPT + "SERVER_NOT_RESPONSIBLE");
                msg = kvstore.get(key);
                i++;
            }
            return msg;
        }
        else{
            logger.error("not connected");
            throw new Exception("not connected");
        }
    }

    public boolean subscribe(String key){
        try{
            reconnectResponsible(key);
            return kvstore.subscribe(key);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public boolean unsubscribe(String key){
        try{
            reconnectResponsible(key);
            return kvstore.unsubscribe(key);
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

}
