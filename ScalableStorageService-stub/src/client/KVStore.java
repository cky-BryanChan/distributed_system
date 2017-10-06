package client;


import app_kvClient.KVClient;
import app_kvClient.Notification_handler;
import app_kvEcs.ECS_Server.SendMsg;
import app_kvServer.model.Metadata;
import com.sun.corba.se.spi.activation.Server;
import common.messages.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Key;
import java.util.*;

import static common.messages.AdmMessage.*;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVStore> ";
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private String serverAddress;
	private int serverPort;
    private cryptography crypt;

    private Subscriber subscriber;
    private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;


	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		serverAddress = address;
		serverPort = port;
        try {
            crypt = new cryptography();
        } catch (Exception e) {
            logger.error("KVStore constructor encryption error" + e.getMessage());
            e.printStackTrace();
        }
    }

	public void run() {

	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	@Override
	/**
	 * make connection to the server
	 */
	public void connect() throws IllegalArgumentException, IOException {
		setRunning(true);
        try {
            clientSocket = new Socket(serverAddress, serverPort);

		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();

		TextMessage rplymsg = receiveMessage();
		int i = 0;
		while(rplymsg.getMsg().isEmpty() && i < 20) {
			i++;
			rplymsg = receiveMessage();
		}
		System.out.println(PROMPT + rplymsg.getMsg());
		logger.info("Connection established");


		//send key to server
		Key key = crypt.getKey();
		TextMessage msg  = KVMessageC.toText(KVMessage.StatusType.UPDATE_CRYPTKEY, key);
        sendMessage(msg);

        rplymsg = receiveMessage();
        System.out.println(PROMPT + rplymsg.getMsg());

        } catch (IOException e) {
			if (e instanceof UnknownHostException){
				throw e;
			}
            logger.error("KVStore connect error" + e.getMessage());
            e.printStackTrace();
        }
	}

	/**
	 * disconnect to the server
	 */
	@Override
	public synchronized void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();

//			for(ClientSocketListener listener : listeners) {
//				listener.handleStatus(ClientSocketListener.SocketStatus.DISCONNECTED);
//			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	public void addListener(ClientSocketListener listener){
		listeners.add(listener);
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
	@Override
	public KVMessage put(String key, String value) throws Exception {
        key = Metadata.hashMD5(key);
		if(isRunning()){
			try {
				TextMessage txtMsg = KVMessageC.toText(KVMessage.StatusType.PUT, key, value, null);
				logger.info("Sending : " + txtMsg.getMsg());
				sendEncryptedMessage(txtMsg);
			}catch (IOException ioe) {
				logger.error("Put value failed");
				throw new Exception("Put value failed");
			}

			TextMessage rplymsg = receiveMessageDecrypted();
			int i = 0;
			while(rplymsg.getMsg().isEmpty() && i < 20) {
				i++;
				rplymsg = receiveMessageDecrypted();
			}

			System.out.println(PROMPT + rplymsg.getMsg());
			KVMessageC rplymsgKV = KVMessageC.toKVmessage(rplymsg);
			return rplymsgKV;
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
	@Override
	public KVMessage get(String key) throws Exception {
		key = Metadata.hashMD5(key);
		if(isRunning()){
			try {
				TextMessage txtMsg = KVMessageC.toText(KVMessage.StatusType.GET, key, "",null);
				logger.info("Sending : " + txtMsg.getMsg());
				sendEncryptedMessage(txtMsg);
			}catch (IOException ioe) {
				logger.error("Get value failed");
				throw new Exception("Get value failed");
			}

			TextMessage rplymsg = receiveMessage();
			if (!cryptography.isEncryption(rplymsg)){
                System.out.println("unencrypted message received!, abandon message");
                return null;
            }

            String msg_str = crypt.decrypt(rplymsg.getMsg());
            logger.info("Receive message:\t '" + msg_str + "'");
            rplymsg = new TextMessage(msg_str);

			int i = 0;
			while(rplymsg.getMsg().isEmpty() && i < 20) {
				i++;
				rplymsg = receiveMessageDecrypted();
			}

			System.out.println(PROMPT + rplymsg.getMsg());
			KVMessageC rplymsgKV = KVMessageC.toKVmessage(rplymsg);
			String hashValue = Metadata.hashMD5(rplymsgKV.getKey()+rplymsgKV.getValue());
			if(rplymsgKV.getHMAC()!=null && !rplymsgKV.getHMAC().equals(hashValue))
				return new KVMessageC(KVMessage.StatusType.VALUE_NOT_MATCHED);
			return rplymsgKV;
		}
		else{
			logger.error("not connected");
			throw new Exception("not connected");
		}
	}


    /* Only used for testing purporse */
	public void mockAsECS(Metadata meta, String size, String strategy){

		TextMessage init = AdmMessageC.toText(
				StatusType.INIT,
				size,
				strategy,
				meta,
				null,null
		);

        TextMessage start = AdmMessageC.toText(
                StatusType.START,
                null,null,null,null,null
        );

        //Socket serverSocket = new Socket("0.0.0.0", 7777);
        TextMessage ecsSocket = AdmMessageC.toText(
                StatusType.ECS_SOCKET,
                null,null,null,null,null
        );

		try {
			sendEncryptedMessage(init);
            sendEncryptedMessage(start);
            sendEncryptedMessage(ecsSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/* Only used for testing purporse */
	public void mockAsECSShutDown(){
		TextMessage shutdown = AdmMessageC.toText(
				StatusType.SHUTDOWN,
				null,null,null,null,null
		);

		try {
			sendEncryptedMessage(shutdown);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    /**
     * send message string to the server
     * @param msg
     * @throws IOException
     * 				message not successfully sent
     */
    public void sendEncryptedMessage(TextMessage msg) throws IOException {
        String encryptedText = null;
        try {
            encryptedText = crypt.encrypt(msg.getMsg());
        } catch (Exception e) {
            e.printStackTrace();
        }
        TextMessage temp = new TextMessage(encryptedText);
        sendMessage(temp);
    }

	/**
	 * send message string to the server
	 * @param msg
	 * @throws IOException
	 * 				message not successfully sent
	 */
	public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	/**
	 * read message string from the server
	 * @return
	 * @throws IOException
	 * 				message not successful read from buffer
	 */
	public TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
  		byte read = (byte) input.read();
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
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

			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

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



        // build final String
		TextMessage msg = new TextMessage(msgBytes);
		return msg;
	}

    public TextMessage receiveMessageDecrypted() throws Exception{
	    TextMessage encypted_msg = receiveMessage();
        // build final String
        String msg_str = crypt.decrypt(encypted_msg.getMsg());
        logger.info("Receive message:\t '" + msg_str + "'");
        TextMessage msg = new TextMessage(msg_str);
        return msg;
    }

    public boolean subscribe(String key){

        int port = 5000 + new Random(1).nextInt(1000);
        while(true){
            try{
                ServerSocket s = new ServerSocket(port);
                s.close();
                break;
            }catch(Exception e){
                port++;
            }
        }

        String hashed_key = Metadata.hashMD5(key);
        TextMessage msg = KVMessageC.toText(KVMessage.StatusType.SUBSCRIBE,hashed_key, String.valueOf(port),null);
        try {
            sendEncryptedMessage(msg);
            TextMessage received_msg = receiveMessageDecrypted();
            System.out.println("Receive message:\t '" + received_msg.getMsg() + "'");

            KVMessageC KV_msg = KVMessageC.toKVmessage(received_msg);
            if(KV_msg.getStatus() == KVMessage.StatusType.SUBSCRIBE_SUCCESS){
                String temp = key+":"+String.valueOf(port);
                List<String> my_list = subscriber.getScribers();
                my_list.add(temp);
                subscriber.setScribers(my_list);
                Notification_handler Nh = new Notification_handler(new ServerSocket(port), key, subscriber);
                Thread t = new Thread(Nh);
                t.start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean unsubscribe(String key){

        List<String> my_list = subscriber.getScribers();
        String hashed_key = Metadata.hashMD5(key);
        String checkKey = Metadata.getCheckKey(my_list,key);
        String port = checkKey.substring(checkKey.indexOf(":") +1);
        logger.debug("unsub port" + port);
        TextMessage msg = KVMessageC.toText(KVMessage.StatusType.UNSUBSCRIBE,hashed_key, port,null);
        try {
            sendEncryptedMessage(msg);
            TextMessage received_msg = receiveMessageDecrypted();
            System.out.println("Receive message:\t '" + received_msg.getMsg() + "'");

            KVMessageC KV_msg = KVMessageC.toKVmessage(received_msg);
            if(KV_msg.getStatus() == KVMessage.StatusType.UNSUBSCRIBE_SUCCESS){
                String remove_key = key+":"+port;
                my_list.remove(remove_key);
                subscriber.setScribers(my_list);
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    public void setSubscriber(Subscriber subscriber) {
        this.subscriber = subscriber;
    }
}
