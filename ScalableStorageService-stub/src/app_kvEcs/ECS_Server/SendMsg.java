package app_kvEcs.ECS_Server;


import common.messages.AdmMessage;
import common.messages.AdmMessage.StatusType;
import common.messages.AdmMessageC;
import common.messages.TextMessage;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static common.messages.AdmMessage.StatusType.REPLICA_PUT;

/**
 * Created by bryanchan on 3/4/17.
 */
public class SendMsg implements Runnable {

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static Logger logger = Logger.getRootLogger();

    Socket socket;
    AdmMessageC msg;
    String status;

    public SendMsg(AdmMessageC msg_, Socket socket_){
        msg = msg_;
        socket = socket_;
        status = msg_.getStatus().toString();
    }

    public static TextMessage receiveMessage(Socket socket) throws IOException {
        InputStream input = socket.getInputStream();
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        logger.info("receiveMessage() for:"+socket);
        byte read = (byte) input.read();
        boolean reading = true;
        while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                logger.error("receiveMessage-->index == BUFFER SIZE");
                if (msgBytes == null) {
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

            if ((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }
			/* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                logger.error("receiveMessage-->DROP SIZE reached");
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
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
        logger.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '" + msg.getMsg()+ "'"
                + "=" + msgBytes + ",");
        return msg;
    }

    public static void sendMessage(Socket socket, TextMessage msg) throws IOException {
        OutputStream output = socket.getOutputStream();
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes);
        output.flush();
        logger.info("sendMessage() ="+msg.getMsg());
        logger.info("SEND \t<" + socket.getInetAddress().getHostAddress()
                + ":" + socket.getPort() + ">: '" + msg.getMsgBytes() + "'");
    }

    public static void send_AdminMsg(Socket socket, AdmMessageC msg) throws IOException {
        TextMessage text = msg.toText();
        SendMsg.sendMessage(socket,text);

        if (msg.getStatus() == StatusType.SHUTDOWN){
            socket.close();
            return;
        }

        switch(msg.getStatus()){
            case REPLICA_PUT:
                break;

            case R1_GET:
                break;

            case R2_GET:
                break;

            case PRIMARY_GET:
                break;

            case R1_PUT:
                break;

            case R2_PUT:
                break;

            default:
                TextMessage replyText = SendMsg.receiveMessage(socket);
                System.out.println(replyText.getMsg());
                break;
        }

    }

    @Override
    public void run() {

        try {
            send_AdminMsg(socket,msg);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
