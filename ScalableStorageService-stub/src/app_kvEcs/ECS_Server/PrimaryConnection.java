package app_kvEcs.ECS_Server;

import app_kvServer.CacheStorage;
import app_kvServer.model.Metadata;
import common.messages.AdmMessage;
import common.messages.AdmMessageC;
import common.messages.TextMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Created by bryanchan on 3/24/17.
 */
@SuppressWarnings("Duplicates")
public class PrimaryConnection implements Runnable{

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private Socket clientSocket;
    private String serverId;

    public PrimaryConnection(Socket clientSocket, String serverID) {
        this.clientSocket = clientSocket;
        this.serverId = serverID;
        this.isOpen = true;
    }

    public PrimaryConnection(){};

    public void run() {
        try {
            AdmMessageC msg = new AdmMessageC(AdmMessage.StatusType.ECS_SOCKET,null,null);
            SendMsg.send_AdminMsg(clientSocket,msg);
            while(isOpen) {
                TextMessage latestMsg = SendMsg.receiveMessage(clientSocket);
                System.out.println(latestMsg.getMsg());
                if (AdmMessageC.isAdminType(latestMsg)) {
                    handleAdmMessage(latestMsg);
                }
            }
        } catch (IOException e) {
            if (ECS_MAIN.ECS.get_replica_socket().containsKey(serverId)){
                System.out.println(e);
            }
            isOpen = false;
        }

    }

    public int handleAdmMessage(TextMessage latestMsg) throws IOException{
        AdmMessage msg = AdmMessageC.toAdmMessage(latestMsg);
        switch (msg.getStatus()) {
            case REPLICA_PUT:
                String key = msg.getKey();
                String value = msg.getValue();
                Metadata meta = ECS_MAIN.ECS.getMeta();
                String server = meta.findServer(serverId);
                String s1 = meta.findSuccessor(server);
                String s2 = meta.findSuccessor(s1);

                //send put to replica 1
                System.out.println("sending put to "+s1);
                Socket socket = ECS_MAIN.ECS.get_replica_socket().get(s1);
                AdmMessageC put_msg = new AdmMessageC(AdmMessage.StatusType.REPLICA_PUT, key, value);
                SendMsg.send_AdminMsg(socket, put_msg);

                //send put to replica 2
                System.out.println("sending put to "+s2);
                socket = ECS_MAIN.ECS.get_replica_socket().get(s2);
                SendMsg.send_AdminMsg(socket, put_msg);
                break;
        }
        return 0;
    }



}
