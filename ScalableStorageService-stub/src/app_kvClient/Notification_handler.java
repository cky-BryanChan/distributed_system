package app_kvClient;

import app_kvEcs.ECS_Server.SendMsg;
import app_kvServer.model.Metadata;
import client.Subscriber;
import common.messages.TextMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

/**
 * Created by apple on 2017/4/2.
 */
public class Notification_handler implements Runnable {
    private static final String PROMPT = "KVClient> ";
    private ServerSocket listen_socket;
    private String key;
    private Subscriber sub;

    public Notification_handler(ServerSocket s, String key, Subscriber sub){
        this.listen_socket = s;
        this.key = key;
        this.sub = sub;
    }
    @Override
    public void run() {
        try {
            while(true){
                List<String> my_list = sub.getScribers();
                String check_key = Metadata.getCheckKey(my_list,this.key);
                if(!sub.getScribers().contains(check_key))
                    break;
                Socket from_server = listen_socket.accept();
                TextMessage msg =  SendMsg.receiveMessage(from_server);
                System.out.println(msg.getMsg());
                System.out.print(PROMPT);
            }

            // message received


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
