package app_kvEcs.ECS_Server;

import app_kvServer.model.Metadata;
import common.messages.AdmMessage;

import java.io.IOException;
import java.net.Socket;

/**
 * Created by bryanchan on 3/25/17.
 */
public class CheckSocket implements Runnable {
    private static String ip;
    private static int port;

    public CheckSocket(String ip, int port){
        this.ip = ip;
        this.port = port;
    }


    @Override
    public void run() {
        try {
            Socket socket = new Socket(ip, port);
            socket.close();
        } catch (IOException er) {
            try{
                System.out.println(er);
                System.out.println("unexpected crash!");

                //remove the socket to stop heartbeat
                String serverId = ip+ ":"+Integer.toString(port);
                ECS_MAIN.ECS.get_replica_socket().remove(serverId);

                //handle crash
                ECS_MAIN.ECS.handleCrash(serverId);

            }catch(Exception err){
                System.out.println(err);
            }



        }
    }
}
