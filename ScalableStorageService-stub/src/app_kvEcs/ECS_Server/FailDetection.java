package app_kvEcs.ECS_Server;

import java.net.Socket;
import java.util.Map;

/**
 * Created by bryanchan on 3/25/17.
 */
public class FailDetection implements Runnable {

    @Override
    public void run() {
        try{
            while(!ECS_Server.flag_shutdown){
                for(Map.Entry<String, Socket> entry : ECS_Server.get_replica_socket().entrySet()){
                    String serverId = entry.getKey();
                    String[] tokens = serverId.split(":");
                    String ip = tokens[0];
                    int port = Integer.parseInt(tokens[1]);
                    Thread.sleep(1000);
                    CheckSocket cs = new CheckSocket(ip,port);
                    Thread t = new Thread(cs);
                    t.start();
                }
            }
        }catch(Exception e){
            System.out.println(e);
        }

    }


}
