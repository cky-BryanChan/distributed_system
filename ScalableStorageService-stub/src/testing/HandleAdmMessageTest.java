package testing;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;
import client.KVStore;
import common.messages.AdmMessage;
import common.messages.AdmMessageC;
import common.messages.KVMessage;
import common.messages.KVMessageC;
import common.messages.TextMessage;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertTrue;

public class HandleAdmMessageTest {

    private static KVStore mockedECS_A;
    private static KVStore mockedECS_B;
    private KVStore clientA;
    private KVStore clientB;

    private static Metadata metadata;
    private static Range range;
    private static String serverId;
    private static int numServer = 2;
    private static int cacheSize = 3;
    private static String strategy = "FIFO";

    @BeforeClass
    public static void doOnce(){

        //Construct metadata
        // have to make sure 2 server running in 0.0.0.0:1234 and 1235 before starting test
        Map<String,String> map = new HashMap<>();
        for (int i = 4; i < 4 + numServer; i ++){
            String serverId = "0.0.0.0:123"+ Integer.toString(i);
            String hex = Metadata.hashMD5(serverId);
            map.put(serverId,hex);
        }
        metadata = new Metadata(map);

        String cachesize = String.valueOf(cacheSize);

        mockedECS_A = new KVStore("localhost",1234);
        mockedECS_B = new KVStore("localhost",1235);

        // Connect and initiate each server
        try {
            TextMessage init = genAdmTextMsg(AdmMessage.StatusType.INIT,cachesize,strategy,
                    metadata,range,serverId);
            mockedECS_A.connect();
            mockedECS_A.sendMessage(init);
            TextMessage init_ra = mockedECS_A.receiveMessage();
            System.out.println(init_ra.getMsg());
            mockedECS_A.disconnect();

            mockedECS_B.connect();
            mockedECS_B.sendMessage(init);
            TextMessage init_rb = mockedECS_B.receiveMessage();
            System.out.println(init_rb.getMsg());
            mockedECS_B.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static TextMessage genAdmTextMsg(
            AdmMessage.StatusType cmd,
            String _cachesize,
            String _strategy,
            Metadata _meta,
            Range _range,
            String _serverId
    ){
        TextMessage msg = AdmMessageC.toText(
                cmd,_cachesize,_strategy,_meta,_range,_serverId);
        return msg;
    }

    @Test
    public void testStart() {
        TextMessage start_reply = null;
        Exception ex = null;
        try {
            mockedECS_A.connect();
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.START,null,null,null,null,null));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_A.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        AdmMessageC start_replyC = AdmMessageC.toAdmMessage(start_reply);
        assertTrue(ex == null && start_replyC.getStatus().equals(AdmMessage.StatusType.START_SUCCESS));
    }

    @Test
    public void testStop(){
        TextMessage start_reply = null;
        Exception ex = null;
        try {
            mockedECS_A.connect();
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.STOP,null,null,null,null,null));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_A.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        AdmMessageC start_replyC = AdmMessageC.toAdmMessage(start_reply);
        assertTrue(ex == null && start_replyC.getStatus().equals(AdmMessage.StatusType.STOP_SUCCESS));
    }

    @Test
    public void testLockWrite(){
        TextMessage start_reply = null;
        Exception ex = null;
        try {
            mockedECS_A.connect();
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.LOCKWRITE,null,null,null,null,null));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_A.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        AdmMessageC start_replyC = AdmMessageC.toAdmMessage(start_reply);
        assertTrue(ex == null && start_replyC.getStatus().equals(AdmMessage.StatusType.LOCKWRITE_SUCCESS));
    }

    @Test
    public void testUnlockWrite(){
        TextMessage start_reply = null;
        Exception ex = null;
        try {
            mockedECS_A.connect();
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.UNLOCKWRITE,null,null,null,null,null));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_A.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        AdmMessageC start_replyC = AdmMessageC.toAdmMessage(start_reply);
        assertTrue(ex == null && start_replyC.getStatus().equals(AdmMessage.StatusType.UNLOCKWRITE_SUCCESS));
    }

    @Test
    public void testUpdateMetadata(){

    }

    @Test
    public void testMoveData(){
        //TODOO populate each server with KVMessage
        String key = null, val = null;
        KVMessage response = null;
        // send new range,
        TextMessage start_reply = null;
        clientA = new KVStore("localhost",1234);
        clientB = new KVStore("localhost",1235);
        Exception ex = null;
        try {
            mockedECS_A.connect();
            clientA.connect();
            clientB.connect();
            mockedECS_B.connect();
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.START,null,null,null,null,null));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_B.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.START,null,null,null,null,null));
            start_reply = mockedECS_B.receiveMessage();

            for (int i = 0 ; i < 6 ; i ++) {
                key = String.valueOf(i);
                val = String.valueOf(10*i);
                   response = clientA.put(key,val);
                   response = clientB.put(key,val);
            }
            /*
            0:CFCD208495D565EF66E7DFF9F98764DA
            1:C4CA4238A0B923820DCC509A6F75849B
            2:C81E728D9D4C2F636F067F89CC14862C
            3:ECCBC87E4B5CE2FE28308FD9F2A7BAF3
            4:A87FF679A2F3E71D9181A67B7542122C
            5:E4DA3B7FBBCE2345D7772B0674A318D5
            0.0.0.0:1234:21CEB624050B4C753EF06EA0885942D7
            0.0.0.0:1235:C8997D9E12B707BA03AC420E39E9311D
            */
            range = new Range(
                    "DDDD0000DDDD0000DDDD0000DDDD0000",
                    "EC00EC00EC00EC00EC00EC00EC00EC00");
            serverId = "0.0.0.0:1235";
            mockedECS_A.sendMessage(
                    genAdmTextMsg(AdmMessage.StatusType.MOVEDATA,null,null,null,range,serverId));
            start_reply = mockedECS_A.receiveMessage();
            mockedECS_A.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        AdmMessageC start_replyC = AdmMessageC.toAdmMessage(start_reply);
        assertTrue(ex == null && start_replyC.getStatus().equals(AdmMessage.StatusType.MOVEDATA_COMPLETED));
    }

















}
