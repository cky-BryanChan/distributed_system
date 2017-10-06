package testing;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;
import common.messages.AdmMessage;
import common.messages.AdmMessageC;
import common.messages.TextMessage;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class MetadataTest extends TestCase{

    private AdmMessageC msg;
    private Metadata meta;
    private Range range;
    private MessageDigest md5;

   @Before
   public void setUp() {

       Map<String,String> test = new HashMap<>();
       for (int i = 0; i < 10; i ++){
           String serverId = "192.168.0." + Integer.toString(i);
           String hex = hashMD5(serverId);
           test.put(serverId,hex);
           //System.out.println("serverId: " + serverId + " is hashed to " + hex );
       }
       /*
        serverId: 192.168.0.0 is hashed to 447F32D25B7D55524675C64D649AE5D9
        serverId: 192.168.0.1 is hashed to F0FDB4C3F58E3E3F8E77162D893D3055
        serverId: 192.168.0.2 is hashed to 6F65E91667CF98DD13464DEAF2739FDE
        serverId: 192.168.0.3 is hashed to 8C27F6B5DC0E9E5A536AA0371648FB4A
        serverId: 192.168.0.4 is hashed to 0252F5158DC3A9531907FB42C00A4138
        serverId: 192.168.0.5 is hashed to 8EBAEC02E0D0F38566870133E565941B
        serverId: 192.168.0.6 is hashed to F23C168FE4B03627754C52F632C547E2
        serverId: 192.168.0.7 is hashed to BD1518783C060C100FB7EB82C619C5A6
        serverId: 192.168.0.8 is hashed to F4E4C0AE3FFCC5F281329A6DB3665E03
        serverId: 192.168.0.9 is hashed to 6379559F580D7BF13DAB6C046EF88DF8
       */
        meta = new Metadata(test);
        msg = new AdmMessageC(AdmMessage.StatusType.INIT);
   }

    @Test
    public void testFindSuccessor() {
        String succ = meta.findSuccessor("192.168.0.0");
        Assert.assertTrue(succ.equals("192.168.0.9"));
    }

    @Test
    public void testFindSuccessorWrapAround() {
        String succ = meta.findSuccessor("192.168.0.8");
        Assert.assertTrue(succ.equals("192.168.0.4"));
    }

    @Test
    public void testFindPredecessor() {
        String succ = meta.findPredecessor("192.168.0.8");
        Assert.assertTrue(succ.equals("192.168.0.6"));
    }

    @Test
    public void testFindPredecessorWrapAround() {
        String succ = meta.findPredecessor("192.168.0.4");
        Assert.assertTrue(succ.equals("192.168.0.8"));
    }

    @Test
    public void testBelongs(){
        String serverId = "192.168.0.8";
        String predecessorId = "192.168.0.6";
        String hashedMiddle = "F23C168FE4B03627754C52F632C547E3";
        range = new Range(meta.getMap().get(meta.findPredecessor(serverId)),meta.getMap().get(serverId));
        System.out.println(range);
        System.out.println("MiddleIndex=" + hashedMiddle);


        boolean start = range.belongs(hashMD5(predecessorId));
        boolean middle = range.belongs(hashedMiddle);
        boolean end = range.belongs(hashMD5(serverId));

        Assert.assertTrue(!start && middle && end);
    }

    @Test
    public void testFindServer(){
        String key = "key11";
        String serverId = meta.findServer(key);
        range = new Range(meta.getMap().get(meta.findPredecessor(serverId)),meta.getMap().get(serverId));

        Assert.assertTrue(range.belongs(hashMD5(key)));
    }

    public void testBelongsWrapAround(){
        String serverId = "192.168.0.4";
        String predecessorId = "192.168.0.8";
        String hashedMiddle = "0152F5158DC3A9531907FB42C00A4138";
        range = new Range(meta.getMap().get(meta.findPredecessor(serverId)),meta.getMap().get(serverId));
        System.out.println(range);
        System.out.println("MiddleIndex=" + hashedMiddle);

        boolean start = range.belongs(hashMD5(predecessorId));
        boolean middle = range.belongs(hashedMiddle);
        boolean end = range.belongs(hashMD5(serverId));

        Assert.assertTrue(!start && middle && end);
    }

    @Test
    public void testAdmMessageCToText(){

        String serverId = "192.168.0.4";
        String cacheSize = Integer.toString(200);
        String strategy = "FIFO";
        range = new Range(meta.getMap().get(meta.findPredecessor(serverId)),meta.getMap().get(serverId));

        TextMessage test = msg.toText(AdmMessage.StatusType.INIT,
                cacheSize,
                strategy,
                meta,
                range,
                serverId);
        System.out.println(test.getMsg());
    }

    private String hashMD5(String key){
        if ( md5 == null ) {
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return (new HexBinaryAdapter()).marshal(md5.digest(key.getBytes()));
    }
}