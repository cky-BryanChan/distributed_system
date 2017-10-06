package common.messages;

import app_kvServer.model.Metadata;
import org.json.JSONObject;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.security.Key;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.spec.SecretKeySpec;


public class KVMessageC implements KVMessage{

    private StatusType mStatus;
    private String mKey;
    private String mValue;
    private Metadata mMetaData;
    private String mAddress;
    private String mPort;
    private Key mCryptKey;
    private static String HMAC;

    public KVMessageC(StatusType status, String key, String value, Metadata meta) {
        mKey=key;
        mValue=value;
        mStatus=status;
        mMetaData=meta;
    }

    public KVMessageC(StatusType status){
        mStatus=status;
    }



    public void setMetadata(Metadata metadata) {
        this.mMetaData = metadata;
    }


    public void setCryptKey(Key cryptKey) {
        this.mCryptKey = cryptKey;
    }

    public void setKey(String key) {
        this.mKey = key;
    }

    public void setValue(String value) {
        this.mValue = value;
    }

    public void setAddress(String address) {
        this.mAddress = address;
    }

    public void setPort(String port) {
        this.mPort = port;
    }

    public void setHMAC(String HMAC) {
        this.HMAC = HMAC;
    }

    /**
     * create string with status key value in jason format
     * @param status status of the operation
     * @param key the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return the string created
     */
    public static TextMessage toText(StatusType status, String key, String value, Metadata meta) {

        key = (key!=null) ? key : "null";
        value = (value!=null) ? value : "null";

        HMAC = Metadata.hashMD5(key+value);
        JSONObject obj = new JSONObject();
            obj.put("status",status.toString());
            obj.put("key",key);
            obj.put("value",value);
            if (meta != null && meta.getMap() != null)
                obj.put("metadata",meta.getMap());
            obj.put("HMAC", HMAC);

        return new TextMessage(obj.toString());
    }



    public static TextMessage toText(StatusType status, Key cryptKey) {
        JSONObject obj = new JSONObject();
        obj.put("status", status.toString());
        if(cryptKey != null) {
            String encodedKey = Base64.encodeBase64String(cryptKey.getEncoded());
            obj.put("cryptKey", encodedKey);
        }
        else
            obj.put("cryptKey", "");
        return new TextMessage(obj.toString());
    }

    /**
     * parse the jason message and create a KVMessageC class with the parsed values
     * @param txtMsg a string message
     * @return KVMessageC class
     */
    public static KVMessageC toKVmessage(TextMessage txtMsg) {
        String strMsg = txtMsg.getMsg();

        JSONObject obj = new JSONObject(strMsg);

        KVMessageC msg = new KVMessageC(StatusType.valueOf(obj.getString("status")));

        HashMap<String,String> metaMap = new HashMap<>();
        if (obj.has("metadata")){
            JSONObject metadata = obj.getJSONObject("metadata");
            Iterator keys = metadata.keys();
            while (keys.hasNext()){
                String k = (String)keys.next();
                String v = metadata.getString(k);
                metaMap.put(k,v);
            }
            msg.setMetadata(new Metadata(metaMap));
        }


        msg.setKey( obj.has("key") ? obj.getString("key") : null);
        msg.setValue( obj.has("value") ? obj.getString("value") : null);
        msg.setHMAC( obj.has("HMAC") ? obj.getString("HMAC") : null);
        if(obj.has("cryptKey") && !Objects.equals(obj.getString("cryptKey"), "")){
            byte[] encodeKey = Base64.decodeBase64(obj.getString("cryptKey"));
            Key key = new SecretKeySpec(encodeKey,0,encodeKey.length, "AES");
            msg.setCryptKey(key);
        }

        return msg;
    }

    /**
     * @return the key that is associated with this message,
     * 		null if not key is associated.
     */
    @Override
    public String getKey() {
        if(mKey!=null && mKey.isEmpty())
            return "null";
        return mKey;
    }

    /**
     * @return the value that is associated with this message,
     * 		null if not value is associated.
     */
    @Override
    public String getValue() {
        if(mValue!= null && mValue.isEmpty())
            return "null";
        return mValue;
    }

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    @Override
    public StatusType getStatus() {
        return mStatus;
    }

    public Metadata getMetaData() {
        return mMetaData;
    }

    public Key get_cryptKey() {
        return mCryptKey;
    }

    public String getHMAC(){return HMAC;}
}

