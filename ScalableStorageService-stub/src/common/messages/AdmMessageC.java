package common.messages;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;

public class AdmMessageC implements AdmMessage {

    private StatusType mCmd;

    public void setCacheSize(String cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    private String cacheSize = null;
    private String strategy = null;
    private Metadata metadata = null;
    private Range range = null;
    private String serverId = null;
    private String value = null;
    private String key = null;

    public AdmMessageC(StatusType mCmd) {
        this.mCmd = mCmd;
    }

    public AdmMessageC(StatusType mCmd_, String size, String strategy_, Metadata meta, String id, Range range_){
        mCmd = mCmd_;
        cacheSize = size;
        strategy = strategy_;
        metadata = meta;
        serverId = id;
        range = range_;

    }
    public AdmMessageC(StatusType mCmd_, String key_, String value_){
        mCmd = mCmd_;
        key = key_;
        value = value_;
    }


    public static TextMessage toText(StatusType mCmd,
                                     String cacheSize,
                                     String strategy,
                                     Metadata metadata,
                                     Range range,
                                     String serverId
    ){
        cacheSize = (cacheSize!=null) ? cacheSize : "null";
        strategy = (strategy!=null) ? strategy : "null";
        serverId = (serverId!=null) ? serverId : "null";


        JSONObject json = new JSONObject();
        json.put("Admin",true);
        json.put("status",mCmd.toString());
        json.put("cacheSize",cacheSize);
        json.put("strategy",strategy);
        if (metadata != null && metadata.getMap() != null)
            json.put("metadata",metadata.getMap());
        if (range != null) {
            JSONObject r = new JSONObject();
            r.put("startindex",range.getStartIndex());
            r.put("endindex",range.getEndIndex());
            json.put("range",r);
        }
        json.put("serverId",serverId);

        return new TextMessage(json.toString());

    }


    public static TextMessage toText(StatusType mCmd,
                                     String key,
                                     String value){
        key = (key!=null) ? key : "null";
        value = (value!=null) ? value : "null";


        JSONObject json = new JSONObject();
        json.put("Admin",true);
        json.put("status",mCmd.toString());
        json.put("key",key);
        json.put("value",value);

        return new TextMessage(json.toString());

    }

    public TextMessage toText(){

        JSONObject json = new JSONObject();
        json.put("Admin",true);
        json.put("status", mCmd.toString());
        json.put("cacheSize",cacheSize);
        json.put("strategy",strategy);
        if (metadata != null && metadata.getMap() != null)
            json.put("metadata",metadata.getMap());
        if (range != null) {
            JSONObject r = new JSONObject();
            r.put("startindex",range.getStartIndex());
            r.put("endindex",range.getEndIndex());
            json.put("range",r);
        }
        if(key != null)
            json.put("key",key);
        if(value != null)
            json.put("value",value);
        json.put("serverId",serverId);

        return new TextMessage(json.toString());
    }

    @Override
    public StatusType getStatus() {
        return mCmd;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String getCacheSize() {
        return cacheSize;
    }

    @Override
    public String getStrategy() {
        return strategy;
    }

    @Override
    public Range getRange() {
        return range;
    }

    @Override
    public String getServerId() {
        return serverId;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static boolean isAdminType(TextMessage msg){
        String string = msg.getMsg();
        JSONObject obj = new JSONObject(string);
        return obj.has("Admin") && obj.getBoolean("Admin") ;
    }

    public static AdmMessageC toAdmMessage(TextMessage latestMsg) {
        String string = latestMsg.getMsg();
        JSONObject obj = new JSONObject(string);

        AdmMessageC msg =  new AdmMessageC(StatusType.valueOf(obj.getString("status")));

        if (obj.has("metadata")){
            HashMap<String,String> metaMap = new HashMap<>();
            JSONObject metadata = obj.getJSONObject("metadata");
            Iterator keys = metadata.keys();
            while (keys.hasNext()){
                String k = (String)keys.next();
                String v = metadata.getString(k);
                metaMap.put(k,v);
            }
            msg.setMetadata(new Metadata(metaMap));
        }
        if (obj.has("range")){
            JSONObject r = obj.getJSONObject("range");
            Range range = new Range(
                    r.getString("startindex"),
                    r.getString("endindex"));
            msg.setRange(range);
        }

        msg.setKey( obj.has("key") ? obj.getString("key") : null);
        msg.setValue( obj.has("value") ? obj.getString("value") : null);
        msg.setCacheSize( obj.has("cacheSize") ? obj.getString("cacheSize") : null);
        msg.setStrategy( obj.has("strategy")   ? obj.getString("strategy") : null);
        msg.setServerId(  obj.has("serverId")  ? obj.getString("serverId")  : null);
        return msg;
    }
}
