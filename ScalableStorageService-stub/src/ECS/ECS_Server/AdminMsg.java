package ECS.ECS_Server;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;
import common.messages.AdmMessage.StatusType;
import common.messages.TextMessage;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * Created by bryanchan on 3/4/17.
 */
public class AdminMsg {
    public StatusType mCmd;
    public String cacheSize;
    public String strategy;
    public Metadata metadata;
    public String serverId;
    public Range range;

    public AdminMsg(StatusType mCmd_, String size, String strategy_, Metadata meta, String id, Range range_){
        mCmd = mCmd_;
        cacheSize = size;
        strategy = strategy_;
        metadata = meta;
        serverId = id;
        range = range_;

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
        json.put("serverId",serverId);

        return new TextMessage(json.toString());
    }

    public static AdminMsg toAdminMsg(TextMessage reply) {

        String string = reply.getMsg();
        JSONObject obj = new JSONObject(string);

        StatusType Status = StatusType.valueOf(obj.has("status") ? obj.getString("status") : null);
        String Size = obj.has("cacheSize") ? obj.getString("cacheSize") : null;
        String Strategy = obj.has("serverId") ? obj.getString("serverId") : null;
        String ServerId = obj.has("serverId") ? obj.getString("serverId") : null;

        Metadata metadata = null;
        Range range = null;

        if (obj.has("metadata")) {
            String str = obj.get("metadata").toString();
            HashMap<String, String> meta = new HashMap<>();
            for (String row : str.split(",")) {
                String k = row.split(":")[0];
                String v = row.split(":")[1];
                meta.put(k, v);
            }
            metadata = new Metadata(meta);
        }

        if (obj.has("range")){
            JSONObject r = obj.getJSONObject("range");
            range = new Range(
                    r.getString("startindex"),
                    r.getString("endindex"));
        }


        return new AdminMsg(Status,Size,Strategy,metadata,ServerId,range);
    }
}
