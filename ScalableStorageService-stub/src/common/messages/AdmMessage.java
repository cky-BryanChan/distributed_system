package common.messages;

import app_kvServer.model.Metadata;
import app_kvServer.model.Range;

public interface AdmMessage {

    public enum StatusType {
        INIT,
        INIT_ERROR,
        INIT_SUCCESS,
        START,
        START_ERROR,
        START_SUCCESS,
        STOP,
        STOP_ERROR,
        STOP_SUCCESS,
        SHUTDOWN,
        LOCKWRITE,
        LOCKWRITE_ERROR,
        LOCKWRITE_SUCCESS,
        UNLOCKWRITE,
        UNLOCKWRITE_ERROR,
        UNLOCKWRITE_SUCCESS,
        MOVEDATA,
        MOVEDATA_ERROR,
        MOVEDATA_COMPLETED,
        MOVEDATA_SRC_INITED,
        MOVEDATA_SRC_ACKED,
        UPDATE,
        UPDATE_ERROR,
        UPDATE_SUCCESS,
        UNKNOWN_MIN,
        REPLICA_PUT,
        NOT_REPLICA,
        PUT_SUCCESS,     		/* Put - request successful, tuple inserted */
        PUT_UPDATE,      		/* Put - request successful, i.e., value updated */
        PUT_ERROR,       		/* Put - request not successful */
        DELETE_SUCCESS,  		/* Delete - request successful */
        DELETE_ERROR,     		/* Delete - request successful */
        ECS_SOCKET,
        ECS_SOCKET_RECEIVED,
        // ADD_REMOVE_NODE
        SUCCESSOR_RM_NODE,
        SUCCESSOR_RM_NODE_SUCCESS,
        R1_CLEAR,
        R1_CLEAR_SUCCESS,
        R2_CLEAR,
        R2_CLEAR_SUCCESS,
        R1_GET,
        R1_GET_DONE,
        R2_GET,
        R2_GET_DONE,
        PRIMARY_GET,
        PRIMARY_GET_DONE,
        R1_PUT,
        R1_PUT_DONE,
        R2_PUT,
        R2_PUT_DONE,
        DONOTSEND
    }

    public StatusType getStatus();

    public Metadata getMetadata();

    public String getCacheSize();

    public String getStrategy();

    public Range getRange();

    public String getServerId();

    public String getKey();

    public String getValue();

}
