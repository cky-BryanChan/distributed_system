package app_kvServer;

// Place to put bunch of constants
public class Constant {
    public static final String FIFO = "FIFO";
    public static final String LRU = "LRU";
    public static final String LFU = "LFU";

    // Message
    public static final String initServerMsg = "Initialize server with cache size: %d strategy %s";

    //ErrorMessage
    public static final String retrieveErrorMsg = "Error!KeyNotFound";
    public static final String badRequestErrorMsg = "Error!BadRequest";
    public static final String UnknownRequestErrorMsg = "Error!Request";
    public static final String KEY_NOT_FOUND = "key not found";
    public static final String KEY_START =">>> key start <<<" ;
    public static final String KEY_END =">>> key end <<<";
    public static final String VAL_START =">>> val start <<<" ;
    public static final String VAL_END =">>> val end <<<" ;

}

