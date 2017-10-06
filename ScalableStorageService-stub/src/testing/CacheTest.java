package testing;

import app_kvServer.CacheStorage;
import app_kvServer.Constant;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * Test for cacheStorage store/retrieve functions
 */
public class CacheTest extends TestCase {

    static {
        CacheStorage.init("FIFO",20);
    }

    private CacheStorage cache;

    @Test
    public void testCacheInitialization() {
        cache = CacheStorage.getInstance();

        assertTrue(cache != null);
    }

    @Test
    public void testCachePutGet(){
        cache = CacheStorage.getInstance();
        cache.store("foo","bar");

        assertTrue(cache.retrieve("foo").equals("bar"));
    }

    @Test
    public void testCachePutGetNotFound(){
        cache = CacheStorage.getInstance();
        cache.store("123","456");

        assertTrue(cache.retrieve("1234").equals(Constant.retrieveErrorMsg));
    }

    @Test
    public void testCacheUpdate(){
        cache = CacheStorage.getInstance();
        cache.store("hello","world");
        String firstVal = cache.retrieve("hello");
        cache.store("hello","java");
        String secondVal = cache.retrieve("hello");

        assertTrue(firstVal.equals("world") && secondVal.equals("java"));
    }
}

