package testing;

import app_kvServer.DiskStorage;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;

public class DiskStorageTest extends TestCase {

    public static final String PRIMARY_FILE = "primary.txt";
    public static final String R1_FILE = "r1.txt";
    public static final String R2_FILE = "r2.txt";
    public static final String KEY_NOT_FOUND = "key not found";


    @Test
    public void testFileInitialization() {
        DiskStorage.initialization();
        File primary = new File(PRIMARY_FILE);
        File r1 = new File(R1_FILE);
        File r2 = new File(R2_FILE);
        assertTrue(primary.exists());
        assertTrue(r1.exists());
        assertTrue(r2.exists());
    }

    @Test
    public void testSetAndGet(){


        //primary get, r1,r2 don't get
        assertTrue(DiskStorage.set("foo","bar",0));
        assertTrue(DiskStorage.set("goo","qar",0));
        assertTrue(DiskStorage.set("hoo","war",0));
        assertTrue(DiskStorage.set("joo","ear",0));
        assertTrue(DiskStorage.set("koo","rar",0));

        assertTrue(DiskStorage.get("foo",0).equals("bar"));
        assertTrue(DiskStorage.get("goo",0).equals("qar"));
        assertTrue(DiskStorage.get("hoo",0).equals("war"));
        assertTrue(DiskStorage.get("joo",0).equals("ear"));
        assertTrue(DiskStorage.get("koo",0).equals("rar"));

        assertFalse(DiskStorage.get("foo",1).equals("bar"));
        assertFalse(DiskStorage.get("goo",1).equals("qar"));
        assertFalse(DiskStorage.get("hoo",1).equals("war"));
        assertFalse(DiskStorage.get("joo",1).equals("ear"));
        assertFalse(DiskStorage.get("koo",1).equals("rar"));

        assertFalse(DiskStorage.get("foo",2).equals("bar"));
        assertFalse(DiskStorage.get("goo",2).equals("qar"));
        assertFalse(DiskStorage.get("hoo",2).equals("war"));
        assertFalse(DiskStorage.get("joo",2).equals("ear"));
        assertFalse(DiskStorage.get("koo",2).equals("rar"));

        //r1 get, primary,r2 don't get
        assertTrue(DiskStorage.set("AA","aa",1));
        assertTrue(DiskStorage.set("BB","bb",1));
        assertTrue(DiskStorage.set("CC","cc",1));
        assertTrue(DiskStorage.set("DD","dd",1));
        assertTrue(DiskStorage.set("EE","ee",1));

        assertTrue(DiskStorage.get("AA",1).equals("aa"));
        assertTrue(DiskStorage.get("BB",1).equals("bb"));
        assertTrue(DiskStorage.get("CC",1).equals("cc"));
        assertTrue(DiskStorage.get("DD",1).equals("dd"));
        assertTrue(DiskStorage.get("EE",1).equals("ee"));

        assertFalse(DiskStorage.get("AA",0).equals("aa"));
        assertFalse(DiskStorage.get("BB",0).equals("bb"));
        assertFalse(DiskStorage.get("CC",0).equals("cc"));
        assertFalse(DiskStorage.get("DD",0).equals("dd"));
        assertFalse(DiskStorage.get("EE",0).equals("ee"));

        assertFalse(DiskStorage.get("AA",2).equals("aa"));
        assertFalse(DiskStorage.get("BB",2).equals("bb"));
        assertFalse(DiskStorage.get("CC",2).equals("cc"));
        assertFalse(DiskStorage.get("DD",2).equals("dd"));
        assertFalse(DiskStorage.get("EE",2).equals("ee"));
    }

    @Test
    public void testKeyNotFound(){
        assertTrue(DiskStorage.get("non_set",0).equals(KEY_NOT_FOUND));
    }

    @Test
    public void testUpdate(){
        assertTrue(DiskStorage.set("foo","bar2",0));
        assertTrue(DiskStorage.get("foo",0).equals("bar2"));
    }

}

