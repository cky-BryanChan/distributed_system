package app_kvServer.model;

import java.util.HashMap;

/**
 * Model class created for every in-memory cache records
 * stored timeStamp (when created)
 *        lastAccess  (updated on each time used)
 *        frequency
 *        reverseKey (back link to its key)
 */
public class Record {
    private long timeStamp;
    private long lastAccess;
    private int frequency;
    private String value;
    private String reverseKey;
    private HashMap<Integer,String> subscribers;

    public Record(long timeStamp, int frequency, long lastAccess, String value, String reverseKey) {
        this.timeStamp = timeStamp;
        this.frequency = frequency;
        this.lastAccess = lastAccess;
        this.value = value;
        this.reverseKey = reverseKey;
        this.subscribers = new HashMap<>();
    }

    // Auto generated get/set and other utils function

    public String getReverseKey() {
        return reverseKey;
    }

    public void setReverseKey(String reverseKey) {
        this.reverseKey = reverseKey;
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public void setLastAccess(long lastAccess) {
        this.lastAccess = lastAccess;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public HashMap<Integer,String> getSubscribers() { return this.subscribers; }

    public void setSubscribers(HashMap<Integer, String> subscribers) {
        this.subscribers = subscribers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record record = (Record) o;

        if (timeStamp != record.timeStamp) return false;
        if (lastAccess != record.lastAccess) return false;
        if (frequency != record.frequency) return false;
        return value.equals(record.value);

    }

    @Override
    public int hashCode() {
        int result = (int) (timeStamp ^ (timeStamp >>> 32));
        result = 31 * result + (int) (lastAccess ^ (lastAccess >>> 32));
        result = 31 * result + frequency;
        result = 31 * result + value.hashCode();
        return result;
    }
}
