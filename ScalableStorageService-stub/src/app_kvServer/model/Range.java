package app_kvServer.model;

public class Range {
    private String startIndex; //exclusive
    private String endIndex;  //inclusive


    //auto generated method
    public Range(String startIndex, String endIndex) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public String getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(String startIndex) {
        this.startIndex = startIndex;
    }

    public String getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(String endIndex) {
        this.endIndex = endIndex;
    }

    @Override
    public String toString() {
        return "Range{" +
                "startIndex='" + startIndex + '\'' +
                ", endIndex='" + endIndex + '\'' +
                '}';
    }
    /**
     * Check if given hashed key is within this.server responsible range
     * @param hashed MD5 hashed of user provided key
     * @return whether the given hashed is belongs to range
     */
    public boolean belongs (String hashed){
        //one server
        if(this.startIndex.equals(this.endIndex))
            return true;
        if (hashed.equals(startIndex)) return false;
        else if (hashed.equals(endIndex)) return true;
        else{
            if (startIndex.compareTo(endIndex) < 0){ // normal, start < end
                return startIndex.compareTo(hashed) < 0 && endIndex.compareTo(hashed) > 0;
            }
            else { // wrap around
                return !(startIndex.compareTo(hashed) > 0 && endIndex.compareTo(hashed) < 0);
            }
        }
    }
}
