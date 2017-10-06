package testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

public class DataSet {
    public static String getContent(String file) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = br.readLine();
        br.close();
        return line;
    }

    public static HashMap<String, String> storeDataSet(final File folder)
            throws Exception {

        int count = 2;
        HashMap<String, String> data = new HashMap<String, String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                storeDataSet(fileEntry);
            } else {
                if(count > 0)
                    count --;
                else
                    data.put(fileEntry.getName(), getContent(folder.getAbsolutePath()+"/"+fileEntry.getName()));
            }
        }
        return data;
    }
}
