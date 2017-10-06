package app_kvServer;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;

public class DiskStorage{


    public static final String PRIMARY_FILE = "primary.txt";
    public static final String R1_FILE = "r1.txt";
    public static final String R2_FILE = "r2.txt";


    private static DiskStorage singleton = null;

    public DiskStorage (){
        initialization();
    }

    public static void initialization(){
        File primary = new File(PRIMARY_FILE);
        File r1 = new File(R1_FILE);
        File r2 = new File(R2_FILE);

        try{
            primary.createNewFile();
            r1.createNewFile();
            r2.createNewFile();
            FileWriter fw = new FileWriter(PRIMARY_FILE);
            fw.write("");
            fw.close();
            fw = new FileWriter(R1_FILE);
            fw.write("");
            fw.close();
            fw = new FileWriter(R2_FILE);
            fw.write("");
            fw.close();
        }
        catch (IOException e) {
            System.out.println(e);
        }

    }

    public static DiskStorage getInstance(){
        if (singleton == null){
            singleton =  new DiskStorage();
        }

        return singleton;
    }

    public static String hashing(String key){
        int hash = 7;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash*31 + key.charAt(i))%1000000;
        }
        return Integer.toString(hash);

    }

    public static String checkFile(Integer file){
        switch (file){
            case 0:
                return PRIMARY_FILE;
            case 1:
                return R1_FILE;
            case 2:
                return R2_FILE;
            default:
                return PRIMARY_FILE;
        }
    }
    public static String get(String key_, Integer f){

        String file_name = checkFile(f);

        String target_line = Constant.KEY_START+":"+hashing(key_);
        BufferedReader br;
        FileReader fr;
        String val = "";

        try {

            fr = new FileReader(file_name);
            br = new BufferedReader(fr);

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {

                // if we find the key identifier
                if (sCurrentLine.equals(target_line)) {
                    String tmp = "";
                    sCurrentLine = br.readLine();

                    // get the get key and check if it is the right one
                    while (!sCurrentLine.equals(Constant.KEY_END)) {
                        tmp += sCurrentLine;
                        sCurrentLine = br.readLine();
                    }

                    if (!tmp.equals(key_))
                        continue;

                    br.readLine();
                    sCurrentLine = br.readLine();

                    //get the value and return
                    while (!sCurrentLine.equals(Constant.VAL_END)) {
                        val += sCurrentLine;
                        sCurrentLine = br.readLine();
                    }
                    br.close();
                    fr.close();
                    return val;

                }

            }
            br.close();
            fr.close();
        }
        catch(IOException e){
            e.printStackTrace();
            System.out.println(e);
        }
        return Constant.KEY_NOT_FOUND;
    }

    public static boolean set(String key_, String val_, Integer f){
        String file_name = checkFile(f);
        String target_line = Constant.KEY_START+":"+hashing(key_);

        try {

            //new input
            if (get(key_,f).equals(Constant.KEY_NOT_FOUND)){
                FileWriter writer = new FileWriter(file_name, true);
                writer.write(target_line+"\r\n");
                writer.write(key_);
                writer.write("\r\n"+Constant.KEY_END+"\r\n");
                writer.write(Constant.VAL_START+"\r\n");
                writer.write(val_);
                writer.write("\r\n"+Constant.VAL_END+"\r\n");
                writer.write("----------\n");
                writer.close();
                //System.out.println("insert complete!");
            }
            // update
            else{
                File file = new File ("temp.txt");
                try
                {
                    file.createNewFile();
                }
                catch(IOException ioe)
                {
                    System.out.println("Error while creating a new empty file :" + ioe);
                }

                FileReader fr = new FileReader(file_name);
                BufferedReader br = new BufferedReader(fr);
                FileWriter fw = new FileWriter("temp.txt");
                String line;
                while ((line = br.readLine()) != null ){
                    if (line.equals(target_line)){
                        while(!line.equals(Constant.VAL_END))
                            line = br.readLine();
                        br.readLine();
                        line = br.readLine();
                        fw.write(target_line+"\r\n");
                        fw.write(key_);
                        fw.write("\r\n"+Constant.KEY_END+"\r\n");
                        fw.write(Constant.VAL_START+"\r\n");
                        fw.write(val_);
                        fw.write("\r\n"+Constant.VAL_END+"\r\n");
                        fw.write("----------\n");
                    }
                    line = line + "\r\n";
                    fw.write(line);

                }
                fw.close();
                br.close();
                fr.close();
                File original = new File (file_name);
                file.renameTo(original);
                //System.out.println("update complete!");
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e);
            System.out.println("Insert Failed");
            return false;
        }
        return true;
    }
}

