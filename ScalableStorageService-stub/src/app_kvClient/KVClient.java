package app_kvClient;


import app_kvEcs.ECS_Server.SendMsg;
import app_kvServer.model.Metadata;
import client.Client;
import client.ClientSocketListener;
import client.Subscriber;
import common.messages.cryptography;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.messages.TextMessage;

public class KVClient implements ClientSocketListener, Subscriber {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private BufferedReader stdin;
    private Client client = null;
    private boolean stop = false;
    private List<String> subscribe_list = new ArrayList<>();


    private String serverAddress;
    private int serverPort;


    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    public Client getClient(){return this.client;}

    /**
     *handle command line entered by the user
     * @param cmdLine
     *          command line entered
     */
    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            disconnect();
            System.out.println(PROMPT + "Application exit!");
            System.exit(1);

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    connect(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length == 2) {
                if (tokens[1].length() <= 20) {
                    if (client != null && client.isRunning()) {
                        try {
                            client.put(tokens[1], "");
                        } catch (Exception e) {
                            printError("Put Fail");
                            printError(e.toString());
                        }
                    } else {
                        printError("Not connected!");
                    }
                } else {
                    printError("Maximum length for key is 20 chars!");
                }
            } else if (tokens.length == 3) {
                if (tokens[1].length() <= 20 && tokens[2].length() <= 120 * 1024) {
                    if (client != null && client.isRunning()) {
                        try {
                            client.put(tokens[1], tokens[2]);
                        } catch (Exception e) {
                            printError("Put Fail");
                            printError(e.toString());
                        }
                    } else {
                        printError("Not connected!");
                    }
                } else {
                    printError("Maximum length for key is 20 chars; " +
                            "Maximum length for value is 120*1024 chars!");
                }

            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (tokens[1].length() <= 20) {
                    if (client != null && client.isRunning()) {
                        try {
                            client.get(tokens[1]);
                        } catch (Exception e) {
                            printError("Get Fail");
                        }
                    } else {
                        printError("Not connected!");
                    }
                } else {
                    printError("Maximum length for key is 20 chars!");
                }
            } else {
                printError("Invalid number of parameters!");
            }
        } else if (tokens[0].equals("disconnect")) {
            disconnect();

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        }
        else if (tokens[0].equals("subscribe") || tokens[0].equals("sub")){
            if (tokens.length != 2)
                printError("Invalid number of parameters!");
            else {
                String key = tokens[1];
                subscribe(key);
            }
        }
        else if(tokens[0].equals("unsubscribe") || tokens[0].equals("unsub")){
            if (tokens.length != 2)
                printError("Invalid number of parameters!");
            else {
                String key = tokens[1];
                if(key.equals("all")){
                    unsubscribeAll();
                }
                else {
                    unsubscribe(key);
                }
            }
        }
        else if(tokens[0].equals("showsub")){
            printSubsribedList();
        }
        else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void connect(String address, int port)
            throws Exception {
        client = new Client(address, port, this);


        try {
            client.connect();
            System.out.print(PROMPT);
            System.out.println("Connected to server: /"+address+":"+port);

//            client.start();
        } catch (Exception e) {
            printError("Unable to connect to server: /"+address+":"+port);
            return;
        }
    }

    private void disconnect() {
        if(client != null) {
            client.disconnect();
            client = null;
        }
        System.out.print(PROMPT);
        System.out.println("Disconnected");
    }

    /**
     * print help message
     */
    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts or updates a key-value pair in the storage server\n " +
                "\t\t\t\t\t\t\t\t deletes entry for the given key if <value>=null \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t retrieves value from the storage server\n");
        sb.append(PROMPT).append("sub <key>");
        sb.append("\t\t subscribe to all the changes to that key\n");
        sb.append(PROMPT).append("unsub <key>");
        sb.append("\t unsubscribe that key\n");
        sb.append(PROMPT).append("unsub all");
        sb.append("\t\t unsubscribe all the subscribed keys\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program\n");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    /**
     * set log level
     * @param levelString
     * @return
     */
    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    @Override
    public void handleNewMessage(TextMessage msg) {
        if(!stop) {
            System.out.println(msg.getMsg());
            System.out.print(PROMPT);
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if(status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void subscribe(String key){
        String checkKey = Metadata.getCheckKey(subscribe_list,key);
        if(!subscribe_list.contains(checkKey)){
            if(client.subscribe(key)){
                System.out.println(PROMPT +"Subscribe successfully!");
            }
            else{
                System.out.println(PROMPT +"Subscribe failed!");
            }
        }else{
            System.out.println(PROMPT +"Already subscribed "+key);
        }
    }

    public void unsubscribe(String key){
            String checkKey = Metadata.getCheckKey(subscribe_list,key);
            if (subscribe_list.contains(checkKey)) {
                if(client.unsubscribe(key)){
                    System.out.println(PROMPT +"Unsubscribe successfully!");
                }
                else {
                    System.out.println(PROMPT +"Unsubscribe failed");
                }
            }else{
                System.out.println(PROMPT +"You did not subscribe "+key);
            }
    }

    public List<String> getSubList(){return subscribe_list;}

    public void printSubsribedList(){
        System.out.print(PROMPT+ "subscribed list: ");
        for (String key : subscribe_list){
            String tokens[] = key.split(":");
            System.out.print(tokens[0].trim()+" , ");
        }
        System.out.println("");
    }

    public void unsubscribeAll(){
        List<String> temp = new ArrayList<>();
        for (String key_port : subscribe_list){
            String tokens[] = key_port.split(":");
            String key = tokens[0].trim();
            System.out.println(PROMPT + "unsubscribing: "+key);
            temp.add(key);
        }

        for (String key: temp){
            unsubscribe(key);
        }
    }
    @Override
    public List<String> getScribers() {
        return this.subscribe_list;
    }

    @Override
    public void setScribers(List<String> list) {
        this.subscribe_list = list;
    }

}
