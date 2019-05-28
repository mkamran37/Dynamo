package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {
    static ArrayList<String> ports = new ArrayList<String>();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static String myPort = null;
    static Context context;
    static String successor = null;
    static boolean flag2 = false;
    static volatile boolean isInserted = false;
    static String[] cols = {"key", "value"};
    static MatrixCursor provider;
    static ConcurrentHashMap<String, ArrayList<String>> val;
    static String message;
    static ConcurrentHashMap<String, ConcurrentHashMap<String, String>> replicateKeys = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
    static ConcurrentHashMap<String, String> myKeys = new ConcurrentHashMap<String, String>();
    static ConcurrentHashMap<String, String> files = new ConcurrentHashMap<String, String>();
    static List<String> nodes = Collections.synchronizedList(new ArrayList<String>());
    static boolean joinComplete;
    static boolean ringmade = false;



    /* ON CREATE FUNCTIONALITY*/
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        joinComplete = false;
        context = getContext();
        TelephonyManager telephonyManager = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String phoneNumber = telephonyManager.getLine1Number();
        myPort = phoneNumber.substring(phoneNumber.length() - 4, phoneNumber.length());
        nodes.add("5556");
        nodes.add("5554");
        nodes.add("5558");
        nodes.add("5560");
        nodes.add("5562");
        try {
            //Gives the port number
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "join", myPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    protected static void doJoin(String port) {
        if(ringmade) {
            int i = nodes.indexOf(port);
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "return", port, nodes.get(i));
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "return", port, nodes.get(i));
            i = nodes.indexOf(port);
            if (i == 0) {
                i = nodes.size() - 1;
            } else {
                i = i - 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "myPred", port, nodes.get(i));
            if (i == 0) {
                i = nodes.size() - 1;
            } else {
                i = i - 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "myPred", port, nodes.get(i));
            joinComplete = true;
        }
        else{
            joinComplete = true;
        }
    } //done

    /*DELETE FUNCTIONALITY*/
    protected static void del(String selection, String port){
        files.remove(selection);
        if(!port.equals(myPort)) {
            if(replicateKeys.containsKey(port)) {
                ConcurrentHashMap<String, String> tmp = replicateKeys.get(port);
                tmp.remove(selection);
                replicateKeys.put(port, tmp);
            }
        }
        else{
            myKeys.remove(selection);
        }
    }
    protected static void deleteAll(String fin_port){
        if(myPort!=fin_port){
            files.clear();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*",myPort, successor);
        }
        else{
            return;
        }
    }
    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (selection.equals("@")) {
           files.clear();
            return 1;
        }
        else if(selection.equals("*")){
            files.clear();
            for(String node : nodes){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*",myPort, node);
            }
            return 1;
        }
        else {
            int i = 0;
            try {
                for (i = 0; i < nodes.size(); i++) {
                    if (i == 0) {
                        if (genHash(nodes.get(i)).compareTo(genHash(nodes.get(nodes.size() - 1))) < 0) {
                            if (!(genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(nodes.size() - 1))) > 0)) {
                                break;
                            }
                        } else {
                            if (genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(nodes.size() - 1))) > 0) {
                                break;
                            }
                        }

                    } else {
                        if (genHash(nodes.get(i)).compareTo(genHash(nodes.get(i - 1))) < 0) {
                            if (!(genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(i - 1))) > 0)) {
                                break;
                            }
                        } else {
                            if (genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(i - 1))) > 0) {
                                break;
                            }
                        }
                    }
                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "del", nodes.get(i), selection, myPort);
            if(i == nodes.size() - 1){
                i = 0;
            }
            else{
                i = i+1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "del", nodes.get(i), selection, myPort);
            if(i == nodes.size() - 1){
                i = 0;
            }
            else{
                i = i+1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "del", nodes.get(i), selection, myPort);
        }

        return 0;
    }
    protected static int doDelete(String selection){
        File f = new File(context.getFilesDir(), selection + ".txt");
        if (f.delete()) {
            return 1;
        }
        return -1;
    }

    /*QUERY FUNCTIONALITY*/
    @Override
    public synchronized MatrixCursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        String k1 = "";
        String k2 = "";
        String k3 = "";
        if (selection.equals("@")) {
            provider = new MatrixCursor(cols);
            for (String file : files.keySet()) {
                String[] tmp = {file, files.get(file)};
                provider.addRow(tmp);
            }
            return provider;
        } else if (selection.equals("*")) {
            String[] cols = {"key", "value"};
            provider = new MatrixCursor(cols);
            message = "";
            for (String file : files.keySet()) {
                String[] tmp = {file, files.get(file)};
                provider.addRow(tmp);
            }
            int i = nodes.indexOf(myPort);
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "doAll", myPort, nodes.get(i), message);
            while (!flag2) {
            }
            flag2 = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "doAll", myPort, nodes.get(i), message);
            while (!flag2) {
            }
            flag2 = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "doAll", myPort, nodes.get(i), message);
            while (!flag2) {
            }
            flag2 = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "doAll", myPort, nodes.get(i), message);
            while (!flag2) {
            }
            flag2 = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "doAll", myPort, nodes.get(i), message);
            while (!flag2) {
            }
            flag2 = false;
            return provider;
        } else {
            int i = 0;
            try {
                while (i < nodes.size()) {
                    if (i == 0) {
                        if (genHash(nodes.get(i)).compareTo(genHash(nodes.get(nodes.size() - 1))) < 0) {
                            if (!(genHash(selection).compareTo(genHash(nodes.get(i))) > 0 && genHash(selection).compareTo(genHash(nodes.get(nodes.size() - 1))) < 0)) {
                                break;
                            }
                        } else {
                            if (genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(nodes.size() - 1))) > 0) {
                                break;
                            }
                        }

                    } else {
                        if (genHash(nodes.get(i)).compareTo(genHash(nodes.get(i - 1))) < 0) {
                            if (!(genHash(selection).compareTo(genHash(nodes.get(i))) > 0 && genHash(selection).compareTo(genHash(nodes.get(i - 1))) < 0)) {
                                break;
                            }
                        } else {
                            if (genHash(selection).compareTo(genHash(nodes.get(i))) < 0 && genHash(selection).compareTo(genHash(nodes.get(i - 1))) > 0) {
                                break;
                            }
                        }
                    }
                    i++;
                }
                Socket s = null;
                try {
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(nodes.get(i)) * 2);
                    OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter out = new BufferedWriter(o);
                    String msgToSend = new StringBuffer().append("query").append("//").append(myPort).append("//").append(nodes.get(i)).append("//").append(selection).append("\n").toString();
                    out.write(msgToSend);
                    out.flush();
                    int timer = 0;
                    InputStreamReader in = new InputStreamReader(s.getInputStream());
                    BufferedReader br = new BufferedReader(in);
                    while ((k1 = br.readLine()) == null && ++timer <= 100) {
                    }
                    k1 = k1 == null ? "" : k1;
                    out.close();
                } catch (IOException e) {
                    Log.e("Client Task", "ClientTask UnknownHostException");
                }
                if (i == nodes.size() - 1) {
                    i = 0;
                } else {
                    i = i + 1;
                }
                try {
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(nodes.get(i)) * 2);
                    OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter out = new BufferedWriter(o);
                    String msgToSend = new StringBuffer().append("query").append("//").append(myPort).append("//").append(nodes.get(i)).append("//").append(selection).append("\n").toString();
                    out.write(msgToSend);
                    out.flush();
                    int timer = 0;
                    InputStreamReader in = new InputStreamReader(s.getInputStream());
                    BufferedReader br = new BufferedReader(in);
                    while ((k2 = br.readLine()) == null && ++timer <= 100) {
                    }
                    k2 = k2 == null ? "" : k2;
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (i == nodes.size() - 1) {
                    i = 0;
                } else {
                    i = i + 1;
                }
                try {
                    s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(nodes.get(i)) * 2);
                    OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter out = new BufferedWriter(o);
                    String msgToSend = new StringBuffer().append("query").append("//").append(myPort).append("//").append(nodes.get(i)).append("//").append(selection).append("\n").toString();
                    out.write(msgToSend);
                    out.flush();
                    int timer = 0;
                    InputStreamReader in = new InputStreamReader(s.getInputStream());
                    BufferedReader br = new BufferedReader(in);
                    while ((k3 = br.readLine()) == null && ++timer <= 100) {
                    }
                    k3 = k3 == null ? "" : k3;
                    out.close();
                } catch (IOException e) {
                    Log.e("Client Task", "ClientTask UnknownHostException");
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        String result = "";

        if (k1.length() > 0 && k2.length() > 0 && k1.equals(k2)){
            result = k1;
        } else if (k2.length() > 0 && k3.length() > 0 && k3.equals(k2)) {
            result = k2;
        } else {
            result = k3;
        }
        String[] tmp = {selection, result};
        provider = new MatrixCursor(cols);
        provider.addRow(tmp);
        return provider;
    }
    protected static void afterAll(String mssg){
        if(mssg.equals("empty")){
            flag2=true;
        }
        else {
            if(mssg.length() > 1){
                String[] tokens = mssg.split(">>");
                for (String token : tokens) {
                    String[] tmp = token.split("!!");
                    String k = tmp[0];
                    String v = tmp[1];
                    String[] res = {k, v};
                    provider.addRow(res);
                }
            }
            flag2 = true;
        }
    }
    public static void doAll(String fin_port, String mssg){
        for (String file : files.keySet()) {

            mssg = file + "!!" + files.get(file) + ">>" + mssg;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "done", fin_port, mssg);
    }
    protected static void doQuery(String final_port, String keyToSearch, Socket info){
        OutputStreamWriter o = null;
        try {
            o = new OutputStreamWriter(info.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter br = new BufferedWriter(o);
        String mssg = "";
        if(myKeys.containsKey(keyToSearch)){
            mssg = myKeys.get(keyToSearch)+"\n";
        }
        else if(files.containsKey(keyToSearch)) {
            mssg = files.get(keyToSearch) + "\n";
        }
        try {
            br.write(mssg);
            br.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    } //done

    /*MISCELLANEOUS FUNCTIONALITY*/
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
    protected static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    } //done
    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    /*INSERT FUNCTIONALITY*/
    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        ringmade = true;
        Set<String> keys = values.keySet();
        String value = "";
        String mykey = null;
        for (String k : keys) {
            if (k.equals("key")) {
                mykey = values.getAsString(k);
            } else {
                value = values.getAsString(k);
            }
        }
        try {
            int i = 0;
            while(i < nodes.size()) {
                if(i == 0) {
                    if(genHash(nodes.get(0)).compareTo(genHash(nodes.get(nodes.size() - 1))) < 0) {
                        if (!(genHash(mykey).compareTo(genHash(nodes.get(i))) > 0 && genHash(mykey).compareTo(genHash(nodes.get(nodes.size() - 1))) < 0)) {
                            break;
                        }
                    } else {
                        if (genHash(mykey).compareTo(genHash(nodes.get(i))) < 0 && genHash(mykey).compareTo(genHash(nodes.get(nodes.size() - 1))) > 0) {
                            break;
                        }
                    }

                }
                else {
                    if(genHash(nodes.get(i)).compareTo(genHash(nodes.get(i - 1))) < 0) {
                        if (!(genHash(mykey).compareTo(genHash(nodes.get(i))) > 0 && genHash(mykey).compareTo(genHash(nodes.get(i - 1))) < 0)) {
                            break;
                        }
                    } else {
                        if (genHash(mykey).compareTo(genHash(nodes.get(i))) < 0 && genHash(mykey).compareTo(genHash(nodes.get(i - 1))) > 0) {
                            break;
                        }
                    }
                }
                i++;
            }
            String main = nodes.get(i);
//            isInserted = false;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", myPort, nodes.get(i), mykey, value).get(1000, TimeUnit.MILLISECONDS);
//            while(!isInserted){}
//            isInserted = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertReplicate", myPort, nodes.get(i), mykey, value, main).get(1000, TimeUnit.MILLISECONDS);
//            while(!isInserted){}
//            isInserted = false;
            if (i == nodes.size() - 1) {
                i = 0;
            } else {
                i = i + 1;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertReplicate", myPort, nodes.get(i), mykey, value, main).get(1000, TimeUnit.MILLISECONDS);
//            while(!isInserted){}
//            isInserted = false;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        return uri;
    }
    protected static void insertReplicate(String finalPort, String mykey, String value, String main){
        if(!replicateKeys.containsKey(main)){
            ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>();
            tmp.put(mykey,value);
            replicateKeys.put(main, tmp);
        } else{
            ConcurrentHashMap<String, String> tmp = replicateKeys.get(main);
            tmp.put(mykey, value);
            replicateKeys.put(main, tmp);
        }
        files.put(mykey,value);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "InsertDone", finalPort);
    }
    protected static void doInsert(String finalPort, String mykey, String value){
        myKeys.put(mykey,value);
        files.put(mykey,value);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "InsertDone", finalPort);
    } //done

    /*REPLICATION FUNCTIONALITY*/
    protected static void replicate(String mssg){
        String[] mssgs = mssg.split(">>");
        for(String msg : mssgs){
            String[] kv = msg.split("<<");
            if(!myKeys.containsKey(kv[0]));
                myKeys.put(kv[0],kv[1]);
            if(!files.containsKey(kv[0]))
                files.put(kv[0],kv[1]);
        }
    }
    protected static void predReplicate(String main, String mssg){
        String[] mssgs = mssg.split(">>");
        for(String msg : mssgs) {
            String[] kv = msg.split("<<");
            if (!replicateKeys.containsKey(main)) {
                ConcurrentHashMap<String, String> tmp = new ConcurrentHashMap<String, String>();
                tmp.put(kv[0], kv[1]);
                replicateKeys.put(main, tmp);
            } else {
                ConcurrentHashMap<String, String> tmp = replicateKeys.get(main);
                if(!tmp.containsKey(kv[0])) {
                    tmp.put(kv[0], kv[1]);
                    replicateKeys.put(main, tmp);
                }
            }
            if(!files.containsKey(kv[0]))
                files.put(kv[0],kv[1]);
        }
        joinComplete = true;
    }
    protected static void doReturn(String fin_port){
        if(replicateKeys.containsKey(fin_port)) {
            ConcurrentHashMap<String, String> tmp = replicateKeys.get(fin_port);
            Set<String> keys = tmp.keySet();
            String mssg = "";
            for (String key : keys) {
                mssg = mssg + key + "<<" + tmp.get(key) + ">>";
            }
            if(mssg.length() < 1){
                return;
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replicate", fin_port, mssg);
        }
    }
    protected static void doMyPred(String fin_port){
        Set<String> keys = myKeys.keySet();
        String mssg = "";
        for(String key : keys){
            mssg = mssg + key + "<<" + myKeys.get(key)+">>";
        }
        if(mssg.length() <=0){
            return;
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "predreplicate",fin_port, myPort, mssg);
    }


}

class ClientTask extends AsyncTask<String, Void, Void> {
    @Override
    protected Void doInBackground(String... msgs) {
        if (msgs[0].equals("join")) {
            try {
                SimpleDynamoProvider.ports.add(SimpleDynamoProvider.REMOTE_PORT0);
                SimpleDynamoProvider.ports.add(SimpleDynamoProvider.REMOTE_PORT1);
                SimpleDynamoProvider.ports.add(SimpleDynamoProvider.REMOTE_PORT2);
                SimpleDynamoProvider.ports.add(SimpleDynamoProvider.REMOTE_PORT3);
                SimpleDynamoProvider.ports.add(SimpleDynamoProvider.REMOTE_PORT4);
                for(int i = 0; i < 5; i++) {
                    Integer remPort = Integer.parseInt(SimpleDynamoProvider.ports.get(i));
                    Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            remPort);
                    OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                    BufferedWriter out = new BufferedWriter(o);
                    String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                    out.write(msgToSend);
                    out.flush();
                    out.close();
                }
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if (msgs[0].equals("set")) {
            try {
                Integer remPort = Integer.parseInt(msgs[2]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuilder().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }else if (msgs[0].equals("predreplicate")) {
            try {
                Integer remPort = Integer.parseInt(msgs[1]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuilder().append(msgs[0]).append("//").append(msgs[2]).append("//").append(msgs[3]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if (msgs[0].equals("insert")) {
            try {
                Integer remPort = Integer.parseInt(msgs[2]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("//").append(msgs[3]).append("//").append(msgs[4]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if(msgs[0].equals("insertReplicate")){
            try {
                Integer remPort = Integer.parseInt(msgs[2]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("//").append(msgs[3]).append("//").append(msgs[4]).append("//").append(msgs[5]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if(msgs[0].equals("return")){
            try {
                Integer remPort = Integer.parseInt(msgs[2]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("myPred")) {
            try {
                Integer remPort = Integer.parseInt(msgs[2]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if (msgs[0].equals("query")) {
            Socket s = null;
            int i = 0;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[2]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("//").append(msgs[2]).append("//").append(msgs[3]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if(msgs[0].equals("doAll")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[2]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuilder().append(msgs[0]).append("//").append(msgs[1]).append("//").append(msgs[3]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }

        }else if (msgs[0].equals("delete")) {
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[2]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("found")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[2]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("done")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[2]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("*")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[2]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[1]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("InsertDone")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = msgs[0];
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }else if(msgs[0].equals("joinComplete")){
            Socket s = null;
            try {
                s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[1]) * 2);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = msgs[0];
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if (msgs[0].equals("replicate")){
            try {
                Integer remPort = Integer.parseInt(msgs[1]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[2]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        } else if(msgs[0].equals("del")){
            try {
                Integer remPort = Integer.parseInt(msgs[1]) * 2;
                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remPort);
                OutputStreamWriter o = new OutputStreamWriter(s.getOutputStream());
                BufferedWriter out = new BufferedWriter(o);
                String msgToSend = new StringBuffer().append(msgs[0]).append("//").append(msgs[2]).append("//").append(msgs[3]).append("\n").toString();
                out.write(msgToSend);
                out.flush();
                out.close();
            } catch (IOException e) {
                Log.e("Client Task", "ClientTask UnknownHostException");
            }
        }
        return null;
    }
}

class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    protected Void doInBackground(ServerSocket... sockets) {
        ServerSocket serverSocket = sockets[0];
        while (true) try {
            serverSocket.setSoTimeout(500);
            Socket info = serverSocket.accept();
            InputStreamReader i = new InputStreamReader(info.getInputStream());
            BufferedReader in = new BufferedReader(i);
            String line = "";
            line = in.readLine();
            if (line != null) {
                String[] tokens = line.split("//");
                if (tokens[0].equals("join"))
                    SimpleDynamoProvider.doJoin(tokens[1]);
                else if (tokens[0].equals("insert")) {
                    String port = tokens[1];
                    String key = tokens[2];
                    String value = tokens[3];
                    SimpleDynamoProvider.doInsert(port, key, value);
                } else if (tokens[0].equals("insertReplicate")) {
                    String port = tokens[1];
                    String key = tokens[2];
                    String value = tokens[3];
                    String main = tokens[4];
                    SimpleDynamoProvider.insertReplicate(port, key, value, main);
                } else if (tokens[0].equals("query")) {
                    String fin_port = tokens[1];
                    String dest_port = tokens[2];
                    String key = tokens[3];
                    SimpleDynamoProvider.doQuery(fin_port, key, info);
                } else if (tokens[0].equals("doAll")) {
                    String port = tokens[1];
                    String mssg = "";
                    if (tokens.length > 2)
                        mssg = tokens[2];
                    SimpleDynamoProvider.doAll(port, mssg);
                } else if (tokens[0].equals("delete")) {
                    SimpleDynamoProvider.doDelete(tokens[1]);
                } else if (tokens[0].equals("done")) {
                    if (tokens.length > 1)
                        SimpleDynamoProvider.afterAll(tokens[1]);
                    else {
                        SimpleDynamoProvider.afterAll("empty");
                    }
                } else if (tokens[0].equals("*")) {
                    SimpleDynamoProvider.deleteAll(tokens[1]);
                } else if (tokens[0].equals("InsertDone")) {
                    SimpleDynamoProvider.isInserted = true;
                } else if (tokens[0].equals("joinComplete")) {
                    SimpleDynamoProvider.joinComplete = true;
                } else if (tokens[0].equals("return")) {
                    SimpleDynamoProvider.doReturn(tokens[1]);
                } else if (tokens[0].equals("replicate")) {
                    SimpleDynamoProvider.replicate(tokens[1]);
                } else if (tokens[0].equals("myPred")) {
                    SimpleDynamoProvider.doMyPred(tokens[1]);
                } else if (tokens[0].equals("predreplicate")) {
                    SimpleDynamoProvider.predReplicate(tokens[1], tokens[2]);
                } else if (tokens[0].equals("del")) {
                    SimpleDynamoProvider.del(tokens[1], tokens[2]);
                }
            }
        } catch(SocketTimeoutException e){
            SimpleDynamoProvider.isInserted = true;
            SimpleDynamoProvider.flag2 = true;
        } catch (IOException e1) {
            break;
        }
        return null;
    }
}

