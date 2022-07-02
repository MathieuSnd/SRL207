import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SimpleServerProgram {

    static final int PORT = 65123;

    static String[] peers;
    static Map<String, Integer> shuffleMap;

    static void sendSyncServerRequest(String server, String req) {
        // connect to the server
        try (
                Socket socketOfClient = new Socket(server, PORT);
                BufferedReader is = new BufferedReader(new InputStreamReader(socketOfClient.getInputStream()));
                BufferedWriter os = new BufferedWriter(new OutputStreamWriter(socketOfClient.getOutputStream()));) {

            os.write(req);
            os.newLine();
            os.flush();
            os.write("QUIT");
            os.newLine();
            os.flush();

        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + server);
            return;
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " + server);
            return;
        }
    }

    static final int max_threads = 128;
    static Integer n_threads = 0;

    private static int input_shuffle_requests = 0;

    static void sendServerRequest(String server, String req) {
        String req_cpy = new String(req);
        // assynchronous request

        while (n_threads == max_threads) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        new Thread(
                new Runnable() {
                    public void run() {
                        synchronized (n_threads) {
                            n_threads++;
                        }

                        sendSyncServerRequest(server, req_cpy);

                        synchronized (n_threads) {
                            n_threads--;
                        }
                    }
                }).start();
    }

    static String encodeValue(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            System.out.println("Error encoding value: " + value);
            return "???";
        }
    }

    static void splitAndMap(String path) {

        HashMap<String, Integer> map = new HashMap<String, Integer>();


        System.out.println("BEGIN MAP");

        // read each line of the file
        try (
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));) {
            String line;
            while ((line = br.readLine()) != null) {
                // split the line
                String[] words = line.split(" ");

                for (int i = 0; i < words.length; i++)
                    words[i] = encodeValue(words[i]);

                // count occurences of each word
                for (String word : words) {
                    Integer count = map.get(word);
                    map.put(word, (count == null) ? 1 : count + 1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }


        System.out.println("CREATE SHUFFLE MAPS");


        // make a map to gather shuffle requests

        String[] shuffleMap = new String[peers.length];

        for (int i = 0; i < peers.length; i++)
            shuffleMap[i] = "";

        // fill shuffle map:
        // compute hashes of words
        for (String word : map.keySet()) {
            // hash of 'word'
            int hash = word.hashCode();

            if (hash < 0)
                hash = -hash;

            int serverId = hash % peers.length;

            shuffleMap[serverId] += word + " " + map.get(word) + "\n";
        }

        System.out.println("SEND SHUFFLE REQUESTS");

        // send requests to the right servers
        for (int i = 0; i < peers.length; i++) {
            String serverHost = peers[i];
            String req = shuffleMap[i];
            // if(req.length() > 0)

            // send to every machine, even if there is nothing
            // to shuffle. This way, every machine can use a
            // counter of machines that sent a shuffle request
            // and then detect when the processing is done.
            sendServerRequest(serverHost, "SHUFFLE\n" + req);
        }

        System.out.println("Done");
    }

    static synchronized void serveShuffle(String word, int count) {
        // increment the count to the shuffle map

        if (shuffleMap == null)
            shuffleMap = new HashMap<String, Integer>();

        Integer currentCount = shuffleMap.get(word);

        if (currentCount == null) {
            shuffleMap.put(word, count);
        } else {
            shuffleMap.put(word, currentCount + count);
        }
    }

    static void downloadFile(String path, BufferedReader br, int bytes) {
        try (
                BufferedWriter bw = new BufferedWriter(new FileWriter(path));) {
            // buffered read
            final int BUFFER_SIZE = 2 * 1024 * 1024;
            char[] buffer = new char[BUFFER_SIZE];
            int n;

            // read exactly bytes bytes
            while (bytes > 0) {
                n = br.read(buffer, 0, Math.min(bytes, BUFFER_SIZE));
                if (n == -1) {
                    System.out.println("downloadFile warning: unexpected end of file");
                    break;
                }

                bw.write(buffer, 0, n);
                bytes -= n;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

        ServerSocket listener = null;

        try {
            listener = new ServerSocket(PORT);
        } catch (IOException e) {
            System.out.println("cannot listen to port " + PORT + ": " + e);
            System.exit(1);
        }

        System.out.println("Listening on port " + PORT);

        // Try to open a server socket on port PORT
        // Note that we can't choose a port less than 1023 if we are not
        // privileged users (root)
        while (true) {
            serve(listener);
        }
    }

    static Object mutex = new Object();

    static void serve(ServerSocket listener) {

        String line;
        BufferedReader is;
        BufferedWriter os;
        Socket socketOfServer = null;

        try {

            // Accept client connection request
            // Get new Socket at Server.
            socketOfServer = listener.accept();

            // Open input and output streams

            // new BufferedInputStream(

            is = new BufferedReader(new InputStreamReader(socketOfServer.getInputStream()));
            os = new BufferedWriter(new OutputStreamWriter(socketOfServer.getOutputStream()));

            while (true) {
                // Read data to the server (sent from client).
                line = is.readLine();
                if (line == null) {
                    System.out.println("Client disconnected");
                } else
                    System.out.println("input request: " + line);

                if (line.startsWith("PEERS ")) {
                    // parse peers
                    try {
                        peers = line.split(" ")[1].split(";");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (line.startsWith("SPLIT ")) {
                    // read file size
                    int bytes = Integer.parseInt(line.split(" ")[1]);

                    // download to a file
                    downloadFile("/tmp/mserandour/splits", is, bytes);

                    new Thread(
                            new Runnable() {
                                public void run() {
                                    // split and map
                                    splitAndMap("/tmp/mserandour/splits");
                                }
                            }).start();
                }
                else if (line.equals("SHUFFLE")) {
                    // shuffle

                    while (true) {
                        line = is.readLine();

                        if (line.equals(""))
                            break;

                        String[] words = line.split(" ");

                        serveShuffle(words[0], Integer.parseInt(words[1]));
                    }

                    synchronized (mutex) {
                        input_shuffle_requests++;
                    }
                } else if (line.startsWith("GET")) {
                    os.write(input_shuffle_requests % peers.length + "\n");
                    if (shuffleMap != null)
                        for (String word : shuffleMap.keySet()) {
                            String res = word + "=" + shuffleMap.get(word) + ";";
                            os.write(res);
                        }
                } else if(line.equals("STATUS")) {
                      os.write(input_shuffle_requests % peers.length + "\n");
                } else if (line.equals("QUIT"))
                    break;

                os.newLine();
                os.flush();
            }

        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
    }
}
