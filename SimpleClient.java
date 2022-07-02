import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

public class SimpleClient {
    static final int PORT = 65123;

    // Server Host
    static List<String> serverHosts;

    static final String batchFile = "long.txt";
    static final String serverHostsFile = "servers.txt";

    static void loadServerHosts() {
        // load server hosts from file
        serverHosts = new ArrayList<String>();

        try (
                BufferedReader br = new BufferedReader(new FileReader(serverHostsFile));) {
            String line;
            while ((line = br.readLine()) != null)
                serverHosts.add(line);

        } catch (IOException e) {
            System.err.println("Couldn't read file " + serverHostsFile);
            return;
        }
    }

    static String sendServerRequest(String server, String req) {
        // connect to the server

        try (
                Socket socketOfClient = new Socket(server, PORT);
                BufferedReader is = new BufferedReader(new InputStreamReader(socketOfClient.getInputStream()));
                BufferedWriter os = new BufferedWriter(new OutputStreamWriter(socketOfClient.getOutputStream()));) {
            os.write(req);
            os.newLine();
            os.flush();

            String res = is.readLine();

            os.write("QUIT");
            os.newLine();
            os.flush();

            return res;

        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + server);
            e.printStackTrace();
            return null;
        } catch (IOException e) {

            System.err.println("Couldn't get I/O for the connection to " + server);
            e.printStackTrace();
            return null;
        }
    }

    static String[] divideText(String text, int nb_parts) {
        // split the text in nb_parts parts of equal number of lines

        String[] parts = new String[nb_parts];
        String[] lines = text.split("\n");

        int minLinesPerPart = lines.length / nb_parts;
        int linesRest = lines.length % nb_parts;

        int curLine = 0;

        for (int i = 0; i < nb_parts; i++) {
            int totalPartLines = minLinesPerPart;
            if (i < linesRest)
                totalPartLines++;

            for (int j = 0; j < totalPartLines; j++)
                parts[i] += lines[curLine++] + "\n";
        }

        assert curLine == lines.length;

        return parts;
    }

    static int fileLineCount(BufferedReader br) {
        int count = 0;
        try {
            while (br.readLine() != null)
                count++;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }

    public static void main(String[] args) {

        loadServerHosts();


        long start = System.currentTimeMillis();

        // build a string with all the servers names
        // separated with a ';'
        String request = "PEERS ";
        for (String s : serverHosts)
            request += s + ";";

        System.out.println("Request: " + request);

        // let the servers know about others
        for (String server : serverHosts) {
            String response = sendServerRequest(server, request);

            if (response == null) // no such host: remove server from list
                return;
        }

        
        long charcount = 0;
        
        // count characters
        try (BufferedReader fileReader = new BufferedReader(new FileReader(batchFile));) {
            while (fileReader.read() != -1) {
                charcount++; /*counts the number of characters read*/
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        
        long charsPerPart = charcount / serverHosts.size();

        
        // dispatch the file to the servers
        try (BufferedReader fileReader = new BufferedReader(new FileReader(batchFile));) {
            
            System.out.println("Dispatching file to servers...");

            
            // send the text to the servers
            for (int i = 0; i < serverHosts.size(); i++) {
                StringBuffer sb = new StringBuffer();

                // use a buffer to read the file
                // The size of this buffer is small enough
                // to fit in the CPU cache
                final int bufferSize = 2 * 1024 * 1024;
                char[] buffer = new char[(int) charsPerPart];

                fileReader.read(buffer, 0, (int) Math.min(bufferSize, (int) charcount));

                long read = charsPerPart;

                // send the rest of cut the word to the server
                String word = "";
                char c;
                do {

                    int v = fileReader.read();

                    c = (char) v;

                    if (v != -1) {
                        read++;
                        word += c;
                    } else
                        break;
                } while (c != ' ');

                
                sb.append(buffer);
                sb.append(word);

                System.out.println("Sending batch to " + serverHosts.get(i));

                byte[] b = sb.toString().getBytes();

                System.out.println("Sending " + b.length + " == " + read + " bytes");

                // print sb
                sendServerRequest(serverHosts.get(i), "SPLIT " + read + "\n" + sb.toString());
            }


            // wait for the servers to finish
            // and fetch the results
            System.out.println("Waiting for servers to finish...");
            
            
            boolean done;

            do {
                done = true;

                for (String server : serverHosts) {
                    String response = sendServerRequest(server, "STATUS");
                    done = Integer.parseInt(response) == serverHosts.size();

                    if (!done) {
                        System.out.println("Waiting for " + server + " to finish... %u", Integer.parseInt(response));
                        break;
                    }
                }

                if(!done)
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
            } while(!done);

            System.out.println("Finished! " + (System.currentTimeMillis() - start) + " ms");


            for (String server : serverHosts) {
                String response = sendServerRequest(server, "GET");

                String[] lines = response.split(";");

            }

            System.out.println("Results fetched! " + (System.currentTimeMillis() - start) + " ms");


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
