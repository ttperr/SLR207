import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map.Entry;

public class SLAVE {
    private static final String USERNAME = "tperrot-21";

    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String MAP_DIRECTORY = HOME_DIRECTORY + "/maps";
    private static final String SHUFFLE_DIRECTORY = HOME_DIRECTORY + "/shuffles";
    private static final String SHUFFLE_RECEIVED_DIRECTORY = HOME_DIRECTORY + "/shufflesreceived";
    private static final String REDUCE_DIRECTORY = HOME_DIRECTORY + "/reduces";

    private static final String MACHINES_FILE = HOME_DIRECTORY + "/machines.txt";

    private static final int PORT = 8888;
    private final HashMap<Integer, String> machines = new HashMap<>();
    private int machineId;

    private BufferedReader readerMaster;
    private BufferedWriter writerMaster;

    public static void main(String[] args) throws NumberFormatException, InterruptedException {
        new SLAVE();
    }

    public SLAVE() {

        File machinesFile = new File(MACHINES_FILE);
        getMachines(machinesFile);

        try {
            ServerSocket serverSocket = new ServerSocket(PORT);
            System.out.println("Waiting for master...");
            Socket masterSocket = serverSocket.accept();

            readerMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            writerMaster = new BufferedWriter(new OutputStreamWriter(masterSocket.getOutputStream()));

            System.out.println("Connected to master on port " + PORT);

            String line;
            while (true) {
                line = readerMaster.readLine();
                if (line == null) {
                    continue;
                }
                System.out.println("Received: " + line);
                if (line.equals("QUIT")) {
                    break;
                } else if (line.startsWith("launchMap")) {
                    String inputFile = line.split(" ")[1];
                    launchMap(inputFile);
                    sayDone();
                } else if (line.startsWith("launchShuffle")) {
                    String inputFile = line.split(" ")[1];
                    launchShuffle(inputFile);
                    processShuffleOutput();
                    sayDone();
                } else if (line.startsWith("Shuffle: ")) {
                  String shuffleReceived = line.split(" ")[1];
                  processShuffleReceived(shuffleReceived);
                } else if (line.equals("launchReduce")) {
                    launchReduce();
                    sayDone();
                } else if (line.startsWith("runCommand")) {
                    String command = line.split(" ")[1];

                    // Execute line
                    Process process = Runtime.getRuntime().exec(command);
                    int code = process.waitFor();
                    System.out.println("Code: " + code);

                    // Send back done.
                    sayDone();
                } else {
                    System.err.println("Invalid command.");
                }
            }
            readerMaster.close();
            writerMaster.close();
            masterSocket.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sayDone() throws IOException {
        writerMaster.write("done.\n");
        writerMaster.newLine();
        writerMaster.flush();
    }

    private void launchMap(String inputFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            String line;
            HashMap<String, Integer> wordCountMap = new HashMap<>();

            while ((line = reader.readLine()) != null) {
                String[] words = line.split(" ");
                for (String word : words) {
                    if (wordCountMap.containsKey(word)) {
                        int count = wordCountMap.get(word);
                        wordCountMap.put(word, count + 1);
                    } else {
                        wordCountMap.put(word, 1);
                    }
                }
            }

            reader.close();

            File mapDirectory = new File(MAP_DIRECTORY);
            mapDirectory.mkdirs();

            String outputFileName = "UM" + inputFile.substring(inputFile.indexOf("S") + 1, inputFile.indexOf(".txt"))
                    + ".txt";
            String outputFilePath = MAP_DIRECTORY + File.separator + outputFileName;
            File outputFile = new File(outputFilePath);
            outputFile.createNewFile();

            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            for (String word : wordCountMap.keySet()) {
                int count = wordCountMap.get(word);
                writer.write(word + ", " + count);
                writer.newLine();
            }

            writer.close();

            System.out.println("Map calculation completed for file " + inputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void launchShuffle(String inputFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            shuffleDirectory.mkdirs();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split(", ");
                if (keyValue.length == 2) {
                    String key = keyValue[0];
                    String value = keyValue[1];
                    int hash = key.hashCode();
                    String hostname = machines.get(hash % machines.size());
                    String outputFileName = hash + "-" + hostname + ".txt";
                    String outputFilePath = SHUFFLE_DIRECTORY + File.separator + outputFileName;
                    File outputFile = new File(outputFilePath);
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true));
                    writer.write(key + ", " + value);
                    writer.newLine();
                    writer.close();
                }
            }

            reader.close();

            System.out.println("Shuffle calculation completed for file " + inputFile);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processShuffleReceived(String shuffleReceived) {
        try {
            String key = shuffleReceived.split(", ")[0];
            int hash = key.hashCode();
            String hostname = machines.get(hash % machines.size());

            String filename = hash + "-" + hostname + ".txt";
            File shuffleReceivedFile = new File(SHUFFLE_RECEIVED_DIRECTORY + File.separator + filename);
            shuffleReceivedFile.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(shuffleReceivedFile, true));
            writer.write(shuffleReceived);
            writer.newLine();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getMachines(File machinesFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(machinesFile));
            String line;
            int machineNumber = 0;
            while ((line = reader.readLine()) != null) {
                machines.put(machineNumber, line);
                if(line.equals(InetAddress.getLocalHost().getHostAddress()))
                    machineId = machineNumber;
                machineNumber++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processShuffleOutput() {
        try {
            File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY);
            shuffleReceivedDirectory.mkdirs();
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            File[] shuffleFiles = shuffleDirectory.listFiles();

            assert shuffleFiles != null;
            for (File shuffleFile : shuffleFiles) {
                int hash = Integer.parseInt(shuffleFile.getName().split("-")[0]);
                int machineNumber = hash % machines.size();
                // String ipAddress = machines.get(hash % machines.size());
                // String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SHUFFLE_RECEIVED_DIRECTORY);

                if (machineNumber == machineId) {
                    Runtime.getRuntime().exec("cp " + shuffleFile.getAbsolutePath() + " " + SHUFFLE_RECEIVED_DIRECTORY);
                } else {
                    // Read the line of the file
                    BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                    String line = reader.readLine();
                    reader.close();

                    writerMaster.write("Send: " + machineNumber);
                    writerMaster.newLine();
                    writerMaster.write(line);
                    writerMaster.newLine();
                    writerMaster.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void launchReduce() {
        try {
            File reduceDirectory = new File(REDUCE_DIRECTORY);
            reduceDirectory.mkdirs();

            File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY + File.separator);
            File[] shuffleReceivedFiles = shuffleReceivedDirectory.listFiles();

            HashMap<String, Integer> wordCountMap = new HashMap<>();

            assert shuffleReceivedFiles != null;
            for (File shuffleReceivedFile : shuffleReceivedFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(shuffleReceivedFile))) {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        String[] keyValue = line.split(", ");
                        if (keyValue.length == 2) {
                            String key = keyValue[0];
                            int value = Integer.parseInt(keyValue[1]);
                            wordCountMap.merge(key, value, Integer::sum);
                        }
                    }
                }
            }

            for (Entry<String, Integer> entry : wordCountMap.entrySet()) {
                String key = entry.getKey();
                int count = entry.getValue();
                int hash = key.hashCode();
                String outputFileName = hash + ".txt";
                String outputFilePath = REDUCE_DIRECTORY + File.separator + outputFileName;
                File outputFile = new File(outputFilePath);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                    writer.write(key + ", " + count);
                    writer.newLine();
                }
            }

            System.out.println("Reduce calculation completed");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
