import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
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

    private static final HashMap<Integer, String> machines = new HashMap<>();
    private static final ArrayList<Socket> clients = new ArrayList<>();
    private static final String MASTER_ADDRESS = "tp-3a101-00.enst.fr";
    private static final int PORT = 8888;


    public SLAVE(int mode, String inputFile) {
        if (mode == 0) {
            launchMap(inputFile);
        } else if (mode == 1) {
            launchShuffle(inputFile);
            processShuffleOutput();
        } else {
            System.err.println("Invalid mode.");
            System.err.println("Usage: java -jar SLAVE.jar <mode> <inputFile>");
            System.exit(1);
        }
    }
    public SLAVE(int mode) {
        if (mode == 2) {
            launchReduce();
        } else {
            System.err.println("Invalid mode.");
            System.err.println("Usage: java -jar SLAVE.jar <mode>");
            System.exit(1);
        }
    }

    public SLAVE(int mode, String masterAddress, int port) throws InterruptedException {
        if (mode == 9) {
            System.out.println("Connecting to " + masterAddress + " on port " + port);
            try {
                Socket clientSocket = new Socket(masterAddress, port);
                clients.add(clientSocket);

                BufferedReader readerMaster = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                BufferedWriter writerMaster = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

                System.out.println("Connected to " + masterAddress + " on port " + port);

                String line;
                while (true) {
                    line = readerMaster.readLine();
                    System.out.println("Received: " + line);
                    if (line.equals("QUIT")) {
                        break;
                    }
                    // Execute line
                    Process process = Runtime.getRuntime().exec(line);
                    int code = process.waitFor();
                    System.out.println("Code: " + code);
                    // Send back done.
                    writerMaster.write(code + "\n");
                    writerMaster.flush();
                }
                readerMaster.close();
                writerMaster.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Invalid mode.");
            System.err.println("Usage: java -jar SLAVE.jar <mode> <masterAddress> <port>");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws NumberFormatException, InterruptedException {
        if (args.length > 3) {
            System.err.println("Usage: java -jar SLAVE.jar <mode> <inputFile || serverAddress> <port>");
            return;
        }

        int mode = Integer.parseInt(args[0]);

        System.out.println("Mode: " + mode);

        File machinesFile = new File(MACHINES_FILE);
        getMachines(machinesFile);

        if (args.length == 1) {
            new SLAVE(mode);
        } else if (args.length == 2) {
            new SLAVE(mode, args[1]);
        } else {
            new SLAVE(mode, args[1], Integer.parseInt(args[2]));
        }
    }

    private static void launchMap(String inputFile) {
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

    private static void launchShuffle(String inputFile) {
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

    private static void getMachines(File machinesFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(machinesFile));
            String line;
            int machineNumber = 0;
            while ((line = reader.readLine()) != null) {
                machines.put(machineNumber, line);
                machineNumber++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processShuffleOutput() {
        try {
            File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY);
            shuffleReceivedDirectory.mkdirs();
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            File[] shuffleFiles = shuffleDirectory.listFiles();

            assert shuffleFiles != null;
            for (File shuffleFile : shuffleFiles) {
                int hash = Integer.parseInt(shuffleFile.getName().split("-")[0]);
                String ipAddress = machines.get(hash % machines.size());
                String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SHUFFLE_RECEIVED_DIRECTORY);

                ProcessBuilder pb = new ProcessBuilder("scp", shuffleFile.getAbsolutePath(), machineDirectory);
                Process p = pb.start();
                p.waitFor();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void launchReduce() {
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
