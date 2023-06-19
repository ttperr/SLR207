import java.io.*;
import java.net.InetAddress;
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

    private static final int PORT = 8888;
    private final ArrayList<String> machines = new ArrayList<>();
    private final Socket[] sockets;
    private ServerSocket serverSocket;
    private int machineId;

    private BufferedReader readerMaster;
    private PrintWriter writerMaster;

    public static void main(String[] args) throws NumberFormatException, InterruptedException {
        new SLAVE();
    }

    public SLAVE() {

        File machinesFile = new File(MACHINES_FILE);
        getMachines(machinesFile);
        sockets = new Socket[machines.size()];

        try {
            serverSocket = new ServerSocket(PORT);
            System.out.println("Waiting for master...");
            Socket masterSocket = serverSocket.accept();

            readerMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            writerMaster = new PrintWriter(masterSocket.getOutputStream(), true);

            System.out.println(
                    "Connected to : " + masterSocket.getInetAddress().getCanonicalHostName() + " on port " + PORT);


            String line;
            while (true) {
                line = readerMaster.readLine();
                System.out.println("Received: " + line);

                if (line.equals("QUIT")) {
                    break;

                } else if (line.equals("connectEachOther")) {
                    connectEachOther();
                    sayDoneToMaster();
                } else if (line.startsWith("launchMap")) {
                    String inputFile = line.split(" ")[1];
                    launchMap(inputFile);
                    sayDoneToMaster();

                } else if (line.startsWith("launchShuffle")) {
                    String inputFile = line.split(" ")[1];
                    launchShuffle(inputFile);

                    endOfShuffle();

                    sayDoneToMaster();

                } else if (line.equals("launchReduce")) {
                    launchReduce();
                    sayDoneToMaster();

                } else if (line.startsWith("launchResult")) {
                    launchResult();
                    sayDoneToMaster();

                } else if (line.startsWith("runCommand")) {
                    String command = line.substring("runCommand ".length());

                    // Execute line command.
                    Process process = Runtime.getRuntime().exec(command);
                    int code = process.waitFor();
                    System.out.println("Code: " + code);

                    // Send back done.
                    sayDoneToMaster();

                } else {
                    throw new IllegalStateException("Unexpected message: " + line);
                }
            }
            System.out.println("Closing connection...");
            readerMaster.close();
            writerMaster.close();
            masterSocket.close();
            serverSocket.close();
            for (Socket socket : sockets) {
                if (socket != null) {
                    socket.close();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void endOfShuffle() {
        Thread shuffleThread = new Thread(this::processShuffleOutput);
        ArrayList<Thread> shuffleReceiveThreads = new ArrayList<>();

        for(int i = 0; i < sockets.length; i++) {
            Socket socket = sockets[i];
            if (socket != null && i != machineId) {
                int finalI = i;
                Thread shuffleReceiveThread = new Thread(() -> {
                    try {
                        listenShuffleMessages(finalI);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                shuffleReceiveThreads.add(shuffleReceiveThread);
                shuffleReceiveThread.start();
            }
        }

        // Démarrer l'exécution du thread
        shuffleThread.start();

        // Attendre la fin du thread
        try {
            shuffleThread.join();
            for (Thread shuffleReceiveThread : shuffleReceiveThreads) {
                shuffleReceiveThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sayDoneToMaster() throws IOException {
        writerMaster.println("DONE.");
    }

    private void connectEachOther() throws IOException {
        Thread connectThread = new Thread(() -> {
            try {
                for (int machineId = 0; machineId < machines.size(); machineId++) {
                    // Lock sockets
                    synchronized (sockets) {
                        if (machineId != this.machineId && sockets[machineId] == null) {
                            sockets[machineId] = new Socket(machines.get(machineId), PORT);
                            System.out.println("Connected to " + machines.get(machineId));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread acceptThread = new Thread(() -> {
            try {
                for (int machineId = 0; machineId < machines.size(); machineId++) {
                    // Lock sockets
                    synchronized (sockets) {
                        if (machineId != this.machineId && sockets[machineId] == null) {
                            sockets[machineId] = serverSocket.accept();
                            System.out.println("Connected to " + machines.get(machineId));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        connectThread.start();
        acceptThread.start();

        try {
            connectThread.join();
            acceptThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
                    String outputFileName = getShuffleFilename(key);
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

    private void processShuffleOutput() {
        try {
            File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY);
            shuffleReceivedDirectory.mkdirs();
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            File[] shuffleFiles = shuffleDirectory.listFiles();

            assert shuffleFiles != null;
            for (File shuffleFile : shuffleFiles) {
                String hashString = shuffleFile.getName().split("_")[0];
                int hash = Integer.parseInt(hashString);
                int machineNumber = hash % machines.size();
                if (machineNumber < 0) {
                    machineNumber += machines.size();
                }

                if (!machines.get(machineNumber).equals(shuffleFile.getName().substring(hashString.length() + 1,
                        shuffleFile.getName().indexOf(".txt")))) {
                    System.err.println("Machine number: " + machineNumber);
                    System.err.println(machines.get(machineNumber));
                    System.err.println(shuffleFile.getName().substring(hashString.length(),
                            shuffleFile.getName().indexOf(".txt")));
                    throw new IllegalStateException("Wrong machine number");
                }

                if (machineNumber == machineId) {
                    Runtime.getRuntime().exec("cp " + shuffleFile.getAbsolutePath() + " " + SHUFFLE_RECEIVED_DIRECTORY);
                } else {
                    // Read the line of the file
                    BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                    String line = reader.readLine();
                    reader.close();

                    //writerMaster.println("Send: " + machineNumber);
                    //writerMaster.println(line);

                    PrintWriter writer = new PrintWriter(sockets[machineNumber].getOutputStream(), true);
                    writer.println("ShuffleReceived: " + line);
                    writer.close();

                    System.out.println("Message sent to machine " + machineNumber + ": " + line);
                }
            }
            for (Socket socket : sockets) {
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                writer.println("DONE.");
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenShuffleMessages(int machineNumber) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(sockets[machineNumber].getInputStream()));
        String line;
        while (true) {
            if ((line = reader.readLine()) != null) {
                System.out.println("Message received from machine " + machineNumber + ": " + line);

                if (line.startsWith("ShuffleReceived: ")) {
                    processShuffleReceived(line.substring("ShuffleReceived: ".length()));
                } else if (line.equals("DONE.")) {
                    break;
                } else {
                    throw new IllegalStateException("Unknown message: " + line);
                }
            }
        }
    }

    private void processShuffleReceived(String shuffleReceived) {
        try {
            String key = shuffleReceived.split(", ")[0];
            String filename = getShuffleFilename(key);
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

    private String getShuffleFilename(String key) {
        int hash = key.hashCode();
        int machineId = hash % machines.size();
        if (machineId < 0) {
            machineId += machines.size();
        }
        String hostname = machines.get(machineId);

        return hash + "_" + hostname + ".txt";
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

    private void launchResult() {
        File reduceDirectory = new File(REDUCE_DIRECTORY);
        File[] reduceFiles = reduceDirectory.listFiles();

        assert reduceFiles != null;
        for (File reduceFile : reduceFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(reduceFile))) {
                String line;
                line = reader.readLine();
                writerMaster.println("Results: " + line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getMachines(File machinesFile) {
        try {

            BufferedReader reader = new BufferedReader(new FileReader(machinesFile));
            String line;
            int machineNumber = 0;
            while ((line = reader.readLine()) != null) {
                machines.add(line);
                if (line.equals(InetAddress.getLocalHost().getCanonicalHostName()))
                    machineId = machineNumber;
                machineNumber++;
            }
            reader.close();
            System.out.println("Machine: " + machineId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
