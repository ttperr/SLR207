import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

public class SLAVE {
    private static final String USERNAME = "tperrot-21";

    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String MAP_DIRECTORY = HOME_DIRECTORY + "/mapsCount";
    private static final String SHUFFLE_DIRECTORY = HOME_DIRECTORY + "/shufflesCount";
    private static final String SHUFFLE_RECEIVED_DIRECTORY = HOME_DIRECTORY + "/shufflesreceivedCount";
    private static final String REDUCE_DIRECTORY = HOME_DIRECTORY + "/reducesCount";

    private static final String MAP_SORT_DIRECTORY = HOME_DIRECTORY + "/mapsSort";
    private static final String SHUFFLE_SORT_DIRECTORY = HOME_DIRECTORY + "/shufflesSort";
    private static final String REDUCE_SORT_DIRECTORY = HOME_DIRECTORY + "/reducesSort";

    private static final String MACHINES_FILE = HOME_DIRECTORY + "/machines.txt";

    private static final String BIG_FILE_TO_PROCESS = "/cal/commoncrawl/CC-MAIN-20230320083513-20230320113513-00000.warc.wet";

    private static final int PORT = 8888;
    private final ArrayList<String> machines = new ArrayList<>();
    private final Socket[] socketsOutput;
    private final Socket[] socketsInput;
    private final PrintWriter[] writers;
    private final BufferedReader[] readers;
    private ServerSocket serverSocket;
    private int machineId;

    private BufferedReader readerMaster;
    private PrintWriter writerMaster;

    private int start;
    private int size;

    private static final double linesToAnalyze = 100000;
    private double linesPerMachine ;
    public static final boolean isTest = false;
    public static final boolean verbose = false;

    public static void main(String[] args) throws NumberFormatException, InterruptedException, IOException {
        new SLAVE();
    }

    public SLAVE() {

        File machinesFile = new File(MACHINES_FILE);
        getMachines(machinesFile);
        socketsOutput = new Socket[machines.size()];
        socketsInput = new Socket[machines.size()];
        writers = new PrintWriter[machines.size()];
        readers = new BufferedReader[machines.size()];

        linesPerMachine = linesToAnalyze / machines.size();

        try {
            serverSocket = new ServerSocket(PORT);
            System.out.println("Waiting for master...");
            Socket masterSocket = serverSocket.accept();

            writerMaster = new PrintWriter(masterSocket.getOutputStream(), true);
            readerMaster = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

            System.out.println(
                    "Connected to : " + masterSocket.getInetAddress().getCanonicalHostName() + " on port " + PORT);

            String line;
            while (true) {
                line = readerMaster.readLine();
                System.out.println("\nReceived: " + line);

                if (line.equals("QUIT")) {
                    break;

                } else if (line.equals("connectEachOther")) {
                    connectEachOther();
                    System.out.println("Connected each other.");
                    sayDoneToMaster();
                } else if (line.startsWith("launchMapCount")) {
                    if (isTest) {
                        String inputFile = line.split(" ")[1];
                        launchMapCount(inputFile, 0, Double.POSITIVE_INFINITY);
                    } else {
                        launchMapCountOnBigFile();
                    }

                    sayDoneToMaster();

                } else if (line.equals("launchShuffleCount")) {
                    launchShuffleCount();

                    endOfShuffleCount();

                    sayDoneToMaster();

                } else if (line.equals("launchReduceCount")) {
                    launchReduceCount();
                    sayDoneToMaster();

                } else if (line.equals("launchMapSort")) {
                    launchMapSort();
                    sayDoneToMaster();

                } else if (line.equals("launchShuffleSort")) {
                    launchShuffleSort();
                    endOfShuffleSort();
                    sayDoneToMaster();

                } else if (line.equals("launchReduceSort")) {
                    launchReduceSort();
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
            for (PrintWriter writer : writers) {
                if (writer != null) {
                    writer.close();
                }
            }
            for (BufferedReader reader : readers) {
                if (reader != null) {
                    reader.close();
                }
            }
            serverSocket.close();
            for (Socket socket : socketsOutput) {
                if (socket != null) {
                    socket.close();
                }
            }
        } catch (IOException | InterruptedException e) {
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
                    if (machineId != this.machineId) {
                        socketsOutput[machineId] = new Socket(machines.get(machineId), PORT);
                        writers[machineId] = new PrintWriter(socketsOutput[machineId].getOutputStream(), true);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        Thread acceptThread = new Thread(() -> {
            try {
                for (int count = 0; count < machines.size() - 1; count++) {
                    Socket tempSocket = serverSocket.accept();
                    String address = tempSocket.getInetAddress().getCanonicalHostName();
                    int machineId = machines.indexOf(address);
                    socketsInput[machineId] = tempSocket;
                    readers[machineId] = new BufferedReader(
                            new InputStreamReader(tempSocket.getInputStream()));
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

    private void launchMapCount(String inputFile, double start, double numberOfLine) throws IOException {
        System.out.println("Launching map on " + inputFile + " from " + start + " to " + (start + numberOfLine));

        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;
        HashMap<String, Integer> wordCountMap = new HashMap<>();
        int lineRead = 0;
        while ((line = reader.readLine()) != null && lineRead < start + numberOfLine) {
            if (lineRead >= start) {
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
            lineRead++;
        }

        reader.close();

        File mapDirectory = new File(MAP_DIRECTORY);
        mapDirectory.mkdirs();

        String outputFileName = "UM" + machineId + ".txt";
        String outputFilePath = MAP_DIRECTORY + File.separator + outputFileName;
        File outputFile = new File(outputFilePath);
        outputFile.createNewFile();

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

        for (String word : wordCountMap.keySet()) {
            int count = wordCountMap.get(word);
            writer.write(word + ", " + count + "\n");
        }

        writer.close();

        System.out.println("Map calculation completed for file " + inputFile);
    }

    private void launchMapCountOnBigFile() {
        // Cut the file in machines.size() parts.

        try {

            if (linesPerMachine == -1) {
                BufferedReader reader = new BufferedReader(new FileReader(SLAVE.BIG_FILE_TO_PROCESS));
                double lines = 0;
                while (reader.readLine() != null) {
                    lines++;
                }
                reader.close();
                System.out.println("Lines: " + lines);
                linesPerMachine = lines / machines.size();
            }

            System.out.println("Lines per machine: " + linesPerMachine);
            launchMapCount(SLAVE.BIG_FILE_TO_PROCESS, machineId * linesPerMachine, linesPerMachine);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void launchShuffleCount() throws IOException {
        String inputFile = MAP_DIRECTORY + File.separator + "UM" + machineId + ".txt";
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
                writer.write(key + ", " + value + "\n");
                writer.close();
            }
        }

        reader.close();

        System.out.println("Shuffle calculation completed for file " + inputFile);

    }

    private void sendShufflesCount() throws IOException {
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
                File shuffleReceivedFile = new File(
                        SHUFFLE_RECEIVED_DIRECTORY + File.separator + shuffleFile.getName());
                // create shuffle received file
                shuffleReceivedFile.createNewFile();
                // read all lines of shuffle file
                BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                String line = reader.readLine();
                reader.close();
                // append the line to the shuffle received file
                BufferedWriter writer = new BufferedWriter(new FileWriter(shuffleReceivedFile, true));
                writer.write(line + "\n");
                writer.close();
            } else {
                // Read the line of the file
                BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                String line = reader.readLine();
                reader.close();

                // writerMaster.println("Send: " + machineNumber);
                // writerMaster.println(line);
                writers[machineNumber].println("ShuffleReceivedCount: " + line);

                if (verbose) {
                    System.out.println("Message sent to machine " + machineNumber + ": " + line);
                }
            }
        }
        for (PrintWriter writer : writers) {
            if (writer != null) {
                writer.println("DONE.");
            }
        }
    }

    private void listenShufflesMessages(int machineNumber) throws IOException {
        String line;
        while (true) {
            if ((line = readers[machineNumber].readLine()) != null) {
                if (verbose) {
                    System.out.println("Message received from machine " + machineNumber + ": " + line);
                }

                if (line.startsWith("ShuffleReceivedCount: ")) {
                    processShuffleReceived(line.substring("ShuffleReceivedCount: ".length()));
                } else if (line.startsWith("ShuffleReceivedSort: ")) {
                    // TODO processShuffleReceivedSort(line.substring("ShuffleReceivedSort: ".length()));
                } else if (line.equals("DONE.")) {
                    break;
                } else {
                    throw new IllegalStateException("Unknown message: " + line);
                } 
            }
        }
    }

    private void endOfShuffleCount() throws IOException {
        System.out.println("End of shuffle");
        Thread shuffleThread = new Thread(() -> {
            try {
                sendShufflesCount();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        ArrayList<Thread> listenShufflesThreads = new ArrayList<>();

        for (int i = 0; i < socketsOutput.length; i++) {
            Socket socket = socketsOutput[i];
            if (socket != null && i != machineId) {
                int finalI = i;
                Thread shuffleReceiveThread = new Thread(() -> {
                    try {
                        listenShufflesMessages(finalI);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                listenShufflesThreads.add(shuffleReceiveThread);
                shuffleReceiveThread.start();
            }
        }
        // Démarrer l'exécution du thread
        shuffleThread.start();

        // Attendre la fin du thread
        try {
            for (Thread listenShufflesThread : listenShufflesThreads) {
                listenShufflesThread.join();
            }
            shuffleThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processShuffleReceived(String shuffleReceived) throws IOException {
        String key = shuffleReceived.split(", ")[0];
        String filename = getShuffleFilename(key);
        File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY);
        shuffleReceivedDirectory.mkdirs();
        File shuffleReceivedFile = new File(SHUFFLE_RECEIVED_DIRECTORY + File.separator + filename);
        shuffleReceivedFile.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(shuffleReceivedFile, true));
        writer.write(shuffleReceived + "\n");
        writer.close();
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

    private void launchReduceCount() throws FileNotFoundException, IOException {
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
                writer.write(key + ", " + count + "\n");
            }
        }

        System.out.println("Reduce calculation completed");

    }

    private void launchMapSort() throws IOException {
        // Get all the files in the reduceCount directory
        File reduceDirectory = new File(REDUCE_DIRECTORY);
        File[] reduceFiles = reduceDirectory.listFiles();

        assert reduceFiles != null;

        // Make the map sort directory and put the file in it
        File mapSortDirectory = new File(MAP_SORT_DIRECTORY);
        mapSortDirectory.mkdirs();

        String outputName = "UMS" + machineId + ".txt";
        String outputFilePath = MAP_SORT_DIRECTORY + File.separator + outputName;
        File outputFile = new File(outputFilePath);

        outputFile.createNewFile();

        HashMap<Integer, String> wordSortMap = new HashMap<>();

        for (File file : reduceFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] keyValue = line.split(", ");
                    if (keyValue.length == 2) {
                        String key = keyValue[0];
                        int value = Integer.parseInt(keyValue[1]);
                        if (wordSortMap.containsKey(value)) {
                            String word = wordSortMap.get(value);
                            word += ", " + key;
                            wordSortMap.put(value, word);
                        } else {
                            wordSortMap.put(value, key);
                        }
                    }
                }
            }
        }

        start = Collections.min(wordSortMap.keySet());
        size = Collections.max(wordSortMap.keySet());

        PrintWriter writer = new PrintWriter(outputFile);
        for (Entry<Integer, String> entry : wordSortMap.entrySet()) {
            int key = entry.getKey();
            String value = entry.getValue();
            writer.println(key + ", <" + value + ">");
        }

        writer.close();

    }

    private void launchShuffleSort() throws NumberFormatException, IOException {
        writerMaster.println("ShuffleSort: " + start + ", " + size);

        String line;
        while ((line = readerMaster.readLine()) != null) {
            if (line.startsWith("ShuffleSort: ")) {
                // Get the start and the size
                String[] startPath = line.split(": ")[1].split(", ");
                start = Integer.parseInt(startPath[0]);
                size = Integer.parseInt(startPath[1]);
                break;
            }
        }

    }

    private void sendShufflesSort() throws IOException {
        // Load map sort file
        String mapSortFile = MAP_SORT_DIRECTORY + File.separator + "UMS" + machineId + ".txt";
        File file = new File(mapSortFile);

        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line;
        while ((line = reader.readLine()) != null) {
            String[] keyValue = line.split(", ");
            if (keyValue.length == 2) {
                int key = Integer.parseInt(keyValue[0]);
                int machine = machineSendSort(key, size);
                if (machine != machineId) {
                    writers[machine].println("ShuffleReceivedSort: " + line);
                } else {
                    // Copy the file to the shuffle directory
                    String shuffleFilename = "UMS" + machineId + ".txt";
                    String shuffleFilePath = SHUFFLE_DIRECTORY + File.separator + shuffleFilename;

                    File shuffleFile = new File(shuffleFilePath);
                    shuffleFile.createNewFile();

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(shuffleFile))) {
                        writer.write(line + "\n");
                    }

                }
            }
        }

    }

    private void endOfShuffleSort() throws IOException {
        // TODO
    }

    private void launchReduceSort() throws IOException {
        // TODO
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

    private int machineSendSort(int number, int size) {
        int machineNumber = number / size;
        if (machineNumber < 0) {
            machineNumber += machines.size();
        }
        return machineNumber;
    }
}
