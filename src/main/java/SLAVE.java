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
    private final Socket[] socketsOutput;
    private final Socket[] socketsInput;
    private final PrintWriter[] writers;
    private final BufferedReader[] readers;
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
        socketsOutput = new Socket[machines.size()];
        socketsInput = new Socket[machines.size()];
        writers = new PrintWriter[machines.size()];
        readers = new BufferedReader[machines.size()];

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
                    for (Socket socket : socketsOutput) {
                        if (socket != null) {
                            System.out.println(socket);
                            System.out.println("Socket is closed ? " + socket.isClosed());
                            System.out.println("Socket is connected ? " + socket.isConnected());
                            System.out.println("Socket is bound ? " + socket.isBound());
                            System.out.println("Socket is input shutdown ? " + socket.isInputShutdown());
                            System.out.println("Socket is output shutdown ? " + socket.isOutputShutdown());
                        }
                    }
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
                        System.out.println("from demand, Connected to " + machines.get(machineId));
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
                    System.out.println("from accept, Connected to " + machines.get(machineId));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        connectThread.start();
        acceptThread.start();

        try {
            connectThread.join();
            System.out.println("Connect thread finished.");
            acceptThread.join();
            System.out.println("Accept thread finished.");
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

    private synchronized void sendShuffles() {
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
                    File shuffleReceveidFile = new File(SHUFFLE_RECEIVED_DIRECTORY + File.separator + shuffleFile.getName());
                    // create shuffle received file
                    shuffleReceveidFile.createNewFile();
                    // read all lines of shuffle file
                    BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                    String line = reader.readLine();
                    reader.close();
                    // append the line to the shuffle received file
                    BufferedWriter writer = new BufferedWriter(new FileWriter(shuffleReceveidFile, true));
                    writer.write(line);
                    writer.newLine();
                    writer.close();
                } else {
                    // Read the line of the file
                    BufferedReader reader = new BufferedReader(new FileReader(shuffleFile));
                    String line = reader.readLine();
                    reader.close();

                    //writerMaster.println("Send: " + machineNumber);
                    //writerMaster.println(line);
                    writers[machineNumber].println("ShuffleReceived: " + line);

                    System.out.println("Message sent to machine " + machineNumber + ": " + line);
                }
            }
            for (PrintWriter writer : writers) {
                if (writer != null) {
                    writer.println("DONE.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listenShufflesMessages(int machineNumber) throws IOException {
        String line;
        while (true) {
            if ((line = readers[machineNumber].readLine()) != null) {
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

    private void endOfShuffle() {
        Thread shuffleThread = new Thread(this::sendShuffles);

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

    private synchronized void processShuffleReceived(String shuffleReceived) {
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
