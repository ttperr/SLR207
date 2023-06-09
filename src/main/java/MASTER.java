import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MASTER {
    public static final String USERNAME = "tperrot-21";

    public static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    public static final String SPLIT_DIRECTORY_REMOTE = HOME_DIRECTORY + "/splits";
    public static final String MAP_COUNT_DIRECTORY_REMOTE = HOME_DIRECTORY + "/mapsCount";

    public static final String TEXT_NAME = "code.txt";

    public static final String RESULT_DIRECTORY = "results";
    public static final String RESULT_FILE = RESULT_DIRECTORY + File.separator + TEXT_NAME;

    public static final String MACHINES_FILE = "data/machines.txt";

    public static final boolean isTest = DEPLOY.isTest;
    public static final boolean sorting = false;


    private final List<ServerHandler> servers = new ArrayList<>(); // Liste des clients connectés
    private List<String> machines = new ArrayList<>();
    private static final int PORT = 8888;

    public static void main(String[] args) {
        MASTER master = new MASTER();
        master.start();
    }

    public MASTER() {
        // Lire le fichier "machines.txt"
        getMachines();

        machines.forEach(ipAddress -> {
            System.out.println("Connecting to " + ipAddress);
            while (!isServerSocketOpen(ipAddress, PORT)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    public void start() {

        System.out.println("Master connected.");

        /* Création de répertoire machine
        // Créer les répertoires sur les machines
        createSplitDirectoryOnMachines();
        waitForCommand();

        System.out.println("Répertoires créés sur les machines.");
        */

        // Calcul du temps
        long startTime = System.currentTimeMillis();
        runConnectEachOther();
        waitForCommand();
        long endTime = System.currentTimeMillis();
        System.out.println("FULL DISTRIBUTED SERVER CREATED");

        System.out.println("Temps de connexion : " + (endTime - startTime) / 1000. + "s");

        // Lancer la phase de map sur les machines
        long startTime2 = System.currentTimeMillis();
        runMapCountPhase();
        // Attendre que tous les SLAVES se terminent
        waitForCommand();
        long endTime2 = System.currentTimeMillis();
        System.out.println("MAP COUNT FINISHED");

        System.out.println("Temps de map count : " + (endTime2 - startTime2) / 1000. + "s");


        // Lancer la phase de shuffle sur les machines
        long startTime3 = System.currentTimeMillis();
        runShuffleCountPhase();
        // Attendre que tous les SLAVES se terminent
        waitForCommand();
        long endTime3 = System.currentTimeMillis();
        System.out.println("SHUFFLE COUNT FINISHED");

        System.out.println("Temps de shuffle count : " + (endTime3 - startTime3) / 1000. + "s");

        // Lancer la phase reduce sur les machines
        long startTime4 = System.currentTimeMillis();
        runReduceCountPhase();
        // Attendre que tous les SLAVES se terminent
        waitForCommand();
        long endTime4 = System.currentTimeMillis();
        System.out.println("REDUCE COUNT FINISHED");

        System.out.println("Temps de reduce count : " + (endTime4 - startTime4) / 1000. + "s");

        if (sorting) {

        // TODO long startTime5 = System.currentTimeMillis();
        // TODO runMapSortPhase();
        // TODO // Attendre que tous les SLAVES se terminent
        // TODO waitForCommand();
        // TODO long endTime5 = System.currentTimeMillis();

        // TODO System.out.println("Temps de map sort : " + (endTime5 - startTime5) / 1000. + "s");

        // TODO // Lancer la phase shuffle sur les machines
        // TODO long startTime6 = System.currentTimeMillis();
        // TODO runShuffleSortPhase();
        // TODO // Attendre que tous les SLAVES se terminent
        // TODO waitForCommand();
        // TODO long endTime6 = System.currentTimeMillis();

        // TODO System.out.println("Temps de shuffle sort : " + (endTime6 - startTime6) / 1000. + "s");

        // TODO // Lancer la phase reduce sur les machines
        // TODO long startTime7 = System.currentTimeMillis();
        // TODO runReduceSortPhase();
        // TODO // Attendre que tous les SLAVES se terminent
        // TODO waitForCommand();
        // TODO long endTime7 = System.currentTimeMillis();

        // TODO System.out.println("Temps de reduce sort : " + (endTime7 - startTime7) / 1000. + "s");
        }

        if (isTest) {
            // Copier les fichiers de reduce vers la machine locale
            runResultPhase();

            // Attendre que tous les SLAVES se terminent
            waitForCommand();

            System.out.println("Résultats fusionnés et présents dans results/" + TEXT_NAME);
        }
        // Ferme tous les clients
        servers.forEach(ServerHandler::close);
        System.out.println("Finished.");

        // Calcul du temps total
        long endTime8 = System.currentTimeMillis();
        System.out.println("Temps total : " + (endTime8 - startTime) / 1000. + "s");

    }

    private void getMachines() {
        try {
            // Lire le fichier "machines.txt"
            Path machinesFilePath = Path.of(MASTER.MACHINES_FILE);
            machines = Files.readAllLines(machinesFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createSplitDirectoryOnMachines() {
        servers.forEach(client -> client.sendCommand("runCommand mkdir -p " + SPLIT_DIRECTORY_REMOTE));
    }

    private void runConnectEachOther() {
        servers.forEach(client -> client.sendCommand("connectEachOther"));
    }

    private void runMapCountPhase() {
        servers.forEach(client -> client.sendCommand("launchMapCount " +
                SPLIT_DIRECTORY_REMOTE + File.separator + "S" + client.getMachineId() + ".txt"));
    }

    private void runShuffleCountPhase() {
        servers.forEach(client -> client.sendCommand("launchShuffleCount"));

    }

    private void runReduceCountPhase() {
        servers.forEach(server -> server.sendCommand("launchReduceCount"));
    }

    private void runMapSortPhase() {
        servers.forEach(server -> server.sendCommand("launchMapSort"));
    }

    private void runShuffleSortPhase() {
        servers.forEach(server -> server.sendCommand("launchShuffleSort"));
    }

    private void runReduceSortPhase() {
        servers.forEach(server -> server.sendCommand("launchReduceSort"));
    }

    private void runResultPhase() {
        servers.forEach(server -> server.sendCommand("launchResult"));
    }

    private void waitForCommand() {
        servers.forEach(ServerHandler::run);
    }

    private class ServerHandler extends Thread {
        private final Socket clientSocket;
        private final BufferedReader in; // receive 0 if the command went well, the error otherwise
        private final PrintWriter out; // send all the commands to the client
        private final int machineId;
        private final String ipAddress;

        public ServerHandler(String ipAddress, List<String> authorizedMachines) throws IOException {
            this.ipAddress = ipAddress;
            this.machineId = authorizedMachines.indexOf(ipAddress);
            if (machineId == -1) {
                throw new IOException("Unauthorized machine: " + ipAddress);
            }
            this.clientSocket = new Socket(ipAddress, PORT);
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        }

        @Override
        public void run() {
            try {
                String clientMessage;
                while ((clientMessage = in.readLine()) != null) {
                    if (clientMessage.equals("DONE.")) {
                        break;
                    }
                    /*
                    else if (clientMessage.startsWith("Send: ")) {
                        System.out.println("Message from machine " + machineId + ": " + ipAddress);
                        String machineNumber = clientMessage.split(" ")[1];
                        String messageToSend = in.readLine();

                        // Send the message to the right machine
                        int machineToSend = Integer.parseInt(machineNumber);
                        servers.get(machineToSend).sendCommand("ShuffleReceivedCount: " + messageToSend);

                        System.out.println("Message sent to machine " + machineToSend + ": " + messageToSend);
                    }
                     */
                    else if (clientMessage.startsWith("Results: ")) {
                        String results = clientMessage.substring("Results: ".length());

                        // Create the result directory if it doesn't exist
                        File resultDirectory = new File(RESULT_DIRECTORY);
                        if (!resultDirectory.exists()) {
                            resultDirectory.mkdir();
                        }

                        // Write the results to the file without deleting the previous results and by adding it compared to the previous occurrence = split(", ")[1] to get sorted results
                        FileWriter fileWriter = new FileWriter(RESULT_FILE, true);
                        PrintWriter printWriter = new PrintWriter(fileWriter);
                        printWriter.println(results);
                        fileWriter.close();
                    } else {
                        System.err.println("Error on machine " + machineId + ": " + ipAddress);
                    }
                }
                System.out.println("Done for machine " + machineId + ": " + ipAddress);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public int getMachineId() {
            return machineId;
        }

        // Send a command to the client
        public void sendCommand(String command) {
            out.println(command);
        }

        public void close() {
            try {
                sendCommand("QUIT");
                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public boolean isServerSocketOpen(String host, int port) {
        try {
            ServerHandler server = new ServerHandler(host, machines);

            servers.add(server);
            System.out.println("Connected to " + host + ":" + port);
            return true; // La connexion a réussi, le socket serveur est ouvert
        } catch (Exception e) {
            return false; // La connexion a échoué, le socket serveur est fermé ou inaccessible
        }
    }
}
