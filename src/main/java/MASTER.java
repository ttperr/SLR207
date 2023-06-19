import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MASTER {
    private static final String USERNAME = "tperrot-21";

    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String SPLIT_DIRECTORY_REMOTE = HOME_DIRECTORY + "/splits";
    private static final String MAP_DIRECTORY_REMOTE = HOME_DIRECTORY + "/maps";

    private static final String RESULT_DIRECTORY = "results";

    private static final String MACHINES_FILE = "data/machines.txt";

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

        System.out.println("Master connected. Starting map phase...");

        /* Création de répertoire machine
        // Créer les répertoires sur les machines
        createSplitDirectoryOnMachines();
        waitForCommand();

        System.out.println("Répertoires créés sur les machines.");
        */

        runConnectEachOther();
        waitForCommand();

        // Lancer la phase de map sur les machines
        runMapPhase();

        // Attendre que tous les SLAVES se terminent
        waitForCommand();

        System.out.println("MAP FINISHED");

        // Lancer la phase de shuffle sur les machines
        runShufflePhase();

        // Attendre que tous les SLAVES se terminent
        waitForCommand();
        System.out.println("SHUFFLE FINISHED");

        // Lancer la phase reduce sur les machines
        runReducePhase();

        // Attendre que tous les SLAVES se terminent
        waitForCommand();
        System.out.println("REDUCE FINISHED");

        // Copier les fichiers de reduce vers la machine locale
        runResultPhase();

        // Attendre que tous les SLAVES se terminent
        waitForCommand();

        System.out.println("Résultats fusionnés et présents dans results/result.txt");

        // Ferme tous les clients
        servers.forEach(ServerHandler::close);
        System.out.println("Finished.");

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

    private void runMapPhase() {
        servers.forEach(client -> client.sendCommand("launchMap " +
                SPLIT_DIRECTORY_REMOTE + File.separator + "S" + client.getMachineId() + ".txt"));
    }

    private void runShufflePhase() {
        servers.forEach(client -> client.sendCommand("launchShuffle " +
                MAP_DIRECTORY_REMOTE + File.separator + "UM" + client.getMachineId() + ".txt"));

    }

    private void runReducePhase() {
        servers.forEach(server -> server.sendCommand("launchReduce"));
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
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        }

        @Override
        public void run() {
            try {
                String clientMessage;
                while ((clientMessage = in.readLine()) != null) {
                    if (clientMessage.equals("DONE.")) {
                        break;
                    } else if (clientMessage.startsWith("Send: ")) {
                        System.out.println("Message from machine " + machineId + ": " + ipAddress);
                        String machineNumber = clientMessage.split(" ")[1];
                        String messageToSend = in.readLine();

                        // Send the message to the right machine
                        int machineToSend = Integer.parseInt(machineNumber);
                        servers.get(machineToSend).sendCommand("ShuffleReceived: " + messageToSend);

                        System.out.println("Message sent to machine " + machineToSend + ": " + messageToSend);
                    } else if (clientMessage.startsWith("Results: ")) {
                        String results = clientMessage.substring("Results: ".length());

                        // Create the result directory if it doesn't exist
                        File resultDirectory = new File(RESULT_DIRECTORY);
                        if (!resultDirectory.exists()) {
                            resultDirectory.mkdir();
                        }

                        // Write the results to the file without deleting the previous results
                        FileWriter fileWriter = new FileWriter(RESULT_DIRECTORY + File.separator + "result.txt", true);
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
