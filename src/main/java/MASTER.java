import java.io.BufferedReader;
import java.io.File;
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
    private static final String REDUCES_DIRECTORY_REMOTE = HOME_DIRECTORY + "/reduces";

    private static final String RESULT_DIRECTORY = "results";

    private static final String MACHINES_FILE = "data/machines.txt";

    private final List<ServerHandler> servers; // Liste des clients connectés
    private List<String> machines = new ArrayList<>();
    private static final int PORT = 8888;

    public static void main(String[] args) {
        MASTER master = new MASTER();
        master.start();
    }

    public MASTER() {
        servers = new ArrayList<>();

        // Lire le fichier "machines.txt"
        getMachines();

        machines.forEach(ipAddress -> {
            System.out.println("Connecting to " + ipAddress);
            while(!isServerSocketOpen(ipAddress, PORT)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                servers.add(new ServerHandler(ipAddress, machines));
                System.out.println("Connected to " + ipAddress);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });


    }


    public void start() {

        System.out.println("Master connected. Starting map phase...");

        // Créer les répertoires sur les machines
        createSplitDirectoryOnMachines(servers);
        waitForCommand(servers);

        System.out.println("Répertoires créés sur les machines.");

        // Lancer la phase de map sur les machines
        runMapPhase(servers);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(servers);

        System.out.println("MAP FINISHED");

        // Lancer la phase de shuffle sur les machines
        runShufflePhase(servers);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(servers);
        System.out.println("SHUFFLE FINISHED");

        // Lancer la phase reduce sur les machines
        runReducePhase(servers);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(servers);
        System.out.println("REDUCE FINISHED");

        /* ****************  RÉCUPÉRATION DE RÉSULTATS *****************

        // Copier les fichiers de reduce vers la machine locale
        List<Process> processesResults = runResultPhase(machines);

        // Attendre que tous les SCP se terminent
        waitForProcesses(processesResults);
        System.out.println("Résultats récupérés.");

        // Merge les résultats
        mergeResults();
        System.out.println("Résultats fusionnés et présents dans result.txt");

        */

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

    private void createSplitDirectoryOnMachines(List<ServerHandler> clients) {
        clients.forEach(client -> client.sendCommand("runCommand mkdir -p " + SPLIT_DIRECTORY_REMOTE));

    }

    private void runMapPhase(List<ServerHandler> clients) {
        clients.forEach(client -> client.sendCommand("launchMap " +
                SPLIT_DIRECTORY_REMOTE + File.separator + "S" + client.getMachineId() + ".txt"));
    }

    private void runShufflePhase(List<ServerHandler> clients) {
        clients.forEach(client -> client.sendCommand("launchShuffle " +
                MAP_DIRECTORY_REMOTE + File.separator + "UM" + client.getMachineId() + ".txt"));

    }

    private void runReducePhase(List<ServerHandler> clients) {
        clients.forEach(client -> client.sendCommand("launchReduce"));
    }

    private List<Process> runResultPhase(List<String> machines) {
        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, REDUCES_DIRECTORY_REMOTE);

            try {
                // Copier le fichier sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("scp", "-r",
                        machineDirectory, RESULT_DIRECTORY + File.separator);
                Process process = pb.start();
                processes.add(process);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private void mergeResults() {
        try {
            // Check if there is a results directory
            File resultsDirectory = new File(RESULT_DIRECTORY);
            if (!resultsDirectory.exists()) {
                System.err.println("Le répertoire " + RESULT_DIRECTORY + " n'existe pas.");
                return;
            }

            // Check if there are files in the results directory
            File[] resultFiles = resultsDirectory.listFiles();

            if (resultFiles == null || resultFiles.length == 0) {
                System.err.println("Le répertoire " + RESULT_DIRECTORY + " est vide.");
                return;
            }

            // Merge the files
            Runtime.getRuntime().exec("cat " + RESULT_DIRECTORY + "/*.txt > " + RESULT_DIRECTORY + "/result.txt");
            System.out.println("Fusion des résultats effectuée.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void waitForProcesses(List<Process> processes) {
        processes.forEach(process -> {
            try {
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new IllegalStateException("Process exited with code " + exitCode);
                }
            } catch (InterruptedException | IllegalStateException e) {
                e.printStackTrace();
            }
        });
    }

    private void waitForCommand(List<ServerHandler> servers) {
        servers.forEach(ServerHandler::waitDone);
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

        public void waitDone() {
            try {
                String clientMessage;
                while ((clientMessage = in.readLine()) != null) {
                    if (clientMessage.equals("done.")) {
                        break;
                    } else if (clientMessage.startsWith("Send: ")) {
                        System.out.println("Message from machine " + machineId + ": " + ipAddress);
                        String machineNumber = clientMessage.split(" ")[1];
                        String messageToSend = in.readLine();

                        // Send the message to the right machine
                        int machineToSend = Integer.parseInt(machineNumber);
                        servers.get(machineToSend).sendCommand("ShuffleReceived: " + messageToSend);

                        System.out.println("Message sent to machine " + machineToSend + ": " + messageToSend);
                    } else {
                        System.err.println("Error on machine " + machineId + ": " + ipAddress);
                    }
                }
                System.out.println("done for machine " + machineId + ": " + ipAddress);
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


    public static boolean isServerSocketOpen(String host, int port) {
        try (Socket ignored = new Socket(host, port)) {
            return true; // La connexion a réussi, le socket serveur est ouvert
        } catch (Exception e) {
            return false; // La connexion a échoué, le socket serveur est fermé ou inaccessible
        }
    }
}
