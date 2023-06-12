package serv_v1;

import java.io.*;
import java.net.ServerSocket;
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

    private static final String SPLIT_DIRECTORY = "data/splits";
    private static final String RESULT_DIRECTORY = "results";

    private static final String SLAVE = "SLAVE";

    private static final String MACHINES_FILE = "data/machines.txt";

    private static final String SERVER = "tp-3a101-00.enst.fr";
    private static final int PORT = 8888;
    private final List<ClientHandler> clients; // Liste des clients connectés
    private List<String> machines = new ArrayList<>();
    private ServerSocket serverSocket;

    public MASTER() {
        clients = new ArrayList<>();

        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MASTER master = new MASTER();
        master.start();
    }

    public void start() {
        // Lire le fichier "machines.txt"
        getMachines();

        // Copier le fichier machines.txt vers les machines
        List<Process> processesCopyMachines = copyMachinesFile(machines);

        // Attendre que toutes les copies se terminent
        waitForProcesses(processesCopyMachines);
        System.out.println("Copie du fichier machines.txt effectuée.");

        System.out.println("Waiting for clients...");

        // Dire aux clients de se connecter
        connectToMachines(machines);

        while (clients.size() < machines.size()) {
            try {
                Socket clientSocket = serverSocket.accept();

                System.out.println("Client connected: " + clientSocket.getInetAddress().getHostName());

                // Crée un thread pour gérer la communication avec le client
                ClientHandler clientHandler = new ClientHandler(clientSocket, machines);
                clients.add(clientHandler);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("All clients connected. Starting map phase...");

        // Créer les répertoires sur les machines
        createSplitDirectoryOnMachines(clients);
        waitForCommand(clients);

        System.out.println("Répertoires créés sur les machines.");

        // Copier les fichiers de data.splits vers les machines
        List<Process> processesSplit = copySplitsToMachines(machines);

        // Attendre que tous les SCP se terminent
        assert processesSplit != null;
        waitForProcesses(processesSplit);
        System.out.println("Copie des data.splits sur les machines effectuée.");

        // Lancer la phase de map sur les machines
        runMapPhase(clients);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(clients);

        System.out.println("MAP FINISHED");

        // Lancer la phase de shuffle sur les machines
        runShufflePhase(clients);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(clients);
        System.out.println("SHUFFLE FINISHED");

        // Lancer la phase reduce sur les machines
        runReducePhase(clients);

        // Attendre que tous les SLAVES se terminent
        waitForCommand(clients);
        System.out.println("REDUCE FINISHED");

        /*****************  RÉCUPÉRATION DE RÉSULTATS *****************
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
        clients.forEach(ClientHandler::close);
        System.out.println("Finished.");

    }

    private void connectToMachines(List<String> machines) {
        machines.forEach(ipAddress -> {
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Construire la commande SSH pour supprimer le répertoire distant
                String command = String.format("ssh %s java -jar %s %s %s %s", machine, HOME_DIRECTORY + File.separator + SLAVE + ".jar", 9, SERVER, PORT);

                // Exécuter la commande à distance
                Runtime.getRuntime().exec(command);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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

    private void createSplitDirectoryOnMachines(List<ClientHandler> clients) {
        clients.forEach(client -> client.sendCommand("mkdir -p " + SPLIT_DIRECTORY_REMOTE));

    }

    private List<Process> copySplitsToMachines(List<String> machines) {
        // Check if there is a data.splits directory
        File splitsDirectory = new File(SPLIT_DIRECTORY);
        if (!splitsDirectory.exists()) {
            System.err.println("Le répertoire " + SPLIT_DIRECTORY + " n'existe pas.");
            return null;
        }

        // Check if there are files in the data.splits directory
        File[] splitFiles = splitsDirectory.listFiles();

        if (splitFiles == null || splitFiles.length == 0) {
            System.err.println("Le répertoire " + SPLIT_DIRECTORY + " est vide.");
            return null;
        }

        // Check if there is the same number of files than the number of machines
        if (splitFiles.length != machines.size()) {
            System.err.println("Le nombre de fichiers dans le répertoire " + SPLIT_DIRECTORY
                    + " n'est pas égal au nombre de machines.");
            return null;
        }

        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SPLIT_DIRECTORY_REMOTE);

            try {
                // Copier le fichier sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("scp",
                        SPLIT_DIRECTORY + File.separator + "S" + machineNumber + ".txt", machineDirectory);
                Process process = pb.start();
                processes.add(process);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private List<Process> copyMachinesFile(List<String> machines) {
        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, HOME_DIRECTORY);

            try {
                // Copier le fichier machines.txt sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("scp",
                        MACHINES_FILE, machineDirectory);
                Process process = pb.start();
                processes.add(process);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private void runMapPhase(List<ClientHandler> clients) {
        clients.forEach(client -> client.sendCommand("java " + "-jar " +
                HOME_DIRECTORY + File.separator + SLAVE + ".jar " + "0 " +
                SPLIT_DIRECTORY_REMOTE + File.separator + "S" + client.getMachineId() + ".txt"));
    }

    private void runShufflePhase(List<ClientHandler> clients) {
        clients.forEach(client -> client.sendCommand("java " + "-jar " +
                HOME_DIRECTORY + File.separator + SLAVE + ".jar " + "1 " +
                MAP_DIRECTORY_REMOTE + File.separator + "UM" + client.getMachineId() + ".txt"));

    }

    private void runReducePhase(List<ClientHandler> clients) {
        clients.forEach(client -> client.sendCommand("java " + "-jar " +
                HOME_DIRECTORY + File.separator + SLAVE + ".jar " + "2"));
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

    private void waitForCommand(List<ClientHandler> clients) {
        clients.forEach(Thread::start);
    }

    private static class ClientHandler extends Thread {
        private final Socket clientSocket;
        private final BufferedReader in; // receive 0 if the command went well, the error otherwise
        private final PrintWriter out; // send all the commands to the client
        private final int machineId;
        private final String ipAddress;

        public ClientHandler(Socket socket, List<String> authorizedMachines) throws IOException {
            this.clientSocket = socket;
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
            this.ipAddress = socket.getInetAddress().getHostName();
            this.machineId = authorizedMachines.indexOf(ipAddress);
            if (machineId == -1) {
                throw new IOException("Unauthorized machine: " + ipAddress);
            }
        }

        @Override
        public void run() {
            try {
                String clientMessage;
                while ((clientMessage = in.readLine()) != null) {
                    if (clientMessage.equals("0")) {
                        break;
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
                sendCommand("quit");
                in.close();
                out.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
