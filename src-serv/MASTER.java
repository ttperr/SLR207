import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

    private static final String SPLIT_DIRECTORY = "splits";

    private static final String SLAVE = "SLAVE";

    private static final String MACHINES_FILE = "machines.txt";

    private static final int PORT = 8080;

    private List<String> machines = new ArrayList<String>();// contiendra tous les flux de sortie vers les clients

    private ServerSocket serverSocket;
    private List<Socket> clients;

    public MASTER() {
        clients = new ArrayList<>();

        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        // Lire le fichier "machines.txt"
        getMachines(MACHINES_FILE);

        System.out.println("Master controller started. Waiting for clients...");

        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                clients.add(clientSocket);
                
                // Crée un thread pour gérer la communication avec le client
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                clientHandler.start();
                
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Créer les répertoires sur les machines
        createSplitDirectoryOnMachines(machines);

        // Copier les fichiers de splits vers les machines
        List<Process> processesSplit = copySplitsToMachines(machines);

        // Attendre que tous les SCP se terminent
        assert processesSplit != null;
        waitForProcesses(processesSplit);
        System.out.println("Copie des splits sur les machines effectuée.");

        // Copier le fichier machines.txt vers les machines
        List<Process> processesCopyMachines = copyMachinesFile(machines);

        // Attendre que toutes les copies se terminent
        assert processesCopyMachines != null;
        waitForProcesses(processesCopyMachines);
        System.out.println("Copie du fichier machines.txt effectuée.");

        // Lancer la phase de map sur les machines
        List<Process> processesMap = runMapPhase(machines);

        // Attendre que tous les SLAVES se terminent
        waitForProcesses(processesMap);

        System.out.println("MAP FINISHED");

        // Lancer la phase de shuffle sur les machines
        List<Process> processesShuffle = runShufflePhase(machines);

        // Attendre que tous les SLAVES se terminent
        waitForProcesses(processesShuffle);

        System.out.println("SHUFFLE FINISHED");

        
    }

    private void getMachines(String machinesFile) {
        try {
            // Lire le fichier "machines.txt"
            Path machinesFilePath = Path.of(machinesFile);
            machines = Files.readAllLines(machinesFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createSplitDirectoryOnMachines(List<String> machines) {
        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Créer le répertoire sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", SPLIT_DIRECTORY_REMOTE);
                Process process = pb.start();
                int exitCode = process.waitFor();

                if (exitCode == 0) {
                    System.out.println("Répertoire créé sur la machine " + machineNumber + ": " + ipAddress);
                } else {
                    System.err.println("Erreur lors de la création du répertoire sur la machine " + machineNumber + ": "
                            + ipAddress);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private List<Process> copySplitsToMachines(List<String> machines) {
        // Check if there is a splits directory
        File splitsDirectory = new File(SPLIT_DIRECTORY);
        if (!splitsDirectory.exists()) {
            System.err.println("Le répertoire " + SPLIT_DIRECTORY + " n'existe pas.");
            return null;
        }

        // Check if there are files in the splits directory
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

    private List<Process> runMapPhase(List<String> machines) {
        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machine = String.format("%s@%s", USERNAME, ipAddress);
            System.out.println("Lancement du MAP sur la machine " + machineNumber + ": " + ipAddress);

            try {
                // Lancer le SLAVE avec les arguments appropriés
                ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar",
                        HOME_DIRECTORY + File.separator + SLAVE + ".jar", "0",
                        SPLIT_DIRECTORY_REMOTE + File.separator + "S" + machineNumber + ".txt");
                Process process = pb.start();
                processes.add(process);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private List<Process> runShufflePhase(List<String> machines) {
        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machine = String.format("%s@%s", USERNAME, ipAddress);
            System.out.println("Lancement du SHUFFLE sur la machine " + machineNumber + ": " + ipAddress);

            try {
                // Lancer le SLAVE avec les arguments appropriés
                ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar",
                        HOME_DIRECTORY + File.separator + SLAVE + ".jar", "1",
                        MAP_DIRECTORY_REMOTE + File.separator + "UM" + machineNumber + ".txt");
                Process process = pb.start();
                processes.add(process);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
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


    private class ClientHandler extends Thread {
        private final Socket clientSocket;
        private final BufferedReader in;
        private final PrintWriter out;

        public ClientHandler(Socket socket) throws IOException {
            this.clientSocket = socket;
            this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            this.out = new PrintWriter(clientSocket.getOutputStream(), true);
        }

        @Override
        public void run() {
            try {
                String clientMessage;
                while ((clientMessage = in.readLine()) != null) {
                    System.out.println("Received: " + clientMessage);
                    out.println(clientMessage);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        MASTER master = new MASTER();
        master.start();
    }
}