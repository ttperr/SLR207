package Step10;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MASTER {
    private static final String USERNAME = "tperrot-21";
    private static final String SPLIT_DIRECTORY_REMOTE = "/tmp/" + USERNAME + "/splits";
    private static final String SPLIT_DIRECTORY = "Step10/splits";
    private static final String COMPUTERS_FILE = "computers.json";

    public static void main(String[] args) {
        try {
            // Lire le fichier "computers.json"
            String computersJson = new String(Files.readAllBytes(Path.of(COMPUTERS_FILE)));
            JSONObject computers = new JSONObject(computersJson);

            // Créer les répertoires sur les machines
            createSplitDirectoryOnMachines(computers);

            // Copier les fichiers de splits vers les machines
            List<Process> processesSplit = copySplitsToMachines(computers);

            // Attendre que tous les SCP se terminent
            assert processesSplit != null;
            waitForProcesses(processesSplit);
            System.out.println("Copie des splits sur les machines effectué.");

            // Lancer la phase de map sur les machines
            List<Process> processesMap = runMapPhase(computers);

            // Attendre que tous les SLAVES se terminent
            waitForProcesses(processesMap);

            System.out.println("MAP FINISHED");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createSplitDirectoryOnMachines(JSONObject computers) {
        computers.keySet().forEach(machineNumber -> {
            String ipAddress = computers.getString(machineNumber);
            
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Créer le répertoire sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", SPLIT_DIRECTORY_REMOTE);
                Process process = pb.start();
                int exitCode = process.waitFor();

                if (exitCode == 0) {
                    System.out.println("Répertoire créé sur la machine " + machineNumber + ": " + ipAddress);
                } else {
                    System.err.println("Erreur lors de la création du répertoire sur la machine " + machineNumber + ": " + ipAddress);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    private static List<Process> copySplitsToMachines(JSONObject computers) {
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
        if (splitFiles.length != computers.length()) {
            System.err.println("Le nombre de fichiers dans le répertoire " + SPLIT_DIRECTORY + " n'est pas égal au nombre de machines.");
            return null;
        }

        List<Process> processes = new ArrayList<Process>();

        computers.keySet().forEach(machineNumber -> {
            String ipAddress = computers.getString(machineNumber);
            String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SPLIT_DIRECTORY_REMOTE);

            try {
                // Copier le fichier sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("scp", splitFiles[Integer.parseInt(machineNumber)].getAbsolutePath(), machineDirectory);
                Process process = pb.start();
                processes.add(process);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private static List<Process> runMapPhase(JSONObject computers) {
        List<Process> processes = new ArrayList<>();

        computers.keySet().forEach(machineNumber -> {
            String ipAddress = computers.getString(machineNumber);
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Lancer le SLAVE avec les arguments appropriés
                ProcessBuilder pb = new ProcessBuilder("ssh", machine, "java", "-jar", "/tmp/" + USERNAME + "/slave.jar", "0", "S" + machineNumber + ".txt");
                Process process = pb.start();
                processes.add(process);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return processes;
    }

    private static void waitForProcesses(List<Process> processes) {
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
}
