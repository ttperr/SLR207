package stepByStep.step10;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MASTER {
    private static final String USERNAME = "tperrot-21";
    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String SPLIT_DIRECTORY_REMOTE = HOME_DIRECTORY + "/splits";
    private static final String SPLIT_DIRECTORY = "Step10/data.splits";
    private static final String SLAVE_CLASS_NAME = "Step10.SLAVE";
    private static final String MACHINES_FILE = "machines.txt";

    public static void main(String[] args) {
        try {
            // Lire le fichier "machines.txt"
            Path machinesFilePath = Path.of(MACHINES_FILE);
            List<String> machines = Files.readAllLines(machinesFilePath);

            // Créer les répertoires sur les machines
            createSplitDirectoryOnMachines(machines);

            // Copier les fichiers de data.splits vers les machines
            List<Process> processesSplit = copySplitsToMachines(machines);

            // Attendre que tous les SCP se terminent
            assert processesSplit != null;
            waitForProcesses(processesSplit);
            System.out.println("Copie des data.splits sur les machines effectuée.");

            // Copier le fichier machines.txt vers les machines
            List<Process> processesCopyMachines = copyMachinesFile(machines);

            // Attendre que toutes les copies se terminent
            waitForProcesses(processesCopyMachines);
            System.out.println("Copie du fichier machines.txt effectuée.");

            // Lancer la phase de map sur les machines
            List<Process> processesMap = runMapPhase(machines);

            // Attendre que tous les SLAVES se terminent
            waitForProcesses(processesMap);

            System.out.println("MAP FINISHED");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createSplitDirectoryOnMachines(List<String> machines) {
        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Créer le répertoire sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("serv_ssh", machine, "mkdir", "-p", SPLIT_DIRECTORY_REMOTE);
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

    private static List<Process> copySplitsToMachines(List<String> machines) {
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

    private static List<Process> copyMachinesFile(List<String> machines) {
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

    private static List<Process> runMapPhase(List<String> machines) {
        List<Process> processes = new ArrayList<>();

        machines.forEach(ipAddress -> {
            int machineNumber = machines.indexOf(ipAddress);
            String machine = String.format("%s@%s", USERNAME, ipAddress);

            try {
                // Lancer le SLAVE avec les arguments appropriés
                ProcessBuilder pb = new ProcessBuilder("serv_ssh", machine, "java", "-cp",
                        HOME_DIRECTORY + File.separator + "SLAVE.jar", SLAVE_CLASS_NAME, "0",
                        SPLIT_DIRECTORY_REMOTE + File.separator + "S" + machineNumber + ".txt");
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
