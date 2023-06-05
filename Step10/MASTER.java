    package Step10;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
            copySplitsToMachines(computers);

            System.out.println("Copie des fichiers terminée.");
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

    private static void copySplitsToMachines(JSONObject computers) {
        // Check if there is a splits directory
        File splitsDirectory = new File(SPLIT_DIRECTORY);
        if (!splitsDirectory.exists()) {
            System.err.println("Le répertoire " + SPLIT_DIRECTORY + " n'existe pas.");
            return;
        }

        // Check if there are files in the splits directory
        File[] splitFiles = splitsDirectory.listFiles();

        if (splitFiles == null || splitFiles.length == 0) {
            System.err.println("Le répertoire " + SPLIT_DIRECTORY + " est vide.");
            return;
        }

        // Check if there is the same number of files than the number of machines
        if (splitFiles.length != computers.length()) {
            System.err.println("Le nombre de fichiers dans le répertoire " + SPLIT_DIRECTORY + " n'est pas égal au nombre de machines.");
            return;
        }

        computers.keySet().forEach(machineNumber -> {
            String ipAddress = computers.getString(machineNumber);
            String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SPLIT_DIRECTORY_REMOTE);

            try {
                // Copier le fichier sur la machine distante
                ProcessBuilder pb = new ProcessBuilder("scp", splitFiles[Integer.parseInt(machineNumber) - 1].getAbsolutePath(), machineDirectory);
                Process process = pb.start();
                int exitCode = process.waitFor();

                if (exitCode == 0) {
                    System.out.println("Fichier copié sur la machine " + machineNumber + ": " + ipAddress);
                } else {
                    System.err.println("Erreur lors de la copie du fichier sur la machine " + machineNumber + ": " + ipAddress);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}