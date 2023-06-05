package Step8;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CLEAN {
    private static final String USERNAME = "tperrot-21";
    private static final String REMOTE_DIR = "/tmp/" + USERNAME + "/";
    private static final String COMPUTERS_FILE = "computers.json";

    public static void main(String[] args) {
        try {
            // Lire le fichier "computers.json"
            String computersJson = new String(Files.readAllBytes(Path.of(COMPUTERS_FILE)));
            JSONObject computers = new JSONObject(computersJson);

            // Parcourir les machines et supprimer le répertoire distant
            computers.keySet().forEach(machineNumber -> {
                String ipAddress = computers.getString(machineNumber);
                String machine = String.format("%s@%s", USERNAME, ipAddress);

                try {
                    // Construire la commande SSH pour supprimer le répertoire distant
                    String command = String.format("ssh %s rm -rf %s", machine, REMOTE_DIR);

                    // Exécuter la commande à distance
                    Process process = Runtime.getRuntime().exec(command);

                    // Attendre la fin de la commande
                    int exitCode = process.waitFor();

                    if (exitCode == 0) {
                        System.out.println("Répertoire supprimé sur la machine " + machineNumber + ": " + ipAddress);
                    } else {
                        System.err.println("Erreur lors de la suppression du répertoire sur la machine " + machineNumber
                                + ": " + ipAddress);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
