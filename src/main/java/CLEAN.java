import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CLEAN {
    private static final String USERNAME = "tperrot-21";

    private static final String REMOTE_DIR = "/tmp/" + USERNAME + "/";

    private static final String MACHINES_FILE = "data/machines.txt";

    private static final String RESULT_DIRECTORY = "results";

    public static void main(String[] args) {
        new CLEAN();
    }

    public CLEAN() {
        try {
            // Lire le fichier "machines.txt"
            Path machinesFilePath = Path.of(MACHINES_FILE);
            List<String> machines = Files.readAllLines(machinesFilePath);

            // Parcourir les machines et supprimer le répertoire distant
            machines.forEach(ipAddress -> {
                int machineNumber = machines.indexOf(ipAddress);
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
                        showErrorMessage("Erreur lors de la suppression du répertoire sur la machine " + machineNumber + ": " + ipAddress, process);
                    }

                    if (machineNumber != 0 && machineNumber % 5 == 0) {
                        Thread.sleep(60000);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });

            // Suppression du dossier results
            Runtime.getRuntime().exec("rm -rf " + RESULT_DIRECTORY);

            // Suppression du dossier tmp
            Runtime.getRuntime().exec("rm -rf tmp");

            System.out.println("\nNettoyage terminé");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void showErrorMessage(String message, Process process) throws IOException {
        System.err.println("-".repeat(80));

        System.err.println(message);
        // Print error stream
        process.getErrorStream().transferTo(System.err);

        System.err.println("-".repeat(80));
        System.exit(1);
    }

}
