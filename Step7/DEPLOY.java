package Step7;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DEPLOY {
    private static final String USERNAME = "tperrot-21";
    private static final String REMOTE_DIR = "/tmp/" + USERNAME + "/";
    private static final String COMPUTERS_FILE = "computers.json";
    private static final String SLAVE_JAR = "SLAVE.jar";

    public static void main(String[] args) {
        try {
            // Lire le fichier "computers.json"
            String computersJson = new String(Files.readAllBytes(Path.of(COMPUTERS_FILE)));
            JSONObject computers = new JSONObject(computersJson);

            // Tester la connexion SSH sur chaque machine et copier le fichier "slave.jar" si la connexion réussit
            computers.keySet().forEach(machineNumber -> {
                String ipAddress = computers.getString(machineNumber);
                String machine = String.format("%s@%s", USERNAME, ipAddress);

                try {
                    // Vérifier la connexion SSH en exécutant la commande "hostname" à distance
                    ProcessBuilder sshPb = new ProcessBuilder("ssh", machine, "hostname");
                    Process sshProcess = sshPb.start();
                    int sshExitCode = sshProcess.waitFor();

                    if (sshExitCode == 0) {
                        // La connexion SSH a réussi
                        System.out.println("Connexion SSH réussie sur la machine " + machineNumber + ": " + ipAddress);

                        // Créer le répertoire dans /tmp s'il n'existe pas déjà
                        ProcessBuilder mkdirPb = new ProcessBuilder("ssh", machine, "mkdir", "-p", REMOTE_DIR);
                        Process mkdirProcess = mkdirPb.start();
                        int mkdirExitCode = mkdirProcess.waitFor();

                        if (mkdirExitCode == 0) {
                            // Le répertoire a été créé avec succès
                            System.out.println("Répertoire créé sur la machine " + machineNumber + ": " + ipAddress);

                            // Copier le fichier "slave.jar" dans le répertoire distant
                            ProcessBuilder scpPb = new ProcessBuilder("scp", SLAVE_JAR, machine + ":" + REMOTE_DIR);
                            Process scpProcess = scpPb.start();
                            int scpExitCode = scpProcess.waitFor();

                            if (scpExitCode == 0) {
                                // La copie du fichier s'est terminée avec succès
                                System.out.println("Fichier copié sur la machine " + machineNumber + ": " + ipAddress);
                            } else {
                                // Une erreur s'est produite lors de la copie du fichier
                                System.err.println("Erreur lors de la copie du fichier sur la machine " + machineNumber + ": " + ipAddress);
                            }
                        } else {
                            // Une erreur s'est produite lors de la création du répertoire
                            System.err.println("Erreur lors de la création du répertoire sur la machine " + machineNumber + ": " + ipAddress);
                        }
                    } else {
                        // La connexion SSH a échoué
                        System.err.println("Échec de la connexion SSH sur la machine " + machineNumber + ": " + ipAddress);
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
