package serv_v0;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class DEPLOY {
    private static final String USERNAME = "tperrot-21";

    private static final String REMOTE_DIR = "/tmp/" + USERNAME + "/";

    private static final String SRC_DIR = "serv_v0";

    private static final String SLAVE = "SLAVE";
    private static final String SLAVE_JAR = "SLAVE.jar";

    private static final String MACHINES_FILE = "data/machines.txt";

    public static void main(String[] args) {
        try {
            // Lire le fichier "machines.txt"
            Path machinesFilePath = Path.of(MACHINES_FILE);
            List<String> machines = Files.readAllLines(machinesFilePath);

            // Création du fichier SLAVE.jar à partir de SLAVE.java et suppression de
            // SLAVE.class directement
            ProcessBuilder javacPb = new ProcessBuilder("javac", SLAVE + ".java");
            Path srcDirPath = Path.of(SRC_DIR);
            javacPb.directory(srcDirPath.toFile());
            Process javacProcess = javacPb.start();
            int javacExitCode = javacProcess.waitFor();

            if (javacExitCode == 0) {
                // La compilation s'est terminée avec succès
                System.out.println("Compilation terminée avec succès");

                // Créer le fichier JAR
                ProcessBuilder jarPb = new ProcessBuilder("jar", "cvfe", ".." + File.separator + SLAVE_JAR, SLAVE,
                        SLAVE + ".class");
                jarPb.directory(srcDirPath.toFile());
                Process jarProcess = jarPb.start();
                int jarExitCode = jarProcess.waitFor();

                if (jarExitCode == 0) {
                    // Le fichier JAR a été créé avec succès
                    System.out.println("Fichier JAR créé avec succès");

                    // Supprimer le fichier SLAVE.class
                    ProcessBuilder rmPb = new ProcessBuilder("rm", SLAVE + ".class");
                    rmPb.directory(srcDirPath.toFile());
                    Process rmProcess = rmPb.start();
                    int rmExitCode = rmProcess.waitFor();

                    if (rmExitCode == 0) {
                        // Le fichier SLAVE.class a été supprimé avec succès
                        System.out.println("Fichier SLAVE.class supprimé avec succès");

                        System.out.println("\n\nDéploiement sur les machines...\n");
                    } else {
                        // Une erreur s'est produite lors de la suppression du fichier SLAVE.class
                        System.err.println("Erreur lors de la suppression du fichier SLAVE.class");
                        return;
                    }
                } else {
                    // Une erreur s'est produite lors de la création du fichier JAR
                    System.err.println("Erreur lors de la création du fichier JAR");
                    return;
                }
            } else {
                // Une erreur s'est produite lors de la compilation
                System.err.println("Erreur lors de la compilation");
                return;
            }

            // Tester la connexion SSH sur chaque machine et copier le fichier "slave.jar"
            // si la connexion réussit
            machines.forEach(ipAddress -> {
                int machineNumber = machines.indexOf(ipAddress);
                String machine = String.format("%s@%s", USERNAME, ipAddress);

                try {
                    // Vérifier la connexion SSH en exécutant la commande "hostname" à distance
                    ProcessBuilder sshPb = new ProcessBuilder("ssh", machine, "hostname");
                    Process sshProcess = sshPb.start();
                    int sshExitCode = sshProcess.waitFor();

                    if (sshExitCode == 0) {
                        // La connexion SSH a réussi.
                        System.out
                                .println("\nConnexion SSH réussie sur la machine " + machineNumber + ": " + ipAddress);

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
                                System.err.println("Erreur lors de la copie du fichier sur la machine " + machineNumber
                                        + ": " + ipAddress);
                                return;
                            }
                        } else {
                            // Une erreur s'est produite lors de la création du répertoire
                            System.err.println("Erreur lors de la création du répertoire sur la machine "
                                    + machineNumber + ": " + ipAddress);
                            return;
                        }
                    } else {
                        // La connexion SSH a échoué.
                        System.err.println(
                                "Échec de la connexion SSH sur la machine " + machineNumber + ": " + ipAddress);
                        return;
                    }

                    if (machineNumber != 0 && machineNumber % 5 == 0) {
                        Thread.sleep(60000);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
