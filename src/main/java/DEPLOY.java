import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class DEPLOY {
    private static final String USERNAME = "tperrot-21";

    private static final String REMOTE_DIR = "/tmp/" + USERNAME;

    private static final String PROJECT_DIR = "src/main";
    private static final String SRC_DIR = PROJECT_DIR + "/java";

    private static final String SLAVE = "SLAVE";
    private static final String SLAVE_JAR = "SLAVE.jar";

    private static final String MACHINES_FILE = "data/machines.txt";

    public static void main(String[] args) {
        new DEPLOY();
    }

    public DEPLOY() {
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
                ProcessBuilder jarPb = new ProcessBuilder("jar", "cvfe", ".." + File.separator + SLAVE_JAR, SLAVE, SLAVE + ".class");
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

                        System.out.println("\nDéploiement sur les machines...");
                    } else {
                        showErrorMessage("Erreur lors de la suppression du fichier SLAVE.class", rmProcess);
                    }
                } else {
                    showErrorMessage("Erreur lors de la création du fichier JAR", jarProcess);
                }
            } else {
                showErrorMessage("Erreur lors de la compilation", javacProcess);
            }

            // Création du répertoire à envoyer
            ProcessBuilder mkdirPb = new ProcessBuilder("mkdir", "-p", "." + REMOTE_DIR);
            Process mkdirProcess = mkdirPb.start();
            int mkdirExitCode = mkdirProcess.waitFor();

            if (mkdirExitCode == 0) {
                // Le répertoire a été créé avec succès
                System.out.println("Répertoire créé avec succès");
            } else {
                System.err.println("-".repeat(80));

                // Une erreur s'est produite lors de la création du répertoire
                System.err.println("Erreur lors de la création du répertoire");
                mkdirProcess.getErrorStream().transferTo(System.err);

                System.err.println("-".repeat(80));
                System.exit(1);
            }

            ProcessBuilder moveJarToDir = new ProcessBuilder("mv", PROJECT_DIR + File.separator + SLAVE_JAR, "." + REMOTE_DIR);
            Process moveJarToDirProcess = moveJarToDir.start();
            int moveToDirExitCode = moveJarToDirProcess.waitFor();

            if (moveToDirExitCode == 0) {
                // Le fichier a été déplacé avec succès
                System.out.println("Fichier déplacé avec succès");
            } else {
                showErrorMessage("Erreur lors du déplacement du fichier SLAVE.jar", moveJarToDirProcess);
            }

            ProcessBuilder moveMachinesFileToDir = new ProcessBuilder("cp", MACHINES_FILE, "." + REMOTE_DIR);
            Process moveMachinesFileToDirProcess = moveMachinesFileToDir.start();
            int moveMachinesFileToDirExitCode = moveMachinesFileToDirProcess.waitFor();

            if (moveMachinesFileToDirExitCode == 0) {
                // Le fichier a été déplacé avec succès
                System.out.println("Fichier déplacé avec succès");
            } else {
                showErrorMessage("Erreur lors du déplacement du fichier machines.txt", moveMachinesFileToDirProcess);
            }


            // Tester la connexion SSH sur chaque machine et copier le fichier "slave.jar"
            // si la connexion réussit
            machines.forEach(ipAddress -> {
                int machineNumber = machines.indexOf(ipAddress);
                String machine = String.format("%s@%s", USERNAME, ipAddress);

                try {
                    // Copier le fichier "SLAVE.jar" dans le répertoire distant
                    ProcessBuilder scpPb = new ProcessBuilder("scp", "-r", "." + REMOTE_DIR, machine + ":");
                    Process scpProcess = scpPb.start();
                    int scpExitCode = scpProcess.waitFor();

                    if (scpExitCode == 0) {
                        // La copie du fichier s'est terminée avec succès
                        System.out.println("Fichiers copiés sur la machine " + machineNumber + ": " + ipAddress);
                    } else {
                        showErrorMessage("Erreur lors de la copie des fichiers sur la machine " + machineNumber + ": " + ipAddress, scpProcess);
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

    private void showErrorMessage(String message, Process process) throws IOException {
        System.err.println("-".repeat(80));

        System.err.println(message);
        // Print error stream
        process.getErrorStream().transferTo(System.err);

        System.err.println("-".repeat(80));
        System.exit(1);
    }
}
