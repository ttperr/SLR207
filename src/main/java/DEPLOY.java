import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DEPLOY {
    private static final String USERNAME = "tperrot-21";

    private static final String TEMP_DIR = "/tmp";
    private static final String REMOTE_DIR = TEMP_DIR + "/" + USERNAME;
    private static final String SPLIT_DIR = REMOTE_DIR + "/splits";

    private static final String PROJECT_DIR = "src/main";
    private static final String SRC_DIR = PROJECT_DIR + "/java";

    private static final String SLAVE = "SLAVE";
    private static final String SLAVE_JAR = "SLAVE.jar";

    private static final String DATA_DIR = "data/";
    private static final String MACHINES_FILE = DATA_DIR + "machines.txt";

    private static final boolean isTest = true;

    private static final String TEXT_FILE = "text/test.txt";

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

            if (javacExitCode != 0) {
                showErrorMessage("Erreur lors de la compilation", javacProcess);
            }
            // La compilation s'est terminée avec succès
            System.out.println("Compilation terminée avec succès");

            // Créer le fichier JAR
            ProcessBuilder jarPb = new ProcessBuilder("jar", "cvfe", ".." + File.separator + SLAVE_JAR, SLAVE,
                    SLAVE + ".class");
            jarPb.directory(srcDirPath.toFile());
            Process jarProcess = jarPb.start();
            int jarExitCode = jarProcess.waitFor();

            if (jarExitCode != 0) {
                showErrorMessage("Erreur lors de la création du fichier JAR", jarProcess);
            }
            // Le fichier JAR a été créé avec succès
            System.out.println("Fichier JAR créé avec succès");

            // Supprimer le fichier SLAVE.class
            ProcessBuilder rmPb = new ProcessBuilder("rm", SLAVE + ".class");
            rmPb.directory(srcDirPath.toFile());
            Process rmProcess = rmPb.start();
            int rmExitCode = rmProcess.waitFor();

            if (rmExitCode != 0) {
                showErrorMessage("Erreur lors de la suppression du fichier SLAVE.class", rmProcess);
            }
            // Le fichier SLAVE.class a été supprimé avec succès
            System.out.println("Fichier SLAVE.class supprimé avec succès");

            System.out.println("\nDéploiement sur les machines...\n");


            // Création du répertoire à envoyer sur la machine principale
            ProcessBuilder mkdirPb = new ProcessBuilder("mkdir", "-p", "." + REMOTE_DIR);
            Process mkdirProcess = mkdirPb.start();
            int mkdirExitCode = mkdirProcess.waitFor();

            if (mkdirExitCode != 0) {
                System.err.println("-".repeat(80));

                // Une erreur s'est produite lors de la création du répertoire
                System.err.println("Erreur lors de la création du répertoire");
                mkdirProcess.getErrorStream().transferTo(System.err);

                System.err.println("-".repeat(80));
                System.exit(1);
            }
            // Le répertoire a été créé avec succès
            System.out.println("Répertoire créé avec succès\n");


            ProcessBuilder moveJarToDir = new ProcessBuilder("mv", PROJECT_DIR + File.separator + SLAVE_JAR,
                    "." + REMOTE_DIR);
            Process moveJarToDirProcess = moveJarToDir.start();
            int moveToDirExitCode = moveJarToDirProcess.waitFor();

            if (moveToDirExitCode != 0) {
                showErrorMessage("Erreur lors du déplacement du fichier SLAVE.jar", moveJarToDirProcess);
            }
            // Le fichier a été déplacé avec succès
            System.out.println("Fichier " + SLAVE_JAR + " déplacé avec succès");

            // Pré process du fichier txt
            if (isTest) {
                preProcessFile(TEXT_FILE, machines);
            }

            ProcessBuilder moveMachinesFileToDir = new ProcessBuilder("cp", MACHINES_FILE, "." + REMOTE_DIR);
            Process moveMachinesFileToDirProcess = moveMachinesFileToDir.start();
            int moveMachinesFileToDirExitCode = moveMachinesFileToDirProcess.waitFor();

            if (moveMachinesFileToDirExitCode != 0) {
                showErrorMessage("Erreur lors du déplacement du fichier machines.txt", moveMachinesFileToDirProcess);
            }
            // Le fichier a été déplacé avec succès
            System.out.println("Fichier machines.txt déplacé avec succès");

            // Création du dossier SPLIT_DIR
            ProcessBuilder mkdirSplitDir = new ProcessBuilder("mkdir", "-p", "." + SPLIT_DIR);
            Process mkdirSplitDirProcess = mkdirSplitDir.start();
            int mkdirSplitDirExitCode = mkdirSplitDirProcess.waitFor();

            if (mkdirSplitDirExitCode != 0) {
                showErrorMessage("Erreur lors de la création du répertoire " + SPLIT_DIR, mkdirSplitDirProcess);
            }
            // Le répertoire a été créé avec succès
            System.out.println("Répertoire " + SPLIT_DIR + " créé avec succès\n");


            // Tester la connexion SSH sur chaque machine et copier le fichier "slave.jar"
            // si la connexion réussit
            machines.forEach(ipAddress -> {
                int machineNumber = machines.indexOf(ipAddress);
                String machine = String.format("%s@%s", USERNAME, ipAddress);

                try {

                    if (isTest) {
                        // Copie du split dans le dossier .REMOTE_DIR
                        ProcessBuilder moveSplitToDir = new ProcessBuilder("cp",
                                DATA_DIR + "splits" + File.separator + "S" + machineNumber + ".txt", "." + SPLIT_DIR + File.separator);
                        Process moveSplitToDirProcess = moveSplitToDir.start();
                        int moveSplitToDirExitCode = moveSplitToDirProcess.waitFor();

                        if (moveSplitToDirExitCode != 0) {
                            showErrorMessage("Erreur lors du déplacement du fichier split", moveSplitToDirProcess);
                        }
                        // Le fichier a été déplacé avec succès
                        System.out.println("\nFichier split S" + machineNumber + " déplacé avec succès");
                    }

                    // Copier du dossier tmp dans le répertoire distant
                    ProcessBuilder scpPb = new ProcessBuilder("scp", "-r", "." + REMOTE_DIR, machine + ":" + TEMP_DIR);
                    Process scpProcess = scpPb.start();
                    int scpExitCode = scpProcess.waitFor();

                    if (scpExitCode != 0) {
                        showErrorMessage("Erreur lors de la copie des fichiers sur la machine " + machineNumber + ": "
                                + ipAddress, scpProcess);
                    }
                    // La copie du fichier s'est terminée avec succès
                    System.out.println("Fichiers copiés sur la machine " + machineNumber + ": " + ipAddress);

                    if (isTest) {
                        // Suppression du split du dossier splits
                        ProcessBuilder rmSplit = new ProcessBuilder("rm",
                                "." + SPLIT_DIR + File.separator + "S" + machineNumber + ".txt");
                        Process rmSplitProcess = rmSplit.start();
                        int rmSplitExitCode = rmSplitProcess.waitFor();

                        if (rmSplitExitCode != 0) {
                            showErrorMessage("Erreur lors de la suppression du fichier split", rmSplitProcess);
                        }
                        // Le fichier a été supprimé avec succès
                        System.out.println("Fichier split S" + machineNumber + " supprimé avec succès");
                    }

                    // Lancer le programme sur la machine distante
                    // ProcessBuilder sshPb = new ProcessBuilder("ssh", machine, "java", "-jar",
                    // REMOTE_DIR + File.separator + SLAVE_JAR);
                    // sshPb.start();
                    // System.out.println(
                    // "Programme lancé sur la machine " + machineNumber + ": " + ipAddress + "\n");


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

    private void preProcessFile(String path, List<String> machines) {
        File file = new File(path);
        int nbMachines = machines.size();
        try {
            String content = Files.readString(file.toPath());
            content = content.replaceAll("\n ", " ");
            content = content.replaceAll("[,\\n]|\\b\\W+\\b", " ");
            content = content.replaceAll(" +", " ");


            // Split in nbMachines files but cut only at spaces
            int chunkSize = content.length() / nbMachines;

            // Find the last space index within each chunk
            List<Integer> lastSpaceIndices = new ArrayList<>();
            int startIndex = 0;
            for (int i = 0; i < nbMachines - 1; i++) {
                int lastSpaceIndex = content.lastIndexOf(" ", startIndex + chunkSize);
                lastSpaceIndices.add(lastSpaceIndex);
                startIndex = lastSpaceIndex + 1;
            }

            // Split the content into chunks and write them to separate files
            startIndex = 0;
            for (int i = 0; i < nbMachines; i++) {
                int endIndex = (i < nbMachines - 1) ? lastSpaceIndices.get(i) : content.length();
                String chunk = content.substring(startIndex, endIndex);

                // Write the chunk to a separate file
                String chunkPath = String.format(DATA_DIR + "splits/S%d.txt", i);

                // Write file with write string method

                FileWriter fileWriter = new FileWriter(chunkPath);
                try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                    bufferedWriter.write(chunk);
                }

                // Close
                fileWriter.close();

                startIndex = endIndex + 1;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
