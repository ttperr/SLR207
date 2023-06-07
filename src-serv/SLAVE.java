import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SLAVE {
    private static final String USERNAME = "tperrot-21";

    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String MAP_DIRECTORY = HOME_DIRECTORY + "/maps";
    private static final String SHUFFLE_DIRECTORY = HOME_DIRECTORY + "/shuffles";
    private static final String SHUFFLE_RECEIVED_DIRECTORY = HOME_DIRECTORY + "/shufflesreceived/";

    private static final String MACHINES_FILE = HOME_DIRECTORY + File.separator + "machines.txt";

    private static final HashMap<Integer, String> machines = new HashMap<Integer, String>();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java SLAVE <mode> <inputFile>");
            return;
        }

        int mode = Integer.parseInt(args[0]);
        String inputFile = args[1];

        System.out.println("Mode: " + mode);
        System.out.println("Input file: " + inputFile);

        File machinesFile = new File(MACHINES_FILE);
        getMachines(machinesFile);

        if (mode == 0) {
            processSplit(inputFile);
        } else if (mode == 1) {
            processMapOutput(inputFile);

            // Exécution de la phase shuffle
            processShuffleOutput();
        } else {
            System.err.println("Mode invalide.");
        }
    }

    private static void processSplit(String inputFile) {
        try {
            // Lire le fichier split
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            List<String> words = new ArrayList<String>();

            // Analyser le split et remplir la map
            String line;
            while ((line = reader.readLine()) != null) {
                String[] lineWords = line.split(" ");
                for (String word : lineWords) {
                    words.add(word);
                }
            }

            reader.close();

            // Création du répertoire de sortie
            File mapDirectory = new File(MAP_DIRECTORY);
            mapDirectory.mkdirs();

            // Écrire le résultat dans le fichier de sortie UMx.txt where x is the string
            // between S and .txt in the input file name
            String outputFileName = "UM" + inputFile.substring(inputFile.indexOf("S") + 1, inputFile.indexOf(".txt"))
                    + ".txt";
            String outputFilePath = MAP_DIRECTORY + File.separator + outputFileName;

            // Crée le fichier de sortie
            System.out.println("Création du fichier de sortie " + outputFilePath);
            File outputFile = new File(outputFilePath);
            outputFile.createNewFile();

            // Écrire le résultat dans le fichier de sortie
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

            // Écrire les mots et leur fréquence dans le fichier de sortie
            for (String word : words) {
                writer.write(word + ", 1");
                writer.newLine();
            }

            writer.close();

            System.out.println("Calcul du map terminé pour le fichier " + inputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processMapOutput(String inputFile) {
        try {
            // Lire le fichier UMx.txt
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            // Créer le répertoire de shuffle s'il n'existe pas
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            shuffleDirectory.mkdirs();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] keyValue = line.split(", ");
                if (keyValue.length == 2) {
                    String key = keyValue[0];
                    String value = keyValue[1];

                    // Obtenir le hash à partir de la clé
                    int hash = key.hashCode();

                    // Obtenir le nom de la machine
                    String hostname = InetAddress.getLocalHost().getHostName();

                    // Créer le nom de fichier pour la phase de shuffle
                    String outputFileName = hash + "-" + hostname + ".txt";
                    String outputFilePath = SHUFFLE_DIRECTORY + File.separator + outputFileName;

                    // Vérifier si le fichier existe déjà
                    File outputFile = new File(outputFilePath);

                    if (!outputFile.exists()) {
                        // Créer le fichier de sortie s'il n'existe pas
                        outputFile.createNewFile();
                    }

                    // Écrire le résultat dans le fichier de sortie
                    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile, true));

                    // Écrire la clé/valeur dans le fichier de sortie
                    writer.write(key + ", " + value);
                    writer.newLine();

                    writer.close();
                }
            }

            reader.close();

            System.out.println("Calcul du shuffle terminé pour le fichier " + inputFile);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getMachines(File machinesFile) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(machinesFile));

            String line;
            int machineNumber = 0;
            while ((line = reader.readLine()) != null) {
                machines.put(machineNumber, line);
                machineNumber++;
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processShuffleOutput() {
        try {
            // Création du dossier de réception des shuffles
            File shuffleReceivedDirectory = new File(SHUFFLE_RECEIVED_DIRECTORY);
            shuffleReceivedDirectory.mkdirs();

            // Lecture des fichiers de shuffle
            File shuffleDirectory = new File(SHUFFLE_DIRECTORY);
            File[] shuffleFiles = shuffleDirectory.listFiles();

            for (File shuffleFile : shuffleFiles) {
                int hash = Integer.parseInt(shuffleFile.getName().split("-")[0]);
                int machine = hash % machines.size();
                String ipAddress = machines.get(machine);

                // Envoi du fichier de shuffle à la machine et si le dossier de réception
                // n'existe pas, le créer
                System.out.println("Envoi du fichier " + shuffleFile.getName() + " à la machine " + ipAddress);
                String machineDirectory = String.format("%s@%s:%s", USERNAME, ipAddress, SHUFFLE_RECEIVED_DIRECTORY);

                ProcessBuilder pb = new ProcessBuilder("scp", shuffleFile.getAbsolutePath(), machineDirectory);
                Process p = pb.start();
                p.waitFor();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
