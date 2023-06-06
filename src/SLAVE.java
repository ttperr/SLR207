import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class SLAVE {
    private static final String USERNAME = "tperrot-21";
    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String MAP_DIRECTORY = HOME_DIRECTORY + "/maps";
    private static final String SHUFFLE_DIRECTORY = HOME_DIRECTORY + "/shuffles";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java SLAVE <mode> <inputFile>");
            return;
        }

        int mode = Integer.parseInt(args[0]);
        String inputFile = args[1];

        System.out.println("Mode: " + mode);
        System.out.println("Input file: " + inputFile);

        if (mode == 0) {
            processSplit(inputFile);
        } else if (mode == 1) {
            processMapOutput(inputFile);
        } else {
            System.err.println("Mode invalide.");
        }
    }

    private static void processSplit(String inputFile) {
        try {
            // Lire le fichier split
            BufferedReader reader = new BufferedReader(new FileReader(inputFile));

            // Créer la map pour stocker les mots et leur fréquence
            Map<String, Integer> wordMap = new HashMap<>();

            // Analyser le split et remplir la map
            String line;
            while ((line = reader.readLine()) != null) {
                String[] words = line.split(" ");
                for (String word : words) {
                    wordMap.put(word, 1);
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
            for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                String word = entry.getKey();
                int frequency = entry.getValue();
                writer.write(word + ", " + frequency);
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

            // Obtenir le hash à partir de la clé ou la clef est comprise entre "/UM" et .txt
            String beginSlice = File.separator + "UM";
            String endSlice = ".txt";
            String key = inputFile.substring(inputFile.indexOf(beginSlice) + beginSlice.length(),
                    inputFile.indexOf(endSlice));
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

            // Écrire les mots et leur fréquence dans le fichier de sortie
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
            }

            writer.close();
            reader.close();

            System.out.println("Calcul du shuffle terminé pour le fichier " + inputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
