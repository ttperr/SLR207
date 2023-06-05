package Step10;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SLAVE {
    private static final String USERNAME = "tperrot-21";
    private static final String HOME_DIRECTORY = "/tmp/" + USERNAME;
    private static final String MAP_DIRECTORY = HOME_DIRECTORY + "/maps";

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
}
