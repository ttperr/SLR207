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
    private static final String SPLIT_DIRECTORY = "/tmp/" + USERNAME + "/splits";
    private static final String MAP_DIRECTORY = "/tmp/" + USERNAME + "/maps";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java SLAVE <mode> <inputFile>");
            return;
        }

        int mode = Integer.parseInt(args[0]);
        String inputFile = args[1];

        if (mode == 0) {
            processSplit(inputFile);
        } else {
            System.err.println("Mode invalide.");
        }
    }

    private static void processSplit(String inputFile) {
        try {
            // Lire le fichier split
            String inputFilePath = SPLIT_DIRECTORY + File.separator + inputFile;
            BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));

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

            // Écrire le résultat dans le fichier de sortie UMx.txt
            String outputFileName = "UM" + inputFile.charAt(1);
            String outputFilePath = MAP_DIRECTORY + File.separator + outputFileName;
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));

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
