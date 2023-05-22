package MultiThread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class MASTER {
    public static void main(String[] args) {
        try {
            ProcessBuilder pb = new ProcessBuilder("ls", "/jesuisunhero");
            Process process = pb.start();

            // Récupérer la sortie standard du processus
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Récupérer la sortie d'erreur du processus
            InputStream errorStream = process.getErrorStream();
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(errorStream));

            // Afficher la sortie standard
            String line;
            System.out.println("Sortie standard:");
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Afficher la sortie d'erreur s'il y en a
            String errorLine;
            System.out.println("Sortie d'erreur:");
            while ((errorLine = errorReader.readLine()) != null) {
                System.out.println(errorLine);
            }

            // Attendre la fin de l'exécution du processus
            int exitCode = process.waitFor();
            System.out.println("Code de sortie : " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

