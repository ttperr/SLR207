package Step5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class MASTER {
    public static void main(String[] args) {
        try {
            String jarPath = "/tmp/tperrot-21/SLAVE.jar";

            ProcessBuilder pb = new ProcessBuilder("java", "-jar", jarPath);
            Process process = pb.start();

            // Récupérer la sortie du processus
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Attendre la fin de l'exécution du processus
            int exitCode = process.waitFor();
            System.out.println("Code de sortie : " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
