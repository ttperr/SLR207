package stepByStep.step6;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MASTER {
    private static final int TIMEOUT = 15000; // Temps d'attente maximum en millisecondes

    public static void main(String[] args) {
        try {
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/tmp/tperrot-21/SLAVE.jar");
            Process process = pb.start();

            // Créer les files pour la sortie standard et la sortie d'erreur
            BlockingQueue<String> standardOutputQueue = new LinkedBlockingQueue<>();
            BlockingQueue<String> errorOutputQueue = new LinkedBlockingQueue<>();

            // Créer les threads pour la lecture de la sortie standard et la sortie d'erreur
            Thread standardOutputThread = createOutputThread(process.getInputStream(), standardOutputQueue);
            Thread errorOutputThread = createOutputThread(process.getErrorStream(), errorOutputQueue);

            standardOutputThread.start();
            errorOutputThread.start();

            // Attendre que l'une des files se remplisse au début
            while (standardOutputQueue.isEmpty() && errorOutputQueue.isEmpty()) {
                // Attendre un court instant avant de vérifier à nouveau
                Thread.sleep(100);
            }

            // Lire les éléments des files jusqu'à ce qu'elles soient vides
            boolean isStandardOutputEmpty = standardOutputQueue.isEmpty();
            boolean isErrorOutputEmpty = errorOutputQueue.isEmpty();

            while (!isStandardOutputEmpty || !isErrorOutputEmpty) {
                String standardOutput = standardOutputQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                if (standardOutput != null) {
                    System.out.println("Sortie standard : " + standardOutput);
                }

                String errorOutput = errorOutputQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
                if (errorOutput != null) {
                    System.out.println("Sortie d'erreur : " + errorOutput);
                }

                isStandardOutputEmpty = standardOutputQueue.isEmpty();
                isErrorOutputEmpty = errorOutputQueue.isEmpty();
            }

            // Arrêter les threads
            standardOutputThread.interrupt();
            errorOutputThread.interrupt();

            // Arrêter le processus
            process.destroy();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Thread createOutputThread(InputStream inputStream, BlockingQueue<String> outputQueue) {
        return new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    outputQueue.put(line);
                }
            } catch (IOException | InterruptedException e) {
                // Le thread peut être interrompu lors d'un timeout, donc l'exception est
                // ignorée
            }
        });
    }
}
