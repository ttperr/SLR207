import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

public class TEST {

    private static final String TEST_PATH = DEPLOY.TEXT_FILE;
    private static final String RESULT_PATH = MASTER.RESULT_FILE;

    public static void main(String[] args) throws IOException {
        if (countWordsOccurrence()) {
            System.out.println("\nTest passed :)");
        } else {
            System.out.println("\nTest failed :(");
        }
    }

    private static boolean countWordsOccurrence() throws IOException {
        File testFile = new File(TEST_PATH);
        File resultFile = new File(RESULT_PATH);

        String content = Files.readString(testFile.toPath());
        content = content.replaceAll("\n ", " ");
        content = content.replaceAll("[,\\n]|\\b\\W+\\b", " ");
        content = content.replaceAll(" +", " ");

        String[] words = content.split(" ");
        HashMap<String, Integer> wordsOccurrence = new HashMap<>();

        for (String word : words) {
            if (wordsOccurrence.containsKey(word)) {
                wordsOccurrence.put(word, wordsOccurrence.get(word) + 1);
            } else {
                wordsOccurrence.put(word, 1);
            }
        }

        BufferedReader reader = new BufferedReader(new FileReader(resultFile));

        String line;
        while((line = reader.readLine()) != null) {
            String[] keyVal = line.split(", ");
            int val = Integer.parseInt(keyVal[1]);
            if (wordsOccurrence.get(keyVal[0]) != val) {
                reader.close();;
                return false;
            }
        }
        reader.close();
        return true;
    }
}
