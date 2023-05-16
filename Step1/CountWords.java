package Step1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountWords {

    private String text;
    private String[] words;

    public CountWords(String path, int limit) {
        text = readFile(path);
        words = text.split(" ");

        System.out.println(
                "Starting to count words...");
        long startTime = System.currentTimeMillis();
        HashMap<String, Integer> wordsMap = countWordsFreq();
        long endTime = System.currentTimeMillis();
        System.out.println("\nTook: " + (endTime - startTime) + " ms to count\n");
        printFreq(wordsMap, limit);
        long endTime2 = System.currentTimeMillis();
        System.out.println("\nTook: " + (endTime2 - endTime) + " ms to print\n");
    }

    private String readFile(String path) {
        File file = new File(path);
        System.out.println(
                "\n################################### " + file.getName() + " ###################################\n");
        BufferedReader br;
        String content = new String();
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                content += " " + line;
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }

    private HashMap<String, Integer> countWordsFreq() {
        HashMap<String, Integer> wordsMap = new HashMap<String, Integer>();
        for (String word : words) {
            if (wordsMap.containsKey(word)) {
                wordsMap.put(word, wordsMap.get(word) + 1);
            } else {
                wordsMap.put(word, 1);
            }
        }
        return wordsMap;
    }

    private void printFreq(HashMap<String, Integer> wordsMap, int limit) {
        // Sorting
        List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(wordsMap.entrySet());
        Collections.sort(sortedList, (a, b) -> {
            int freqComparison = b.getValue().compareTo(a.getValue());
            if (freqComparison == 0) {
                return a.getKey().compareTo(b.getKey());
            }
            return freqComparison;
        });
        // Printing
        int k = 0;
        for (Map.Entry<String, Integer> entry : sortedList) {
            if (limit < 0 || k < limit) {
                k += 1;
                System.out.println("#" + k + " " + entry.getKey() + ": " + entry.getValue());
            } else {
                break;
            }
        }
    }

    public static void main(String[] args) {
        // new CountWords("Step1/data/input.txt", -1);
        // new CountWords("Step1/data/forestier_mayotte.txt", -1);
        new CountWords("Step1/data/deontologie_police_nationale.txt", 50);
        new CountWords("Step1/data/domaine_public_fluvial.txt", 50);
        new CountWords("Step1/data/sante_publique.txt", 50);
        // new
        // CountWords("/tmp/slr207/CC-MAIN-20230320083513-20230320113513-00000.warc.wet",
        // 50);
    }

}
