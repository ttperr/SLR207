package Step1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class CountWords {
    
    private String text;

    private HashMap<String, Integer> wordsMap;
    private HashSet<String> wordsSet;
    private ArrayList<String> wordsList;

    public CountWords(String path) {
        wordsMap = new HashMap<String, Integer>();
        wordsSet = new HashSet<String>();
        wordsList = new ArrayList<String>();

        text = new String();
        text = readFile(path);

        System.out.println("Starting to count words with HashMap...");
        long startTime = System.currentTimeMillis();
        countWordsWithHashMap();
        long endTime = System.currentTimeMillis();

        // System.out.println("Starting to count words with HashSet...");
        // long startTime2 = System.currentTimeMillis();
        // countWordsWithHashSet();
        // long endTime2 = System.currentTimeMillis();

        // System.out.println("Starting to count words with ArrayList...");
        // long startTime3 = System.currentTimeMillis();
        // countWordsWithArrayList();
        // long endTime3 = System.currentTimeMillis();

        System.out.println("HashMap: " + (endTime - startTime) + " ms");
        // System.out.println("HashSet: " + (endTime2 - startTime2) + " ms");
        // System.out.println("ArrayList: " + (endTime3 - startTime3) + " ms");
    }

    private String readFile(String path) {
        File file = new File(path);
        BufferedReader br;
        String content = new String();
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                content += line;
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }

    private void countWordsWithHashMap() {
        String[] words = text.split(" ");
        for (String word : words) {
            if (wordsMap.containsKey(word)) {
                wordsMap.put(word, wordsMap.get(word) + 1);
            } else {
                wordsMap.put(word, 1);
            }
        }
        // Printing
        // for (String key : wordsMap.keySet()) {
        //     System.out.println(key + ": " + wordsMap.get(key));
        // }
    }

    private void countWordsWithHashSet() {
        String[] words = text.split(" ");
        for (String word : words) {
            wordsSet.add(word);
        }
        // Printing the word: count
        for (String word : wordsSet) {
            int count = 0;
            for (String w : words) {
                if (w.equals(word)) {
                    count++;
                }
            }
            System.out.println(word + ": " + count);
        }
    }

    private void countWordsWithArrayList() {    
        String[] words = text.split(" ");
        for (String word : words) {
            if (!wordsList.contains(word)) {
                wordsList.add(word);
            }
        }
        // Printing the word: count
        for (String word : wordsList) {
            int count = 0;
            for (String w : words) {
                if (w.equals(word)) {
                    count++;
                }
            }
            System.out.println(word + ": " + count);
        }
    }

    public static void main(String[] args) {
        new CountWords("./tmp/slr207/input.txt");
    }

}
