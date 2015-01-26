package feed.model;

import java.io.*;
import java.util.HashMap;

/**
 * Created by minza on 1/26/15.
 */
public class Dictionary {

    private HashMap<String, Integer> dictionary;
    private String source;
    private Integer lastIndex;
    private Integer size;

    // constructs dictionary from previous rss-feeds stored in a file by passing file name
    public Dictionary(String pathToFile)  {
        lastIndex = -1;
        this.source = pathToFile;
        this.dictionary = new HashMap<String, Integer>();
        fillDictionary(this.source);
        this.size = this.dictionary.size();
    }

    public String getSource(){return this.source;}

    public HashMap<String, Integer> getDictionary(){return this.dictionary;}

    public Integer getLastIndex(){return this.lastIndex;}

    public double[] initBagOfWordVector(){return new double[this.size];}

    public Integer getSize(){return this.size;}



    public void update(FeedItem newFeed){
        String content = newFeed.getTitle() + " " + newFeed.getDescription();
        String[] tokens = content.split("\\s+");
        for(String w : tokens){
            String dicEntry = w.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();
            if(!dicEntry.isEmpty()){
                if(!this.dictionary.containsKey(dicEntry)){
                    this.dictionary.put(dicEntry,this.lastIndex = this.lastIndex + 1);
                    this.size = this.dictionary.size();
                }
            }
        }
    }

    public int[] getIndexVector(){
        int[] vect = new int[this.size];
        for(int i = 0; i < this.size; i++){
            vect[i] = i;
        }
        return vect;
    }

    private void fillDictionary(String src){
        File inputFile = new File(src);

        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(inputFile));
            String line;
            while((line = br.readLine()) != null){
                String[] words = line.split("\\s+");
                for(int i = 7; i < words.length; i++){

                    //preprocess: lowercase, just letters and numbers
                    String dicEntry = words[i].toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();
                    if(!dicEntry.isEmpty()){
                        if(!this.dictionary.containsKey(dicEntry)){
                            this.dictionary.put(dicEntry,this.lastIndex = this.lastIndex + 1);
                        }
                    }
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
    }

        return;

    }
}
