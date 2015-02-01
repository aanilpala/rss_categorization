package rss.categorizer.Batch;

import org.apache.commons.lang.ArrayUtils;

import java.util.*;


/**
 * Created by minza on 2/1/15.
 */
public class BatchDictionary {
    private HashMap<String, ArrayList<Integer>> dictionary;
    private List<Double> tfIdf;
    private int lastIndex;
    private int size;
    private int numDoc;


    public BatchDictionary(){


        this.tfIdf = new ArrayList<Double>();
        // tfIdf.set(-1,0.0);
        //word, [index, docWithTermCount]
       // ArrayList<Integer> current = new ArrayList<Integer>();

        this.dictionary = new HashMap<String, ArrayList<Integer>>();
        this.lastIndex = -1;
        this.size = dictionary.size();
        this.numDoc = 0;
    }

    public HashMap<String, ArrayList<Integer>> getDictionary(){return this.dictionary;}

    public Integer getLastIndex(){return this.lastIndex;}

    public Integer getSize(){return this.size;}

    public int[] getIndexVector(){
        int[] vect = new int[this.size];
        for(int i = 0; i < this.size; i++){
            vect[i] = i;
        }
        return vect;
    }

    public void update(String sentences){
        this.numDoc++;
        boolean isNewDoc = true;
        String[] terms = sentences.split("\\s+");


        for(String w : terms){

            if(!w.isEmpty()) {
                if (this.dictionary.containsKey(w)) {
                    if (isNewDoc) {
                        this.dictionary.get(w).set( 1, this.dictionary.get(w).get(1) + 1);
                        isNewDoc = false;

                    }
                } else {
                    w.toLowerCase().replaceAll("[^\\dA-Za-z]", "").trim();

                    ArrayList<Integer> current = new ArrayList<Integer>();
                    this.lastIndex += 1;
                    current.add(0, this.getLastIndex());
                    current.add(1, 1);
                    this.dictionary.put(w, current);
                    this.size = dictionary.size();
                    this.tfIdf.add(this.dictionary.get(w).get(0), Math.log(this.numDoc));


                }
            }
        }
        updateTfIdf();

    }

    public double[] getTfIdfVector(){

        return ArrayUtils.toPrimitive(this.tfIdf.toArray(new Double[this.tfIdf.size()]));
    }

    public int getNumDoc(){
        return this.numDoc;
    }

    public void updateTfIdf(){
        double tfIdfVal = 0.0;
        Iterator it = this.dictionary.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry pairs = (Map.Entry)it.next();
            tfIdfVal = Math.log((double)this.numDoc/(double)this.dictionary.get(pairs.getKey()).get(1));
            this.tfIdf.set(this.dictionary.get(pairs.getKey()).get(0), tfIdfVal);
           // System.out.println(pairs.getKey() + " = " + pairs.getValue());
        }
   //
   //

    }
}
