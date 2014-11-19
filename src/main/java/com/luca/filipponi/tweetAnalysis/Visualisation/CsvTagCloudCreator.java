package com.luca.filipponi.tweetAnalysis.Visualisation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Class for create a csv file with the top Word, that can be used for create a tag Cloud with JavaScript
 * <p/>
 * <p/>
 * Created by luca on 06/06/14.
 */
public class CsvTagCloudCreator {
    //uso questa classe per la creazioni del file csv a partire dai sequenceFile

    //read the frequency-file

    /*
    Copia il codice da sequenceFile creator in modo da leggere il frequency file e poter creare il tag cloud, sia per tutto il dataset
    che per i vari cluster
    */


    public static void main(String args[]) throws IOException {


        boolean printAll = true;
        int n = 0; //number of entry to visualize


        //creo un hashmap dove salvo la coppia IdWord:Value

        HashMap<String, String> wordIdOccurrency = new HashMap<String, String>();
        HashMap<String, String> wordOccurrency = new HashMap<String, String>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        // opening sequenceFIle reader


        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("TextualClustering/input/frequency.file-0"), conf);

        BufferedWriter wr = new BufferedWriter(new FileWriter("src/main/java/com/luca/filipponi/tweetAnalysis/Visualisation/topic_wordsElezioni.csv"));


        int i = 0;
        Writable key;
        Writable value;
        int maxFrequency = 0;

        try {
            // <?> indicates that I'don't know the class to be modeled, perhaps
            // I can put Class<Writable>
            Class<?> keyClass = reader.getKeyClass();
            Class<?> valueClass = reader.getValueClass();

            // in this way I should create an instance of the correct class
            key = (Writable) (keyClass.newInstance());
            value = (Writable) (valueClass.newInstance());

            System.out.println("Key class: " + keyClass.toString()
                    + " Value class: " + valueClass.toString());

            // allocate key-vale based on the writing function
            wordIdOccurrency.put(key.toString(), value.toString());

            while (reader.next(key, value)) {
                System.out.println("Key: " + key.toString() + " Value:"
                        + value.toString());

                if (Integer.parseInt(value.toString()) > maxFrequency)
                    maxFrequency = Integer.parseInt(value.toString());
                wordIdOccurrency.put(key.toString(), value.toString());
                i++;
                if (printAll == false && i > n)
                    break; // exit from the cicle
            }
            reader.close();

            System.out.println("MaxFrequency value " + maxFrequency);

            reader = new SequenceFile.Reader(fs, new Path("TextualClustering/input/dictionary.file-0"), conf);

            System.out.println("Reading the dictionary file");

            try {
                // <?> indicates that I'don't know the class to be modeled, perhaps
                // I can put Class<Writable>
                keyClass = reader.getKeyClass();
                valueClass = reader.getValueClass();

                // in this way I should create an instance of the correct class
                key = (Writable) (keyClass.newInstance());
                value = (Writable) (valueClass.newInstance());

                System.out.println("Key class: " + keyClass.toString()
                        + " Value class: " + valueClass.toString());

                // allocate key-vale based on the writing function
                wordIdOccurrency.put(key.toString(), value.toString());
                wr.write("text,size,topic");
                wr.write("\n");
                while (reader.next(key, value)) {
                    System.out.println("Key: " + key.toString() + " Value:"
                            + value.toString() + " Frequency: " + wordIdOccurrency.get(value.toString()));
                    wr.write(key.toString() + "," + wordIdOccurrency.get(value.toString()) + ",0");
                    wr.write("\n");

                    //append in the 2th hashmap to create an ordered hashmap
                    wordOccurrency.put(key.toString(), wordIdOccurrency.get(value.toString()));

                    wordIdOccurrency.put(key.toString(), value.toString());
                    i++;
                    if (printAll == false && i > n)
                        break; // exit from the cycle
                }


            } catch (InstantiationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            reader.close();
            wr.close();

            //declaring an array of List as big as the max word frequency
            List[] indexFrequency; //the +1 is neede becouse the numbers starts to 1
            indexFrequency = new ArrayList[maxFrequency + 1];


            int index;
            String word;
            Iterator it = wordOccurrency.entrySet().iterator();
            while (it.hasNext()) {

                Map.Entry pairs = (Map.Entry) it.next();
                System.out.println(pairs.getKey() + " = " + pairs.getValue());

                index = Integer.parseInt(pairs.getValue().toString());
                word = pairs.getKey().toString();


                if (indexFrequency[index] == null) {


                    indexFrequency[index] = new ArrayList<String>();
                }


                boolean add = indexFrequency[index].add(word);

            }


            //scrivo su file la frequenza di ogni parola in ordine crescente
            wr = new BufferedWriter(new FileWriter("src/main/java/com/luca/filipponi/tweetAnalysis/Visualisation/FrequencyWord.txt"));

            for (i = (indexFrequency.length) - 1; i > 0; i--) {

                if (indexFrequency[i] != null) {
                    wr.write("Frequency: " + i + ", Word List: { ");
                    System.out.print("Frequency: " + i + ", Word List: { ");

                    for (Object s : indexFrequency[i]) {

                        if (indexFrequency[i].size() > 1) {
                            wr.write(s.toString() + ", ");
                            System.out.print(s.toString() + ", ");
                        } else {
                            wr.write(s.toString());
                            System.out.print(s.toString() + ", ");
                        }
                    }
                    System.out.println(" }");
                    wr.write(" }\n");
                }


            }

            int numWord = 100;

            wr = new BufferedWriter(new FileWriter("src/main/java/com/luca/filipponi/tweetAnalysis/Visualisation/top" + numWord + "Word.csv"));


            System.out.println("text,size,topic\n");
            wr.write("text,size,topic\n");


            for (i = (indexFrequency.length) - 1; i > 0; i--) {

                if (indexFrequency[i] != null) {
                    if (numWord == 0)
                        break;


                    for (Object s : indexFrequency[i]) {

                        if (numWord > 0) {
                            numWord--;
                            System.out.println("Word: " + numWord + " " + s + "," + i + ",0\n");
                            wr.write(s + "," + i + ",0\n");
                        } else
                            break;

                    }


                }


            }
            wr.close();

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        //creo un file di topic in modo che vengano ottenute solo n parole partendo dall'hashMap che ho creato


    }

}