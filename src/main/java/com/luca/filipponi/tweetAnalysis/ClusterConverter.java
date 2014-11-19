package com.luca.filipponi.tweetAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.HashMap;

public class ClusterConverter {

    /**
     * This class contains in the main code for create a .txt file, starting
     * from a csv clustering file, and the initial dataset, in order to
     * visualize all the text of the tweet belonging to a cluster
     *
     * @param args
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */

    public static void main(String args[]) throws IOException,
            InstantiationException, IllegalAccessException {

        // creating the hashtable
        HashMap<String, String> table = new HashMap<String, String>();

        // inserting all the key-value from the sequenceFile

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                "TextualClustering/input/InitialCsvTweetVector/part-r-00000"),
                conf);

        Writable key;
        Writable value;

        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        // in this way I should create an instance of the correct class
        key = (Writable) (keyClass.newInstance());
        value = (Writable) (valueClass.newInstance());

        System.out.println("Key class: " + keyClass.toString()
                + " Value class: " + valueClass.toString());

        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {
            table.put(key.toString(), value.toString());

        }

        reader.close();

        // I've created the hashtable

        // reading from the csv file
        String csvFile = "consoleOutput/TextualClustering/k-means/k=3/Elezioni[20-150-1550].csv";

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        BufferedWriter wr = new BufferedWriter(
                new FileWriter(
                        "consoleOutput/TextualClustering/k-means/k=3/Elezioni[20-150-1550]Converted.csv"));

        String line;

        wr.write("Clustering contained in csv file: " + csvFile);

        while ((line = br.readLine()) != null) {

            String[] cluster = line.split(","); // in questo modo tokenizzo
            // tutta la riga

            if (cluster.length == 1) {
                System.out.println(cluster[0]);
                wr.write(cluster[0] + "\n");
                continue;

            }

            System.out.println("Tweet Contained in Cluster: " + cluster[0]);
            wr.write("\n\nTweet Contained in Cluster: " + cluster[0] + "\n\n");

            for (int i = 1; i < cluster.length; i++) {

                System.out.println("Id: " + cluster[i] + " text: "
                        + table.get(cluster[i]));
                wr.write("Id: " + cluster[i] + " text: "
                        + table.get(cluster[i]) + "\n");

            }

        }
        br.close();
        wr.close();

    }

    /**
     * Given a CSV input file, creates a .txt, where the are the cluster and the
     * original Text of the tweet Uses an hash map to record all the pair
     * tweetID,tweetText, in order to obtain a fast access to the orginal text
     * given a tweetID from the clustering
     *
     * @param originalVectorFile originalVectorFile The sequenceFile of the initial vector
     *                           (needed for saving the text of each tweet id)
     * @param csvInputFile       The csv file of the clustering (must export in csv)
     * @param outputFile         Output file where store the converted File
     * @param print              if true, print in output all value, useful for debug purpose
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public static void convert(String originalVectorFile, String csvInputFile,
                               String outputFile, boolean print) throws IOException,
            InstantiationException, IllegalAccessException {

        HashMap<String, String> table = new HashMap<String, String>();

        // inserting all the key-value from the sequenceFile

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                originalVectorFile), conf);

        Writable key;
        Writable value;

        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        // in this way I should create an instance of the correct class
        key = (Writable) (keyClass.newInstance());
        value = (Writable) (valueClass.newInstance());

        System.out.println("Converting csv cluster file: Input file"
                + csvInputFile + " output file" + outputFile
                + " original Vector file" + originalVectorFile);

        System.out.println("Key class: " + keyClass.toString()
                + " Value class: " + valueClass.toString());

        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {
            table.put(key.toString(), value.toString());

        }

        reader.close();

        // I've created the hashtable

        // reading from the csv file


        BufferedReader br = new BufferedReader(new FileReader(csvInputFile));
        BufferedWriter wr = new BufferedWriter(new FileWriter(outputFile));

        String line;

        wr.write("Clustering contained in csv file: " + csvInputFile);

        while ((line = br.readLine()) != null) {

            String[] cluster = line.split(","); // in questo modo tokenizzo
            // tutta la riga

            if (cluster.length == 1) {
                System.out.println(cluster[0]);
                wr.write(cluster[0] + "\n");
                continue;

            }
            if (print == true)
                System.out.println("Tweet Contained in Cluster: " + cluster[0]);
            wr.write("\n\nTweet Contained in Cluster: " + cluster[0] + "\n\n");

            for (int i = 1; i < cluster.length; i++) {

                if (print == true)
                    System.out.println("Id: " + cluster[i] + " text: "
                            + table.get(cluster[i]));
                wr.write("Id: " + cluster[i] + " text: "
                        + table.get(cluster[i]) + "\n");

            }

        }
        br.close();
        wr.close();

    }

}
