package com.luca.filipponi.tweetAnalysis.graphlab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by luca on 20/08/14.
 * <p/>
 * this class is for decode the output of graphlab clustering, which is composed from 2 file.
 * <p/>
 * The first one is a file containing the centroid in which there is:
 * <p/>
 * <p/>
 * (tab)
 * CentroidID   Feature represented as sparse vector (separed by space)
 * <p/>
 * The second one is a file containing the association between vector and centroid:
 * <p/>
 * Vectorid CentroidID
 * vectorID CentroidID ... for each input vector
 */
public class graphlabOutputDecoder {


    static Logger mylog = Logger.getLogger(graphlabOutputDecoder.class);

    public static void main(String args[]) throws IOException {


        PropertyConfigurator.configure("Logger/log4j.properties");


        /**
         *
         * This code needs a lots of file to retrieve the original tweet from the graphlab clustering:
         *
         *
         *
         */


        /**
         *
         * Loading data from graphlab output inside data structures
         *
         *
         *
         *
         */


        String clustersOutput = "/Users/luca/Desktop/tesi/tweetAnalysis/graphlabClusteringResults/clustering tanimoto server Dis/centroid.txt";

        String clusteredPoints = "/Users/luca/Desktop/tesi/tweetAnalysis/graphlabClusteringResults/clustering tanimoto server Dis/clusteredPoint.txt.1_of_1";

        String dictionaryFile = "TextualClustering/input/dictionary.file-0";


        String tfidfVector = "src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/tfidf_vector.txt";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        HashMap<Integer, String> dictionaryMap = loadDictionary(dictionaryFile, fs, conf);
        HashMap<Integer, RandomAccessSparseVector> centroidMap = new HashMap<Integer, RandomAccessSparseVector>();


        //ArrayList for saving top Feature
        int numTopWord = 15;
        ArrayList<String> topFeatureList = new ArrayList<String>();


        int numFeature = dictionaryMap.size();

        mylog.debug("DictionaryMap size: " + numFeature);

        //printing all the key value in dictionary file
//        for (int i : dictionaryMap.keySet()) {
//
//            if (i < 200)
//                mylog.debug("key: " + i + " Value:" + dictionaryMap.get(i));
//
//
//        }


        BufferedReader br = new BufferedReader(new FileReader(clustersOutput));


        String line;

        while ((line = br.readLine()) != null) {

            //diving the cluster id with the feature vector
            String token[] = line.split("\t", 2);

            int centroidNumber = Integer.parseInt(token[0]);

            mylog.debug("Centroid number: " + centroidNumber);
//            mylog.debug("Feature vector: " + token[1]);

            String featureArray[] = token[1].split(" ");


            mylog.debug("Number of feature different from zero: " + featureArray.length);


            /*

            Adesso quello che dovrei fare è utilizzare il dictionary file per vedere quante sono le feature e quali sono
            in modo da poter instanziare uno sparse vector per ogni centroide ed andargli poi ad assegnare i valori
            che estraggo dal risultato di graphlab (bisogna tenere conto delle e all'interno dei numeri)


             */


            //if the dictionary file has n record, means that each vector has n feature

            //creating a vector that represent the centroid
            RandomAccessSparseVector centroidVector = new RandomAccessSparseVector(numFeature);

            //for each value different from zero adding the value in the vector

            for (String e : featureArray) {

                String featureToken[] = e.split(":", 2); //each feature has the form numFeature:DoubleValue

                int featureIndex = Integer.parseInt(featureToken[0]);
                double featureValue = Double.valueOf(featureToken[1]);

//                //the number has exponential notation, this means that i've to use bigdecimal class
//                BigDecimal myNumber = new BigDecimal(featureToken[1]);
//
//
//                double featureValue = myNumber.doubleValue();


//                mylog.debug("Index: " + featureIndex + " doubleValue: " + featureValue);

                centroidVector.set(featureIndex, featureValue);


            }

            centroidMap.put(centroidNumber, centroidVector);


        }


        //in this way i've an hashmap with all the centroid

        int k = 1;

        for (int e : centroidMap.keySet()) {

            RandomAccessSparseVector vector = centroidMap.get(e);

            //adesso devi vedere quali sono le top feature scorrendo tutti gli atributi e vedendo quelli che hanno il
            //valore maggiore, ma nn posso usare il treeSet o il TreeMap perche c'è il problema che potri avere
            //duplicati e perdere informazioni importanti


            ArrayList<Integer> topFeature = getTopFeature(vector, numTopWord);
            line = "";


            mylog.debug("The top features for the centroid " + e + " are:");

            for (Integer i : topFeature) {

                mylog.debug("===============> " + dictionaryMap.get(i) +
                        " with value: " + vector.get(i) + " dictionary position: " + i);


                //saving the topfeature as a list
                line += "TopWord " + k + ": " + dictionaryMap.get(i) +
                        " with value: " + vector.get(i) + "\n";

                k++;

            }


            topFeatureList.add(e - 1, line);
            k = 0;


        }



            /*
            now 'ive extracted the tof feature for the centroid, now i've to get the top tweet for each centroid
            to get the top tweet i've to :

            1)read the file with the assigment of vector to cluster
            2)load the file with the relationship with incremental id and original tweetID
            2)how to get the top??? there isn't the distance in the file like in mahout impl of vector


            */


        //Starting fromm the correlation file for tweetId and incremental Id, creatings a readable clustering file


        mylog.debug("Loading the tweetIdtoIncremental file in a data structure");


        TreeMap<Integer, String> tweetIDToIncremental = new TreeMap<Integer, String>();


        br = new BufferedReader(new FileReader("src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/tweetIdtoIncremental.txt"));

        int incrementalID;


        while ((line = br.readLine()) != null) {

            String token[] = line.split(" ", 2);

                /*


                each line is like this: 469141077023522816 206237

                so after the split:

                token[0]=469141077023522816 -> tweetID
                token[1]=206237 -> IncrementalID



                 */


            incrementalID = Integer.parseInt(token[1]);


            tweetIDToIncremental.put(incrementalID, token[0]);


        }

        mylog.debug("File loaded, the data structure size is: " + tweetIDToIncremental.size());



        /*

        adesso leggo il secondo file dei risultati, in modo da sapere per ogni idincrementale, il cluster a cui appartiene
         e posso farlo in questo modo:

        Leggo il risultato del clustering dove, per ogni riga c'è l'id incrementale e il cluster di appartenza

        Il dataset iniziale si trova in :


       String sequenceFileXasmos = "DatesetXasmosSequenceFile/part-r-00000"; ed è un sequence file che contiene
       key value del tipo text() text(), quindi per riottenere i tweet iniziali mi è sufficiente fare una hashmap
       che mi colleghi l'id del tweet al suo testo




        */


        conf = new Configuration();
        fs = FileSystem.get(conf);

        String sequenceFileXasmos = "DatesetXasmosSequenceFile/part-r-00000";
        HashMap<String, String> dataset = new HashMap<String, String>();


        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(sequenceFileXasmos), conf);

        //open stream for destination file (there should be something that increment file name according to size)


        Text key = new Text();
        Text value = new Text();


        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {

            //read the seqFile

            //for each tweet getting the cluster

            String tweetId = key.toString();
            String tweetText = value.toString();

            dataset.put(tweetId, tweetText);


        }


        //closing buffer from sequence file and to vector file
        reader.close();


        mylog.debug("Original dataset with tweet loaded :" + dataset.size());


        mylog.debug("Writing clustering report: ");


        int numCentroid = centroidMap.size();
        int clusterID;
        String tweet, tweetID;
        List<List<String>> clusteringResult = new ArrayList<List<String>>(numCentroid);


        //inizializing the data structure
        for (int i = 0; i < numCentroid; i++) {


            clusteringResult.add(new ArrayList<String>());


        }


        //in clustering result, for each index there is the associated cluster id


        //writing clustering report
        BufferedWriter wr = new BufferedWriter(new FileWriter(new File("src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/ClusteringResultsk=" + numCentroid + ".txt")));


        //lettura del file del clustering, dove c'è l'id incrementale e il cluster associato
        br = new BufferedReader(new FileReader(clusteredPoints));


        while ((line = br.readLine()) != null) {


            //tab separated incrementalID    clusterID
            String token[] = line.split("\t", 2);

            clusterID = Integer.parseInt(token[1]);


            (clusteringResult.get(clusterID - 1)).add(token[0]);


        }


        br.close();


        wr.write("CLUSTERING REPORT:");
        wr.newLine();

        wr.write("There are " + numCentroid + " clusters");
        wr.newLine();


        List<String> vectorList = new ArrayList<String>();
        int j = 0;


        for (int i = 0; i < numCentroid; i++) {

            wr.write("Tweet belonging to cluster " + i);
            wr.newLine();
            line = topFeatureList.get(i);
            wr.write(line);

            wr.newLine();

            vectorList = clusteringResult.get(i);

            for (String e : vectorList) {

                //questo è l'idincrementale
                incrementalID = Integer.parseInt(e);


                //retrieving original tweetID
                tweetID = tweetIDToIncremental.get(incrementalID);

                tweet = dataset.get(tweetID);


                wr.write(tweetID + "\t" + tweet);
                wr.newLine();
                j++;

            }


        }

        //calculating percentage

        for (int i = 0; i < numCentroid; i++) {

            // j/100 = numtweet/percentage

            int numberOfTweet = clusteringResult.get(i).size();

            double percentage = (double) numberOfTweet / (j / 100);

            mylog.debug("Tweet belonging to cluster " + (i + 1) + ": " + numberOfTweet + " Percentage: " + percentage);


        }


        mylog.debug("Report File written, there were " + j + " tweet, and " + numCentroid + " centroids");


        mylog.debug("Calculating the 10 closest vector to the centroid");


        /*

        graphlab lack of a method to write the distance between the vector and the centroid, there is 2 solutions:

        1)Changing graphlab code in order to write the distance in the report file
        2)Once get the result, analyze the vector and centroid position to recaculate the distance


        */


        //the centroiMap variable store the centroid in mahout randomAccessSparse Vector format


        DistanceMeasure tanimotoMeasure = new TanimotoDistanceMeasure();
        RandomAccessSparseVector vett;
        double distance = 0;

        //reading the tfidf file


        //hashMap that stored all the tfidf vector
        HashMap<String, String> tfidfMap;


        //Each treeMap has an orderedSet of the distance of each vector to centroid
        List<TreeMap<Double, String>> distanceList = new ArrayList<TreeMap<Double, String>>();


        //initialize the data structures

        for (int i = 0; i < numCentroid; i++) {

            distanceList.add(i, new TreeMap<Double, String>());


        }


        tfidfMap = loadtfidfToHashMap(tfidfVector);
        TreeMap<Double, String> treeMap;

        mylog.debug("tfidfVectorLoaded, size " + tfidfMap.size());


        for (int e : centroidMap.keySet()) {

            mylog.debug("Calculating distance for centroid " + e);

            RandomAccessSparseVector centroidVector = centroidMap.get(e);

            //the index for centroid vector goes from 1 to 5 , in the clustering Results List  and the distance List goes from 0 to 4


            vectorList = clusteringResult.get(e - 1); //ottengo la lista di tutti gli id  dei vettori che appartengono a questo centroide

            treeMap = distanceList.get(e - 1);


            for (String s : vectorList) {


//                    mylog.debug("Calcolo Distanza tra il centroide: "+e+" ed il vettore cn id incrementale: "+s);

                String vector = tfidfMap.get(s);

//                    mylog.debug("Feature vector: "+vector);

                vett = createVectorFromString(vector, numFeature);

//                    mylog.debug(vett.toString());
//                    mylog.debug(centroidVector.toString());


                //poi calcolo la distanza e la metto in un insieme ordinato, alla fine ritorno i piu vicini

                distance = tanimotoMeasure.distance(centroidVector, vett);

//                mylog.debug("Distance: "+distance);


                //aggiungo la distanza alla treeMap


                incrementalID = Integer.parseInt(s);


                //retrieving original tweetID
                tweetID = tweetIDToIncremental.get(incrementalID);

                tweet = dataset.get(tweetID);


                treeMap.put(distance, tweet);


            }


        }

//        mylog.debug("Last Calculated Distance: " + distance + " (Should be a number from 0 to 1)");

//        mylog.debug("All distance Calculated, now I should create a treeMap to store all the distance and get the closer vector to Centroid");


        mylog.debug("Top Tweet based on Distance:");


        for (int i = 0; i < distanceList.size(); i++) {

            mylog.debug("Top Tweet Cluster: " + i);

            treeMap = distanceList.get(i);

            j = 0;

            for (double d : treeMap.keySet()) {

                if (j < 15) {
                    //printing only the first 15 element

                    mylog.debug("Distance: " + d + "Tweet: " + treeMap.get(d));

                } else {

                    break;
                }

                j++;

            }

        }

    }


    public static RandomAccessSparseVector createVectorFromString(String stringVector, int cardinality) {

        RandomAccessSparseVector vector = new RandomAccessSparseVector(cardinality); //create a randomAccessSparse Vector with cardinality equal as diciotanry size


        String token[] = stringVector.split(" "); //splitting each feature


        for (String e : token) {

            String featureToken[] = e.split(":", 2); //each feature has the form numFeature:DoubleValue

            int featureIndex = Integer.parseInt(featureToken[0]);
            double featureValue = Double.valueOf(featureToken[1]);

//                //the number has exponential notation, this means that i've to use bigdecimal class
//                BigDecimal myNumber = new BigDecimal(featureToken[1]);
//
//
//                double featureValue = myNumber.doubleValue();


//                mylog.debug("Index: " + featureIndex + " doubleValue: " + featureValue);

            vector.set(featureIndex, featureValue);


        }

        if (vector == null) {

            mylog.debug("null vector for input String: " + stringVector);

        }


        return vector;
    }


    public static HashMap<String, String> loadtfidfToHashMap(String tfidfVector) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(tfidfVector));


        HashMap<String, String> tfidfMap = new HashMap<String, String>();

        String line;

        while ((line = br.readLine()) != null) {


            String token[] = line.split(" ", 2); // split separates id from featureVector


            tfidfMap.put(token[0], token[1]);


        }


        return tfidfMap;


    }


    public static HashMap<Integer, String> loadDictionary(String dictionaryFile, FileSystem fs, Configuration conf) throws IOException {

        mylog.debug("Loading dictionary file: " + dictionaryFile);


        HashMap<Integer, String> dictionaryMap = new HashMap<Integer, String>();

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                dictionaryFile), conf);

        // provo a usare i metodi di reader per ottenre le classi ed iterare,
        // anche perchè se è un sequence file sicuramente le classi fanno parte
        // dell'ecosistema di haoop

        int i = 0; // counter for the print

        Writable key;
        Writable value;
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

            while (reader.next(key, value)) {
                dictionaryMap.put(Integer.parseInt(value.toString()), key.toString());
                i++;
            }

        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        reader.close();

        mylog.debug("Number of record read from dictionaryfile: " + i);


        return dictionaryMap;

    }

    /**
     * Given a feature feature, return a list with the index of the associated feature
     *
     * @param vector The feature vector
     * @param n      The number of top feature to return
     * @return The list with the index of n top feature
     */


    public static ArrayList<Integer> getTopFeature(RandomAccessSparseVector vector, int n) {


        ArrayList<Integer> featureList = new ArrayList<Integer>(n); //size of n


        int size = vector.size();


//            mylog.debug("Vector size"+size);

        //iterate throught the vector


        double value;
        int indexOfMax = 0;


        //iterate to get the n top value
        for (int j = 0; j < n; j++) {

//                mylog.debug("Searchin for the "+j+" max");

            double max = -1000000; //initialize the max as the first value
            int i = 0;

            while (i < size) {

                value = vector.get(i); //get the value for the feature i
                if (value > max) {

                    if (!featureList.contains(i)) {

//                           mylog.debug("The element i isn't contained, saving it as local maxi (index"+i+") ");
                        max = value;
                        indexOfMax = i;
                    }
//                        else
//                        mylog.debug("The element in position "+i+" is already contained in feature vector!!");


                }


                i++;
            }

//                mylog.debug("Global max number"+j+" index "+indexOfMax);
            //saving the index of max
            featureList.add(indexOfMax);


        }


        return featureList;

    }


}
