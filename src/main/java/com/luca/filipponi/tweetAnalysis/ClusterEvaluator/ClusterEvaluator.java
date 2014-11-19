package com.luca.filipponi.tweetAnalysis.ClusterEvaluator;

import com.luca.filipponi.tweetAnalysis.ClusteringDumper.ClusterPrinter;
import com.luca.filipponi.tweetAnalysis.JsonGraph.JsonGraph;
import com.luca.filipponi.tweetAnalysis.JsonGraph.JsonGraphWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.utils.clustering.AbstractClusterWriter;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.vectors.VectorHelper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

/**
 * Class for analyze the result of a clustering:
 * <p/>
 * Exporting the result into txt,csv or graphml, using Cluster Dumper.
 * <p/>
 * Then calculate the different percentage distribution of vector in cluster using a temporal sliding windows.
 * Starting analyzing one day, add 1 day each time, arriving to analyze the whole dataset.
 * <p/>
 * <p/>
 * Created by luca on 30/06/14.
 */
public class ClusterEvaluator {


    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/dbTwitter";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "";


    static Logger mylog = Logger.getLogger(ClusterEvaluator.class);


    /**
     * The main method takes as input 2 directories:
     * <p/>
     * The clusteredPoints dir, where there is all the clustered points obtained from the clustering
     * <p/>
     * The cluster-x-final, so the directroy for clustering convergence
     *
     * @param args
     */

    public static void main(String args[]) throws Exception {


        PropertyConfigurator.configure("Logger/log4j.properties");

        long maxHeapSize = Runtime.getRuntime().maxMemory();
        long freeHeapSize = Runtime.getRuntime().freeMemory();
        long totalHeapSize = Runtime.getRuntime().totalMemory();
        mylog.debug("Max Heap Size = " + (maxHeapSize / 1024) / 1024 + " Mbyte");
        mylog.debug("Free Heap Size = " + (freeHeapSize / 1024) / 1024 + " Mbyte");
        mylog.debug("Total Heap Size = " + (totalHeapSize / 1024) / 1024 + " Mbyte");


        //NUMBER OF CLUSTERS
        int k = 5;
        String distanceMeasure = "tanimoto";
        String inputDir = "TextualClustering";
        String kmeansOutputString = inputDir + "/output/k-means/k=" + k;
        String initialVectorDir = kmeansOutputString + "/clusteredPoints";


        mylog.debug("Exporting cluster with k=" + k);


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String originalVectorFile = "DatesetXasmosSequenceFile/part-r-00000";
        String dumperOutputFile; //need for more export
        ClusterDumper.OUTPUT_FORMAT format;

        //    EXPORT DI K-means
        dumperOutputFile = "consoleOutput/TextualClustering/k-means/k="
                + k + "/" + distanceMeasure + "EuropeanElection.txt";


        //text format for the first
        format = ClusterDumper.OUTPUT_FORMAT.TEXT;


        mylog.debug("Starting writing " + dumperOutputFile);


        if (!(new File(dumperOutputFile).exists())) {


            ClusterPrinter.exportWithDictionary(kmeansOutputString, inputDir
                            + "/input/dictionary.file-0", // dictionary file
                    dumperOutputFile, // file di output
                    format, // formato di output
                    "100");

        } else {

            mylog.debug("Il file è gia stato esportato precedentemente");

        }


        mylog.debug("Provo ad estrarre le top word senza dover esportare tutto");


        //faccio una lista di sortedMap in cui andrò ad inserire le distance di ogni vettore ad ogni cluster per calcolare quelli piu vicini


        List<TreeMap<Double, String>> clusterMinDist = new ArrayList<TreeMap<Double, String>>();


        mylog.debug("Extracting information from the cluster in:" + kmeansOutputString);


        File directory = new File(kmeansOutputString);
        ArrayList<String> names = new ArrayList<String>(Arrays.asList(directory.list()));
        String clusteringFinalDirectory = null;
        String pointsDirString = kmeansOutputString + "/clusteredPoints";
        //in questo modo l'array dovrebbe avere tutte le cartelle che sono dentro kmean

        for (String s : names) {

            if (s.endsWith("final")) {
                System.out.println("Selected dir: " + s);
                clusteringFinalDirectory = kmeansOutputString + "/" + s;
            }
        }

        mylog.debug("Points directory " + pointsDirString + " Clustering-final " + clusteringFinalDirectory);


        int numWord = 30;
        String topFeatureList[] = new String[k];
        String topWord;
        String dictionaryFile = inputDir + "/input/dictionary.file-0";
        String dictionary[] = VectorHelper.loadTermDictionary(conf, dictionaryFile);


        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                clusteringFinalDirectory + "/part-r-00000"), conf);


        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Writable key;
        Writable value;

        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        System.out.println("Key class: " + keyClass.toString()
                + " Value class: " + valueClass.toString());

        // allocate key-vale based on the writing function, this time is a cluster
        //the key is the cluster id and the value is a ClusterWritable


        key = (Writable) (keyClass.newInstance());
        value = (Writable) (valueClass.newInstance());

        ClusterWritable cluster = new ClusterWritable();

        //inserting all the center of cluster
        Vector centerList[] = new Vector[k];
        //inizializzo la lista pari a k


        int clusterID;
        boolean converged;
        DistanceMeasure dist = new TanimotoDistanceMeasure(); //la sto dchiarando io, ma dovrei fare in modo che usi lo stesso criterio usato nel clsuter (nel mio caso lo so che è tanimoto)

        while (reader.next(key, value)) {

            cluster = (ClusterWritable) value; //in questo modo posso accedere ai metodi di cluster writable


            //converto il cluster Writable in Cluster cosi posso accere al centro radius ecc ecc
            Cluster cl = cluster.getValue();


            clusterID = cl.getId();
            Vector center = cl.getCenter();


            String topFeature = AbstractClusterWriter.getTopFeatures(center, dictionary, numWord);

            mylog.debug("Top Feature Cluster " + clusterID + topFeature);

            topFeatureList[clusterID] = topFeature;


            converged = cl.isConverged();


            if (converged == false) {

                mylog.debug("Converged is false, this means cluster isn't converged," +
                        " or is not cluster-final or there was some problem");


            }

            mylog.debug("Id Cluster: " + clusterID + " Converged:" + cl.isConverged());

            mylog.debug("Adding the ClusterId " + clusterID + " to the list");
            centerList[clusterID] = center;


            //mylog.debug("Cluster center:"+center.toString()); //non lo stampo perchè è lunghissimo
            // mylog.debug(" Radius:"+cl.getRadius()); //anche il radius è instampabile perche troppo lungo


        }

        reader.close();


        //adesso ho una lista con tutti i centri dei cluster e ne calcolo le distance rispettive

        //controllo se la lista ha tutti gli elementi


        for (Vector e : centerList) {
            if (e == null) {

                mylog.debug("There a null centroid");

            }

        }


        TreeMap<Double, String> orderedClusterDistance = new TreeMap<Double, String>();


        for (int i = 0; i < k; i++) {

            for (int j = i + 1; j < k; j++) {

                double clDistance = dist.distance(centerList[i], centerList[j]);


                mylog.debug("Distance between Cluster " + i + " Cluster " + j + " Value: " + clDistance);
                //inserisco la distanza in un treeSet cn una stringa che mi descrive quali sono i clsuter
                orderedClusterDistance.put(clDistance, "Distance between Cluster " + i + " Cluster " + j);

            }


        }


        Set<Double> orderedDistanceSet = orderedClusterDistance.keySet(); //restituisce un insieme ordinato di chiavi

        mylog.debug("Ordered Distance List:");

        for (double d : orderedDistanceSet) {


            mylog.debug(orderedClusterDistance.get(d) + " Value: " + d);


        }


        for (int i = 0; i < k; i++)
            clusterMinDist.add(new TreeMap<Double, String>()); //initialising


        double distance;


        String line;
        String id;


        mylog.debug("Reading the clustered Points Dir, in this directory there is all the vector of the clustering");


        File vectorDir = new File(initialVectorDir);


        names = new ArrayList<String>(Arrays.asList(vectorDir.list()));
        //in questo modo l'array dovrebbe avere tutte le cartelle che sono dentro kmean
        int max = 0;
        int app = 0;

        for (String s : names) {

            if (s.startsWith("part-m-")) {
                app = Integer.parseInt(s.substring(7));
                if (app > max) {
                    max = app;

                }
            }

        }

        max++;

        mylog.debug("There is " + max + " part-m file");


        if (max > 9) {

            mylog.debug("You have more than 9 part-m file in clustered Points dir, this can lead to problem, check te code!!!");


        }


        int counter = 0;

        //need max+1
        for (int i = 0; i < max; i++) {

            String mappedVector = initialVectorDir + "/part-m-0000" + i;

            mylog.debug("Reading file: " + mappedVector);

            reader = new SequenceFile.Reader(fs, new Path(mappedVector), conf);

            int stop = 0;


            // <?> indicates that I'don't know the class to be modeled, perhaps I
            // can put Class<Writable>
            keyClass = reader.getKeyClass();
            valueClass = reader.getValueClass();

            // in this way I should create an instance of the correct class
            IntWritable vectorKey = new IntWritable();
            WeightedPropertyVectorWritable vectorValue = new WeightedPropertyVectorWritable();

//            System.out.println("Key class: " + keyClass.toString()
//                    + " Value class: " + valueClass.toString());
//            System.out.println("Key class: " + keyClass.toString()
//                    + " Value class: " + valueClass.toString());

            // allocate key-vale based on the writing function, this time is a cluster
            //the key is the cluster id and the value is a ClusterWritable


//            mylog.debug("Printing only 1 on 150k vector");



            /*

                    In this cicle, I read all the vector from the clustered point directerty,
                    inserting all the distance and the ID, in a List<TreeMap<Double,String>
                    where the element in the list is the clusterID and the treeMap, is an odered map,
                    with Distance,Tweet ID



             */


            while (reader.next(vectorKey, vectorValue)) {

                counter++;
                //ne stampo solo un po

                clusterID = vectorKey.get(); //get the id of the cluster belonging this vector

                //computing distance:


//                    dist.distance(centerList[clusterID],vectorValue.getVector());

                //ora che ho la distanza devo ottenere l'id del vettore, in modo da risalire al testo originiario

                Map<Text, Text> properties = vectorValue.getProperties();
                NamedVector named = (NamedVector) vectorValue.getVector();
                String vectorName = named.getName();


//                    mylog.debug("Vector name: "+vectorName);


                Collection<Text> coll = properties.values();
                distance = 0;

                for (Text e : coll) {


                    distance = 0.0;

                    String distanceString = e.toString();

//                        mylog.debug("Valore stringa distance: " + distanceString);

                    distance = Double.parseDouble(distanceString);
                    //dovrei gestire l'eccezione
//                        mylog.debug("Distance from centroid: " + distance);

                }


//                    mylog.debug("Vector Properties: "+properties.toString()+" Properties size: "+properties.size());


//                    mylog.debug("Vector name: "+vectorName);


                //aggiungo tutto

                clusterMinDist.get(clusterID).put(distance, vectorName);


//                    //devo accedere al centroide, prenderne il centro e calcolare la distanza
//                    mylog.debug("Distance between cluster:"+clusterID+" and Vector: "+vectorValue.getProperties().toString());
//


                stop++;
            }

            reader.close();


        }

        mylog.debug("Valore counter: " + counter);
//
//
//        while ((line = br.readLine()) != null) {
//
//
//            if (line.startsWith("VL-")) {
//
//                actualCluster = Integer.parseInt(line.charAt(3) + ""); //
//
//                //TODO this words only for cluster<10, to IMPROVE
//
//                mylog.debug("Cluster numero: " + actualCluster);
//
//
//            }
//
//            if (line.startsWith("\t1.0 : [distance=")) {

//
//                String token[] =line.split(":"); //splitto con i due punti
//
//                //a questo punto nell'indice token[1] si trova la distanza
//
//                if(line.charAt(29)=="]"){
//
//                    //significa che è il valore lungo
//
//
//
//
//                }

//
//                try {
//
//
//                    distance = Double.parseDouble(line.substring(17, 31));
//
//                    //if the above line hasn't launch an exception the distance is long
//
//                    //adding tweet id as key, and distance as value
//
//
//                    String token[] = line.split(":");
//
//                    id = token[2].substring(1, 19); //un po grossolano ma funziona
//
//
//                    clusterMinDist.get(actualCluster).put(distance, id);
//
//
//                } catch (NumberFormatException e) {
//
//
//                    //exploiting the exception, if exception has launched the number is 1 char less
//
//
//                    distance = Double.parseDouble(line.substring(17, 30));
//
//                    //if the above line hasn't launch an exception the distance is long
//
//                    //adding tweet id as key, and distance as value
//
//
//                    String token[] = line.split(":");
//
//                    id = token[2].substring(1, 19); //un po grossolano ma funziona
//
//
//                    // mylog.debug("Valore id tweet nelle eccezioni: " + id + " Valore distanza: " + distance);
//
//
//                    clusterMinDist.get(actualCluster).put(distance, id);
//
//
//                }
//
//
//            }

        //mylog.debug(distance);


//        }


        //ho letto tutto il file


        //va abbastanca bene devi controllare che vengano fuori bene gli id e stare attento che altri sono piu corti
        //e fare in modo di risalire al tweet originario, ma va bene


//        br.close();


        //reading the initial dataset, in order to obtain the text of the tweet from the id

        HashMap<String, String> table = new HashMap<String, String>();


        // opening sequenceFIle reader
        reader = new SequenceFile.Reader(fs, new Path(
                originalVectorFile), conf);


        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        keyClass = reader.getKeyClass();
        valueClass = reader.getValueClass();

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


        //at this point I should have k HashSet, where i can retrieve an ordered list of tweet id


        int actualCluster = 0;
        int totalVectorNumber = 0;

        for (TreeMap<Double, String> set : clusterMinDist) {


            mylog.debug("10 top tweet in cluster " + actualCluster + ": ");


            Set<Double> orderedDistance = set.keySet();
            totalVectorNumber += set.size(); //mi salvo quanti sono i vettori del dataser

            int j = 0;

            for (Double e : orderedDistance) {


                if (j > 10) {

                    break;
                }


                id = set.get(e);

                mylog.debug("Distance Value: " + e + " Id : " + id + " Text: " + table.get(id));

                j++;
            }


            actualCluster++;

            System.out.print("\n\n");

        }


        int sommaVettoriCluster = 0;
        for (TreeMap<Double, String> e : clusterMinDist) {
            //conto quanti vettori sono effettivamente rimasti nei cluster dopo l'avere preso solo quelli in una determinata finestra temporale


            sommaVettoriCluster += e.keySet().size();


        }

        mylog.debug("Sum of all the vector that are actually in the cluster " + sommaVettoriCluster);


        //a questo punto stampo le percentuali di vettori in ogni cluster e la distanza media

        actualCluster = 0;
        int clusterSize = 0;
        double clusterDensity = 0;
        double distanceSum = 0;
        double intraClusterDensity = 0;
        double percentage = 0.0;
        double percentageSum = 0.0;
        int vectorSum = 0;

        for (TreeMap<Double, String> map : clusterMinDist) {

            mylog.debug("Analyzing cluster:" + actualCluster);


            orderedDistanceSet = map.keySet(); // is an ordered set, I don't need it ordered, but is already populated

            for (double e : orderedDistanceSet) {

                distanceSum += e; //sommo tutte le distance


            }


            clusterSize = map.size();

            percentage = (100.0 / totalVectorNumber) * clusterSize; //trovo la percentuale di vettori in questo cluster
            percentageSum += percentage;
            vectorSum += clusterSize;
            clusterDensity = distanceSum / clusterSize;
//            mylog.debug("Total distance: "+distanceSum);
            mylog.debug("The Cluster: " + actualCluster + " has " + clusterSize + " element so the " + percentage + "% " +
                    ", and an Intra-density of: " + clusterDensity);

            intraClusterDensity += clusterDensity;

            actualCluster++;
            //azzero la distanza

            distanceSum = 0;

        }

        intraClusterDensity = intraClusterDensity / clusterMinDist.size();

        mylog.debug("The percentage sum is " + percentageSum + " and the vectorSum is: " + vectorSum + " (just to check if value are correct) ");
        mylog.debug("Total intra-cluster density: " + intraClusterDensity);

        //inizializzo dati per la creazione del json


        String jsonFile = "src/main/java/com/luca/filipponi/tweetAnalysis/D3Visualize/Graph.json";


        File file = new File(jsonFile);
        BufferedWriter wr = new BufferedWriter(new FileWriter(file));


        JsonGraph grafo = new JsonGraph();


        //cluster mindist è una lista di k treeMap


        //scorro la lista per ottenere i tweet di ogni cluster

        int nodeCentroidCounter = 0;
        int nodeVectorCounter = 0;
        int cutter = 0;
        for (int i = 0; i < k; i++) {


            grafo.addNode("Cluster" + i, i + "");


            TreeMap<Double, String> tweetIdInCluster = clusterMinDist.get(0);

            Set<Double> distanceSet = tweetIdInCluster.keySet();

            for (Double e : distanceSet) {
                cutter++;

                if (cutter % 20 == 0) {


                    //adesso ho l'id, il cluster e la distanza, quindi posso creare il nodo e l'arco

                    grafo.addNode(tweetIdInCluster.get(e) + "", i + ""); //il nome è l'id e il gruppo è il cluster
                    nodeVectorCounter++;
                    grafo.addEdge(nodeCentroidCounter + "", nodeVectorCounter + "", "1");


                }
            }


            nodeCentroidCounter = nodeVectorCounter;


        }

        mylog.debug("Nodi nel json: " + nodeVectorCounter);

        //ora scrivo il grafo in json


        JsonGraphWriter.writeJsonGraph(grafo, wr);


        mylog.debug("Evaluate the evolution of the percentage between diffent time windows");

//        mylog.debug("Exiting method, system.exit in line 702");
//        System.exit(23);


        //create an HashMap<String,String> with some different time windows

        List<String> timeWindows = new ArrayList<String>();

        /*

        nel dataset il primo e l'ultimo tweet hanno come orario rispettivamente
        2014-05-19 16:17:23 e 2014-05-30 10:17:52
        quindi come riferimento come inizio del dataset prendo

         */


        //in questo modo posso mettere finestre temporali che analizzeranno i risultatidel clustering


        String giorno1 = "2014-05-19 00:00:00";
        String giorno2 = "2014-05-20 00:00:00";
        String giorno3 = "2014-05-21 00:00:00";
        String giorno4 = "2014-05-22 00:00:00";
        String giorno5 = "2014-05-23 00:00:00";
        String giorno6 = "2014-05-24 00:00:00";
        String giorno7 = "2014-05-25 00:00:00";
        String giorno8 = "2014-05-26 00:00:00";
        String giorno9 = "2014-05-27 00:00:00";
        String giorno10 = "2014-05-28 00:00:00";
        String giorno11 = "2014-05-29 00:00:00";
        String giorno12 = "2014-05-30 00:00:00";
        String giorno13 = "2014-05-31 00:00:00";


//        //finestra temporale fino alla chiusura dei seggi:
//        timeWindows.put("2014-05-19 00:00:00", "2014-05-25 23:59:59");
//
//        //finestra temporale del risultato finale delle elezioni "Nel sito del ministero  26/05/2014 - 12:52"
//        timeWindows.put("2014-05-26 12:50:00", "2014-06-25 23:59:59"); //ho messo fino al 26 giugno cosi prende tutto
//
//        //finestra temporale per tutto il dataset
//        timeWindows.put("2013-01-01 00:00:00", "2015-01-01 00:00:00");


        //TODO dovrei aggiungere la generazione di finestre temporali in maniera automatica così è fatto male

        //giorno per giorno

        timeWindows.add(giorno1 + " " + giorno2);
        timeWindows.add(giorno2 + " " + giorno3);
        timeWindows.add(giorno3 + " " + giorno4);
        timeWindows.add(giorno4 + " " + giorno5);
        timeWindows.add(giorno5 + " " + giorno6);
        timeWindows.add(giorno6 + " " + giorno7);
        timeWindows.add(giorno7 + " " + giorno8);
        timeWindows.add(giorno8 + " " + giorno9);
        timeWindows.add(giorno9 + " " + giorno10);
        timeWindows.add(giorno10 + " " + giorno11);
        timeWindows.add(giorno11 + " " + giorno12);
        timeWindows.add(giorno12 + " " + giorno13);

        //ogni due giorni

        timeWindows.add(giorno1 + " " + giorno3);
        timeWindows.add(giorno3 + " " + giorno5);
        timeWindows.add(giorno5 + " " + giorno7);
        timeWindows.add(giorno7 + " " + giorno9);
        timeWindows.add(giorno9 + " " + giorno11);
        timeWindows.add(giorno11 + " " + giorno13);


        timeWindows.add(giorno1 + " " + giorno3);
        timeWindows.add(giorno2 + " " + giorno4);
        timeWindows.add(giorno3 + " " + giorno5);
        timeWindows.add(giorno4 + " " + giorno6);
        timeWindows.add(giorno5 + " " + giorno7);
        timeWindows.add(giorno6 + " " + giorno8);
        timeWindows.add(giorno7 + " " + giorno9);
        timeWindows.add(giorno8 + " " + giorno10);
        timeWindows.add(giorno9 + " " + giorno11);
        timeWindows.add(giorno10 + " " + giorno12);
        timeWindows.add(giorno11 + " " + giorno13);


        //ogni tre giorni

        timeWindows.add(giorno1 + " " + giorno4);
        timeWindows.add(giorno2 + " " + giorno5);
        timeWindows.add(giorno3 + " " + giorno6);
        timeWindows.add(giorno4 + " " + giorno7);
        timeWindows.add(giorno5 + " " + giorno8);
        timeWindows.add(giorno6 + " " + giorno9);
        timeWindows.add(giorno7 + " " + giorno10);
        timeWindows.add(giorno8 + " " + giorno11);
        timeWindows.add(giorno9 + " " + giorno12);
        timeWindows.add(giorno10 + " " + giorno13);


        //ogni 4 giorni

        timeWindows.add(giorno1 + " " + giorno5);
        timeWindows.add(giorno2 + " " + giorno6);
        timeWindows.add(giorno3 + " " + giorno7);
        timeWindows.add(giorno4 + " " + giorno8);
        timeWindows.add(giorno5 + " " + giorno9);
        timeWindows.add(giorno6 + " " + giorno10);
        timeWindows.add(giorno7 + " " + giorno11);
        timeWindows.add(giorno8 + " " + giorno12);
        timeWindows.add(giorno9 + " " + giorno13);

        //ogni 5 giorni

        timeWindows.add(giorno1 + " " + giorno6);
        timeWindows.add(giorno2 + " " + giorno7);
        timeWindows.add(giorno3 + " " + giorno8);
        timeWindows.add(giorno4 + " " + giorno9);
        timeWindows.add(giorno5 + " " + giorno10);
        timeWindows.add(giorno6 + " " + giorno11);
        timeWindows.add(giorno7 + " " + giorno12);
        timeWindows.add(giorno8 + " " + giorno13);


        //ogni 6 giorni

        timeWindows.add(giorno1 + " " + giorno7);
        timeWindows.add(giorno2 + " " + giorno8);
        timeWindows.add(giorno3 + " " + giorno9);
        timeWindows.add(giorno4 + " " + giorno10);
        timeWindows.add(giorno5 + " " + giorno11);
        timeWindows.add(giorno6 + " " + giorno12);
        timeWindows.add(giorno7 + " " + giorno13);

        //ogni 7 giorni

        timeWindows.add(giorno1 + " " + giorno8);
        timeWindows.add(giorno2 + " " + giorno9);
        timeWindows.add(giorno3 + " " + giorno10);
        timeWindows.add(giorno4 + " " + giorno11);
        timeWindows.add(giorno5 + " " + giorno12);
        timeWindows.add(giorno6 + " " + giorno13);

        //ogni 8 giorni

        timeWindows.add(giorno1 + " " + giorno9);
        timeWindows.add(giorno2 + " " + giorno10);
        timeWindows.add(giorno3 + " " + giorno11);
        timeWindows.add(giorno4 + " " + giorno12);
        timeWindows.add(giorno5 + " " + giorno13);

        //ogni 9 giorni

        timeWindows.add(giorno1 + " " + giorno10);
        timeWindows.add(giorno2 + " " + giorno11);
        timeWindows.add(giorno3 + " " + giorno12);
        timeWindows.add(giorno4 + " " + giorno13);

        //ogni 10 giorni

        timeWindows.add(giorno1 + " " + giorno11);
        timeWindows.add(giorno2 + " " + giorno12);
        timeWindows.add(giorno3 + " " + giorno13);

        //ogni 11 giorni

        timeWindows.add(giorno1 + " " + giorno12);
        timeWindows.add(giorno2 + " " + giorno13);

        //ogni 12 giorni (cioè tutto il dataset)

        timeWindows.add(giorno1 + " " + giorno13);


        mylog.debug("Time Windows size: " + timeWindows.size());


        List<TreeMap<Double, String>> originalClusterMinDist = new ArrayList<TreeMap<Double, String>>();


        mylog.debug("Salvo la struttura dati originale ");
        for (int i = 0; i < k; i++) {

            originalClusterMinDist.add((TreeMap<Double, String>) clusterMinDist.get(i).clone());


        }


        String dateFrom;
        String dateTo;

        File timeWindowsCsv = new File("src/main/java/com/luca/filipponi/tweetAnalysis/ClusterEvaluator/TimeWindowsk=" + k + ".csv");
//        if(!(timeWindowsCsv.exists())){
//
//            mylog.debug("TimeWindowsCsv don't exist");
//
//
//
//        }


        wr = new BufferedWriter(new FileWriter("src/main/java/com/luca/filipponi/tweetAnalysis/ClusterEvaluator/TimeWindowsk=" + k + ".csv"));

        //creo l'intestazione del csv

        wr.write("TimeWindows,");

        for (int i = 0; i < k; i++) {

            wr.write("Cluster" + i);

            if (i + 1 == k) {
                wr.write("\n");

            } else {
                wr.write(",");

            }


        }

        for (String date : timeWindows) {

            dateFrom = date.substring(0, 19);
            dateTo = date.substring(20);


            if (dateFrom == null || dateTo == null) {
                mylog.debug("No time window set !!!!");

            } else {

                System.out.println("\n\n");
                mylog.debug("The time windows analyzed is from " + dateFrom + " to " + dateTo);


            }

            HashSet<String> tweetSetBytime = getTweetByTime(dateFrom, dateTo);


            clusterMinDist = new ArrayList<TreeMap<Double, String>>();


            //Ricopio la struttura dati originale in modo da fare cn tutti i dati per ogni finestra temporale
            for (int j = 0; j < k; j++) {

                clusterMinDist.add((TreeMap<Double, String>) originalClusterMinDist.get(j).clone());


            }


            for (int i = 0; i < k; i++) {

                //svuoto clusterMinDist senno avrei i tweet residue dell'altra finestra temporale


                TreeMap<Double, String> treeMap = clusterMinDist.get(i); //non posso usare direttamente treeMap !!!


                orderedDistanceSet = treeMap.keySet();


                ArrayList<Double> elementToRemove = new ArrayList<Double>();

                for (double e : orderedDistanceSet) {

                    //accedo all'id dei tweet dei cluster sfruttando le strutture dati che ho creato prima


                    id = treeMap.get(e);
                    if (!(tweetSetBytime.contains(id))) {
                        //se nn lo contiene non si trova nel set del range temporale


                        elementToRemove.add(e);


                    }


                }
                // a questo punto rimuovo tutti gli elemeti che sono in elementToRemove

                for (double e : elementToRemove) {

                    treeMap.remove(e);

                }


            }

            sommaVettoriCluster = 0;
            for (TreeMap<Double, String> e : clusterMinDist) {
                //conto quanti vettori sono effettivamente rimasti nei cluster dopo l'avere preso solo quelli in una determinata finestra temporale


                sommaVettoriCluster += e.keySet().size();


            }

            mylog.debug("Sum of all the vector that are actually in the cluster " + sommaVettoriCluster);

            int totalVectorByTime = tweetSetBytime.size();

//        mylog.debug("Number of vector in that temporal window "+totalVectorByTime);


            percentageSum = 0.0;
            vectorSum = 0;


            wr.write(dateFrom + "/" + dateTo + ",");


            for (int i = 0; i < k; i++) {
                //gli elementi dorebbero essere diminuiti

                TreeMap<Double, String> treeMap = clusterMinDist.get(i);
                clusterSize = treeMap.keySet().size();
                vectorSum += clusterSize;
                percentage = (100.0 / sommaVettoriCluster) * clusterSize;
                percentageSum += percentage;
                mylog.debug("Number of vector contained in cluster " + i + " : " + clusterSize + " so the " + percentage + "%");

                wr.write(percentage + "");

                if (i + 1 == k) {
                    wr.write("\n");

                } else {
                    wr.write(",");

                }


            }

            mylog.debug("The percentage sum is " + percentageSum + " and the vectorSum is: " + vectorSum + " (just to check if value are correct) ");


        }


        wr.close(); //chiusa del csv dove ho esportato i csv della timewindows


    }


    public static String printTopWord(String topWord) {

        //Should add the weight of each word

        String topWordString = "";

        mylog.debug("Stringa in input al metodo di split " + topWord);

        String topWordArray[] = topWord.split("_");

        mylog.debug("Array Estratto" + topWord + " size " + topWord.length());

        for (String e : topWordArray) {

            topWordString = topWordString + e + " ";


        }


        return topWordString;


    }


    public static HashSet<String> getTweetByTime(String datefrom, String dateTo) throws ClassNotFoundException, SQLException {


        HashSet<String> recordSet = new HashSet<String>();
        String tweetID;
        //qui devo fare un query sul db in modo che mi restituisca solo i tweet che siano compresi nell'intervallo
        //di tempo che ho richiesto

        int k = 0;

        Connection conn = null;
        Statement stmt = null;


        //STEP 2: Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

        //STEP 3: Open a connection
//            System.out.println("Connecting to database...");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);

        //STEP 4: Execute a query
//            System.out.println("Creating statement...");
        stmt = conn.createStatement();
        String sql;

        //SELECT id_tweet,text,created_at from twitter where created_at > '2014-05-26 10:32:30' and created_at < '2014-05-28 10:32:30'


        sql = "SELECT id_tweet,text,created_at FROM twitter WHERE created_at >= \'" + datefrom + "\' and created_at < \'" + dateTo + "\'";


        System.out.println("Executing query: " + sql);
        ResultSet rs = stmt.executeQuery(sql);
        boolean add;

        while (rs.next()) {

            tweetID = rs.getString("id_tweet");
            add = recordSet.add(tweetID); //inserisco il tweet, è strano ma ci sono dei duplicati
            if (add == false) {

                // mylog.debug("Can't add the tweet ID "+tweetID+" There is a duplicate");

            }
            k++;
        }

        rs.close(); //close the result set


        System.out.println("Rows obtained " + k + " HashSet size :" + recordSet.size() + " (should be the same as row number if is different there was some duplicate) ");
        //STEP 6: Clean-up environment


        stmt.close();
        conn.close();


        return recordSet;


    }


}
