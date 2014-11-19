package com.luca.filipponi.tweetAnalysis;

import com.luca.filipponi.tweetAnalysis.Analyzer.MyAnalyzer;
import com.luca.filipponi.tweetAnalysis.ClusterEvaluator.ClusterEvaluator;
import com.luca.filipponi.tweetAnalysis.ClusteringDumper.ClusterPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.clustering.ClusterDumper.OUTPUT_FORMAT;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Class for single clustering, the number of cluster in this class is obtained from the size of the List of
 * random Vector used for starting Centroid.
 */


public class TextualClustering {


    static Logger mylog = Logger.getLogger(ClusterEvaluator.class);


    public static void main(String args[]) throws ClassNotFoundException,
            IOException, InterruptedException, InstantiationException,
            IllegalAccessException {


        PropertyConfigurator.configure("Logger/log4j.properties");


        boolean stop = true;


        final String inputDir = "TextualClustering/input"; // input directory of
        // vector for
        // clustering
        final String clusteringDir = "TextualClustering/output";

        //DIRECTORY DATASET ELEZIONI PICCOLO
        //final String initialVectorDir = inputDir + "/InitialCsvTweetVector"; // directory

        //DIRECTORY DATASET ELEZIONI GRANDE
        final String initialVectorDir = "DatesetXasmosSequenceFile";


        final String inputDataset = "input/DatasetElezioniCsvUtf-8.txt";

        Configuration conf = new Configuration();

        System.out.println(conf);
        FileSystem fs = FileSystem.get(conf);

        conf.set("mapred.map.tasks", "4"); //4 core and 4 mappers o almeno cosi dovrebbe fare
        conf.set("mapred.reduce.tasks", "4");

        System.out.println(conf.get("mapred.reduce.tasks"));
        System.out.println(conf.get("mapred.map.tasks"));

        /*
        conf.set("mapred.min.split.size", "20"); //in questo modo faccio aumentare il numero di mapper
        conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        conf.set("mapreduce.job.tracker", "localhost:54311");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "localhost:");


        System.out.println(conf.get("mapred.min.split.size"));
        System.out.println(conf.get("mapred.job.tracker"));
        System.out.println(conf.get("fs.default.name"));

        */

        int minSupport = 5;
        /*
         * the minimum frequency of the feature in the entire corpus to be
		 * considered for inclusion in the sparse vector this mean that a word
		 * has to appear at least n times to be considered, due to the short
		 * length of the tweet I've set this value to 1
		 */

        int minDf = 5; // min document frequency for a word to be considered
        int maxDFPercent = 95;

		/*
         * The df parameters is used for eliminate word that appears to much or
		 * to little in the document
		 * 
		 * the minDF if for filter the too little the other is for the too much
		 * 
		 * The max percentage of vectors for the DF. Can be used to remove
		 * really high frequency features. Expressed as an integer between 0 and
		 * 100. Default 99
		 */

        int maxNGramSize = 3;
        int minLLRValue = 750;

        /*
        This flags works only when n-grams is >1 Very significant n-gram as large scores,
         such as 1000; less significant ones have lower scores.
         Although there&rsquo;s no specific method for choosing this value,
         the rule of thumb is that n-grams with a LLR value less than 1.0 are irrelevant.
         The value of this parameters depends on the use case
         */




		/*
         * the are the value for the n-gram, that is used for correlate word
		 * togheter:
		 * 
		 * the maxNgramSize indicates the max number of gram, if for example
		 * I've the sentences:
		 * 
		 * "Hi my name is luca" the n-gram with n-gram= 2 is :
		 * 
		 * Hi my Hi name Hi is Hi luca My name My is My luca ... and so one (
		 * event the single word is considered the is always a word count step)
		 * 
		 * 
		 * the minLLRValue is the minValue of log likelihood ratio to used to
		 * prune ngrams, in this way are considered only n-gram with a ratio
		 * >minLLValue (to avoid non-sense ngram)
		 */

        int reduceTasks = 1;// the number of reducer

        int chunkSize = 64;// the number of chunk stored in each node

        float tfNorm = -1.0f;
        boolean tfLogNormalization = false;

        float tfidfNorm = 2;
        boolean tfidfLogNormalization = true;
        /*
         * The normalization parameters are very important:
		 * 
		 * in the first case, for the tfvector there is not normalization, a
		 * tweet is vectorized in this way:
		 * 
		 * id "xyz" : "Hi jhon my name is jhon"
		 * 
		 * each word has a weight equal to the number of time it appears in the
		 * content
		 * 
		 * Jhon=2 and the other =1
		 * 
		 * so the tweet will be something like id= {23:1, 56:2, 83:1, 92:1, ...}
		 * {the vector is ordered without duplicates}
		 * 
		 * this is ok when there is an tfidf step after the tf, beacuse the
		 * tfidf vectoring will ignore value less than 1
		 * 
		 * 
		 * In the case of tfidf the normalization is used, becouse with the
		 * normalization we give less weight to more shot text, that has more
		 * zero value, for a detailed description of normalizion see mahout in
		 * action (in short the normalization is in a 3 dim [x,y,z] with p-norm
		 * value
		 * 
		 * x= x/(x^p + y^p + z^p)^1/p y= y/(x^p + y^p + z^p)^1/p z= z/(x^p + y^p
		 * + z^p)^1/p
		 */

        boolean sequentialAccessOutput = true;


        if (!new File(initialVectorDir).exists()) {
            System.out
                    .println("Inital Vector doesn't exist, launching the first mapReduce from json file");
            SequenceFileCreator.createTweetFromCsvSequenceFile(inputDataset,
                    initialVectorDir);

        } else
            System.out.println("Inital Vector folder exist, skipping the creation of sequenceFIle");

        // reading the sequence File <text(),text()>
        SequenceFileCreator.ReadSequenceFile(fs, conf, initialVectorDir
                + "/part-r-00000", false, 30);

        // tokenizing the vector
        System.out.println("Creating the tokenized vector in " + inputDir + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);


        /*
        System.out.println("Eliminating Dictionary, Frequency File and wordCount Folder");

        HadoopUtil.delete(conf, new Path(inputDir
                + "/dictionary.file-0"));
        HadoopUtil.delete(conf, new Path(inputDir
                + "/frequency.file-0"));
        HadoopUtil.delete(conf, new Path(inputDir
                + "/wordcount"));
        */

        Path tokenizedPath = new Path(inputDir,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        // standard analyzer has to be changed
        // Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_48);

        Analyzer analyzer = new MyAnalyzer();

        String tokenizedDirectory = inputDir + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER;
        System.out.println("Directory tokenizzata: " + tokenizedDirectory);
        if (!new File((inputDir + "/" + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER)).exists()) {
            DocumentProcessor.tokenizeDocuments(new Path(initialVectorDir), // initial
                    // directory
                    // of
                    // vector
                    analyzer.getClass().asSubclass(Analyzer.class), tokenizedPath, // tokenized-document
                    // path
                    conf);
        } else
            System.out.println(
                    "tokenized document directory exist, skipping mapReduce");


        // chiusura dell'analyzer anche se nn so ancora perchè
        analyzer.close();

        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER
                + "/part-m-00000", false, 100);

        // create the tf-vector and wordcount folder


        if (!new File(inputDir + "/" + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER).exists()) {
            DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                    new Path(inputDir),
                    DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER, conf,
                    minSupport, maxNGramSize, minLLRValue, // minimun log-likelyhood
                    // ratio used only when
                    // n-gram > 1
                    tfNorm, // normalization value, if there is tf-idf phase after
                    // have to be -1.0f
                    tfLogNormalization, // normalization boolean,if there is tf-idf
                    // phase after have to be false
                    reduceTasks, chunkSize, sequentialAccessOutput, false); // named
            // Vector
            // da
            // vedere
        } else
            System.out.println("tf file exist");


        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/tf-vectors/part-r-00000", false, 100);
        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/dictionary.file-0", false, 100);

        if (maxNGramSize == 1) {
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/part-r-00000", false, 100);
        } else {
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/ngrams/part-r-00000", false, 30000);
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/subgrams/part-r-00000", false, 10000);
        }


        //dopo la fase di tokenizzazione, alcuni tweet di diventano uguali, quindi vanno rimossi i duplicati

        mylog.debug("Removing duplicate (-if exist) from tf");

        //hashMap where I save all the value for tf sequence File
        List<String> tfList = new ArrayList<String>();

        mylog.debug("Reading tf-vector");

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                inputDir + "/tf-vectors/part-r-00000"), conf);

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
                i++;
                //  mylog.debug("Key: " + key.toString() + " Value:" + value.toString());
                tfList.add(value.toString());
            }


            reader.close();


            mylog.debug("Number of vector: " + i + " HashMap size " + tfList.size());


            LinkedHashSet<String> set = new LinkedHashSet<String>(tfList);

            int duplicates = tfList.size() - set.size();


            mylog.debug("element in set: " + set.size() + " There is: " + duplicates + " Duplicates");

            if (duplicates > 0) {

                mylog.debug("Removing the " + duplicates + " Duplicates");


                LinkedHashSet<String> tfListNoDup = new LinkedHashSet<String>();


                mylog.debug("Creating a new tf-vectors without duplicates");


                //creating a sequence File write for the new File
                SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(
                        inputDir + "/tf-vectors(NoDup)/part-r-00000"), keyClass, valueClass);

                reader = new SequenceFile.Reader(fs, new Path(
                        inputDir + "/tf-vectors/part-r-00000"), conf);

                // provo a usare i metodi di reader per ottenre le classi ed iterare,
                // anche perchè se è un sequence file sicuramente le classi fanno parte
                // dell'ecosistema di haoop

                int read = 0; // counter for the print
                int inserted = 0;


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

                while (reader.next(key, value)) {

                    if (!(tfListNoDup.contains(value.toString()))) {
                        //write only not duplicate vec

                        inserted++;
                        writer.append(key, value);
                        tfListNoDup.add(value.toString());


                    }

                    read++;
                    /*
                    if(inserted%100000==0)
                     mylog.debug("Read: "+read+" Inserted: "+inserted+" Key: " + key.toString() + " Value:" + value.toString());
                    */

                }


                reader.close();
                writer.close();

                mylog.debug("Vector Writter in the new file: " + inserted + " Vector read from the new file" + read);


                mylog.debug("Deleting old vector file with duplicates, and renaming the new one, to preserve code compatibility");

                File oldtfFolder = new File(inputDir + "/tf-vectors");


                mylog.debug("Deleting " + oldtfFolder.toString());

                try {
                    HadoopUtil.delete(conf, new Path(oldtfFolder.toString()));
                } catch (IOException e) {

                    mylog.debug("Can't delete the tfidf folder");
                }


                File newtfFolder = new File(inputDir + "/tf-vectors(NoDup)");


                File fol = new File(inputDir + "/tf-vectors");


                mylog.debug(newtfFolder.renameTo(fol));

                mylog.debug("Iterating throught the new tf-vector folder, to be sure there isn't duplicate");


                reader = new SequenceFile.Reader(fs, new Path(
                        inputDir + "/tf-vectors/part-r-00000"), conf);

                // provo a usare i metodi di reader per ottenre le classi ed iterare,
                // anche perchè se è un sequence file sicuramente le classi fanno parte
                // dell'ecosistema di haoop

                read = 0; // counter for the print

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

                while (reader.next(key, value)) {


                    read++;

                }

            } else {

                mylog.debug("There wasn't duplicate to remove, this lead to two possibilieties:" +
                        "\n 1)There wasn't duplicate in the vector \n 2) the duplicate was removed in the previous one \n" +
                        "to check this, try to delete the vector folder to see if there is any change");


            }


        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        System.out.println("Calcolo i tf-idf vector");
        if (!(new File(inputDir + "/tfidf-vectors").exists())) {


            mylog.debug("Calculate the Document Frequency");

            Pair<Long[], List<Path>> datasetFeature = TFIDFConverter.calculateDF(
                    new Path(inputDir,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                    new Path(inputDir), conf, chunkSize);

            System.out
                    .println("Print the pair<long[],List<Path>> datasetFeature from the calculate DF");

            Long[] longArray = datasetFeature.getFirst();
            List<Path> pathList = datasetFeature.getSecond();

            for (long y : longArray) {

                System.out.println(" " + y);

            }

            for (Path p : pathList) {

                System.out.println(" " + p.toString());

            }

            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/df-count/part-r-00000", false, 100);
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/frequency.file-0", false, 100);


            TFIDFConverter.processTfIdf(new Path(inputDir,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), new Path(
                            inputDir), // output directory
                    conf, // configuration file
                    datasetFeature, // dataset frequency Pair
                    minDf, // min document frequency default value 1
                    maxDFPercent, // maxDocumentFrequency default value 99
                    tfidfNorm, // normalization value, for tf-idf this should be
                    // -1.0f otherwise >=1,
                    tfidfLogNormalization, sequentialAccessOutput, // boolean
                    // sequentialAccessOutput
                    true, // named vector if true used the documentID (in this case
                    // the tweet id)
                    reduceTasks); // numero di reducer
        } else
            System.out.println("tf-idf vector file exist");


        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/tfidf-vectors/part-r-00000", false, 1000);


        // k-means clustering
        int k; // number of clustering for kmeans clustering

        // Per i cluster la cosa che viene veramente considerata è quanti elementi
        // ci sono in questa lista e non
        // il k che viene solamente utilizzato per generare le stringhe
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(20);
        list.add(150);
        list.add(1550);
        list.add(1000);
        list.add(300);
        //list.add(2003);
        //list.add(1905);
        //list.add(1998);
        //list.add(16);
        //list.add(457);
        //list.add(991);
        //

        k = list.size();

        // creating the centroid of election dataset
        /*

        don't need to delete anymore

        HadoopUtil.delete(conf, new Path(clusteringDir + "/centroidCreator/"
                + k + "/part-00000"));
        */


        if (!(new File(inputDir + "/centroidCreator/tanimoto/" + k).exists())) {
            SequenceFileCreator.tanimotoCentroidCreator(fs, conf, k, list, inputDir
                    + "/centroidCreator/tanimoto/" + k + "/part-00000", inputDir
                    + "/tfidf-vectors/part-r-00000");
        } else
            System.out.println("The tanimote centroid for k=" + k + " exist");


        if (!(new File(inputDir + "/centroidCreator/cosine/" + k).exists())) {
            //added the cosine similatiry distance measure for Centroid
            SequenceFileCreator.cosineCentroidCreator(fs, conf, k, list, inputDir
                    + "/centroidCreator/cosine/" + k + "/part-00000", inputDir
                    + "/tfidf-vectors/part-r-00000");
        } else
            System.out.println("The cosine centroid for k=" + k + " exist");


        Path kmeansOutput = new Path(clusteringDir, "k-means/k=" + k);// directory
        // for
        // k-mean
        // clustering
        Path fuzzyKmeansOutput = new Path(clusteringDir, "fuzzyK-means/k=" + k);


        String kmeansOutputString = new String(clusteringDir + "/k-means/k=" + k);

        String fuzzyKmeansOutputString = new String(clusteringDir + "/fuzzyK-means/k=" + k);


        //TODO sarebbe da mettere un enum per non sbagliare, ma nel caso di cosineSimilarity la stringa deve essere "cosine"


        final String distanceMeasure = "cosine";



        /*
        HadoopUtil.delete(conf, kmeansOutput); // deleting the k-means
        // directory outptu

        // arrayList for centroidCreator

        // in this part i'm using a centroid file obtained writing k cluster
        // manually choosing k random vector form the input dataSet

        */
        System.out.println(conf.get("mapred.min.split.size"));


        KMeansDriver.run(conf, new Path(inputDir
                        + "/tfidf-vectors/part-r-00000"),// input-vector
                new Path(inputDir + "/centroidCreator/" + distanceMeasure + "/" + k
                        + "/part-00000"),// centroids folder
                kmeansOutput, 0.001, 20, true, 0.0, false);


        // at this point the clustering is done i've to add that takes the
        // clustering dir with the clusteredPoints and the x-final
        // and print the output


        if (stop == true) {
            //Using this variable to stop the main, for now I don't need neither export or fuzzy k-means
            System.exit(48);
        }


        HadoopUtil.delete(conf, fuzzyKmeansOutput); //deleting the fuzzykmeans outputdire with hadoop util

       /*

       public static void run(Configuration conf,
                         Path input,
                         Path clustersIn,
                         Path output,
                         double convergenceDelta,
                         int maxIterations,                 Internal src declaration
                         float m,
                         boolean runClustering,
                         boolean emitMostLikely,
                         double threshold,
                         boolean runSequential)

        maxIterations - the maximum number of iterations
        m - the fuzzification factor, see http://en.wikipedia.org/wiki/Data_clustering#Fuzzy_c-means_clustering
        runClustering - true if points are to be clustered after iterations complete
        emitMostLikely - a boolean if true emit only most likely cluster for each point
        threshold - a double threshold value emits all clusters having greater pdf (emitMostLikely = false)
        runSequential - if true run in sequential execution mode

        */


        FuzzyKMeansDriver.run(conf,
                new Path(inputDir + "/tfidf-vectors/part-r-00000"), //vector location
                new Path(inputDir + "/centroidCreator/" + distanceMeasure + "/" + k + "/part-00000"), //centroid location
                fuzzyKmeansOutput,  //fuzzy k-means clustering location
                0.001, //convergence treshold
                20, //max num iteration
                10.0f, //fuzzyness factor ??
                true,
                true, //if true emits all clusters having greater pdf
                0.0,
                false); //if sequential

        //uso il clustering dirichlet


        try {

            // in this way for each cluster i create a a txt dump with
            // topword,clsuterinfo and vector value,
            // and a txt file with the original text of each cluster (the csv
            // file is used for the converted

            String dumperOutputFile; //need for more export
            OUTPUT_FORMAT format;

            //    EXPORT DI K-means
            dumperOutputFile = "consoleOutput/TextualClustering/k-means/k="
                    + k + "/" + distanceMeasure + "ElezioniNGram3.txt";
            format = ClusterDumper.OUTPUT_FORMAT.TEXT;
            ClusterPrinter.exportWithDictionary(kmeansOutputString, inputDir
                            + "/dictionary.file-0", // dictionary file
                    dumperOutputFile, // file di output
                    format, // formato di output
                    "100");

            dumperOutputFile = "consoleOutput/TextualClustering/k-means/k=" + k
                    + "/" + distanceMeasure + "ElezioniNGram3.csv";
            format = ClusterDumper.OUTPUT_FORMAT.CSV;
            ClusterPrinter.exportWithDictionary(kmeansOutputString, inputDir
                            + "/dictionary.file-0", // dictionary file
                    dumperOutputFile, // file di output
                    format, // formato di output
                    "100");

            ClusterConverter.convert(initialVectorDir + "/part-r-00000",
                    dumperOutputFile,
                    "consoleOutput/TextualClustering/k-means/k=" + k
                            + "/" + distanceMeasure + "ElezioniConvertedNgram3.txt", false);


            dumperOutputFile = "consoleOutput/TextualClustering/fuzzyK-means/k="
                    + k + "/" + distanceMeasure + "ElezioniNGram3.txt";
            format = ClusterDumper.OUTPUT_FORMAT.TEXT;
            ClusterPrinter.exportWithDictionary(fuzzyKmeansOutputString, inputDir
                            + "/dictionary.file-0", // dictionary file
                    dumperOutputFile, // file di output
                    format, // formato di output
                    "100");

            dumperOutputFile = "consoleOutput/TextualClustering/fuzzyK-means/k=" + k
                    + "/" + distanceMeasure + "ElezioniNGram3.csv";
            format = ClusterDumper.OUTPUT_FORMAT.CSV;
            ClusterPrinter.exportWithDictionary(fuzzyKmeansOutputString, inputDir
                            + "/dictionary.file-0", // dictionary file
                    dumperOutputFile, // file di output
                    format, // formato di output
                    "100");

            ClusterConverter.convert(initialVectorDir + "/part-r-00000",
                    dumperOutputFile,
                    "consoleOutput/TextualClustering/fuzzyK-means/k=" + k
                            + "/" + distanceMeasure + "ElezioniConvertedNgram3.txt", false);


        } catch (Exception e) {

            System.out.println("There was some problem exporting the cluster");
            e.printStackTrace();
        }

    }

}
