package com.luca.filipponi.tweetAnalysis.graphlab;

import com.luca.filipponi.tweetAnalysis.Analyzer.MyAnalyzer;
import com.luca.filipponi.tweetAnalysis.SequenceFileCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by luca on 18/08/14.
 * <p/>
 * With this class, I create the input file for graphlab.
 * According to graphlab doc, input file can be sparse vecotor with id, so I write on a txt file the result of the tfidf weighting
 */


public class graphlabInputLoader {


    static Logger mylog = Logger.getLogger(graphlabInputLoader.class);


    public static void main(String args[]) throws IOException {

        PropertyConfigurator.configure("Logger/log4j.properties");


        String dataset = "DatesetXasmosSequenceFile/part-r-00000";




        //parameters for method
        String tfidfVector = "src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/Vectors/tfidf-vectors/part-r-00000";
        String vectorForGraphlab = "src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/tfidf_vector.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        mylog.debug("Createing tfidf vector from initial dataser");

        try {


            createTFIDF(dataset, fs, conf);


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        mylog.debug("Creating tfidf vector file for graphlab, source file: " + tfidfVector + " target file");


        writeTFIDF(tfidfVector, vectorForGraphlab, fs, conf);


    }

    public static void createTFIDF(String dataset, FileSystem fs, Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {


        String inputDir = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/vectors";

        int minSupport = 5;

        int minDf = 5; // min document frequency for a word to be considered

        int maxDFPercent = 95;

        int maxNGramSize = 3;
        int minLLRValue = 750;


        int reduceTasks = 1;// the number of reducer (anche questo un po a caso
        // devi vedere in base al rapporto map/reduce)
        int chunkSize = 64;// the number of chunk stored in each node (per ora
        // l'hai messo a caso vedi bene)

        float tfNorm = -1.0f;
        boolean tfLogNormalization = false;

        float tfidfNorm = 2;
        boolean tfidfLogNormalization = true;


        boolean sequentialAccessOutput = true;

        mylog.debug("Starting all the tf-idf operation");


        mylog.debug("Creating the tokenized vector in " + inputDir + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);


        Path tokenizedPath = new Path(inputDir,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        // standard analyzer has to be changed
        // Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_48);

        Analyzer analyzer = new MyAnalyzer();

        String tokenizedDirectory = inputDir + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER;
        System.out.println("Directory tokenizzata: " + tokenizedDirectory);
        if (!new File((inputDir + "/" + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER)).exists()) {
            DocumentProcessor.tokenizeDocuments(new Path(dataset), // initial
                    // directory
                    // of
                    // vector
                    analyzer.getClass().asSubclass(Analyzer.class), tokenizedPath, // tokenized-document
                    // path
                    conf);
        } else {
            //System.out.println("tokenized document directory exist, skipping mapReduce");
            mylog.debug("tokenized document directory exist, skipping mapReduce");
        }

        // chiusura dell'analyzer anche se nn so ancora perchè
        analyzer.close();

        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER
                + "/part-m-00000", false, 100);

        // create the tf-vector and wordcount folder


        mylog.debug("Creating the tf vector");


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
        } else {
            System.out.println("tf file exist, skipping creation");
            mylog.debug("tf file exist, skipping creation");
        }

        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/tf-vectors/part-r-00000", false, 100);
        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/dictionary.file-0", false, 100);

        if (maxNGramSize == 1) {
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/part-r-00000", false, 100);
        } else {
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/ngrams/part-r-00000", false, 100);
            SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                    + "/wordcount/subgrams/part-r-00000", false, 100);
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


//            System.out.println("Calculate the Document Frequency");
        mylog.debug("Calculate the document Frequency");

        Pair<Long[], List<Path>> datasetFeature = TFIDFConverter.calculateDF(
                new Path(inputDir,
                        DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                new Path(inputDir), conf, chunkSize);

        // System.out.println("Print the pair<long[],List<Path>> datasetFeature from the calculate DF");
        mylog.debug("Print the pair<long[],List<Path>> datasetFeature from the calculate DF");
        Long[] longArray = datasetFeature.getFirst();
        List<Path> pathList = datasetFeature.getSecond();

        for (long y : longArray) {

            // System.out.println(" " + y);
            mylog.debug("NumFeature " + y);
        }

        for (Path p : pathList) {

            mylog.debug("PathList " + p.toString());
            //System.out.println(" " + p.toString());

        }

        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/df-count/part-r-00000", false, 100);
        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/frequency.file-0", false, 100);


        //  System.out.println("Calcolo i tf-idf vector");
        mylog.debug("Calcolo i tf-idf vector");
        if (!(new File(inputDir + "/tfidf-vectors").exists())) {
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
        } else {
            System.out.println("tf-idf vector file exist");
            mylog.debug("tf-idf vector file exist");
        }

        SequenceFileCreator.ReadSequenceFile(fs, conf, inputDir
                + "/tfidf-vectors/part-r-00000", false, 1000);


    }




    /**
     * Given the tfidf sequenceFile and an output file, the tfidf is write in the form need for graphlab clustering
     *
     * @param tfidfVector
     * @param outputFileName
     */

    public static void writeTFIDF(String tfidfVector, String outputFileName, FileSystem fs, Configuration conf) throws IOException {


        // prendo in input il sequencefile e la classe del key/value e stampo ad
        // output i valore

        mylog.debug("writeTFIDF method ");


        // open stream from tfidf vector
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(tfidfVector), conf);

        //open stream for destination file (there should be something that increment file name according to size)

        BufferedWriter wr = new BufferedWriter(new FileWriter(outputFileName));


        // provo a usare i metodi di reader per ottenre le classi ed iterare,
        // anche perchè se è un sequence file sicuramente le classi fanno parte
        // dell'ecosistema di haoop

        int i = 0; // counter for the print
        int w = 0; //counter for the write
        HashMap<String, String> tweetIDtoIncremental = new HashMap<String, String>();


        //the tfidf file is composed in this way, with a Text() key and a VectorWritable() value


        //Key: 468510598977044480
        // Value:468510598977044480:{13005:0.11844228593502767,20843:0.102650813155774,22350:0.10730833106264637,24960:0.11785062133132633,34085:0.07239554613440335,39207:0.12385107772039489,42097:0.1400115572309035,47136:0.12625782264733287,54687:0.1111090737961341,54718:0.12454474301532911,60439:0.120382014789553}

        /**
         *
         * The file should be like this:
         *
         * 1 0:-4.31568  3:-0.396959 5:-6.29507
         * 2 5:-4.56112  9:-1.74917
         * 3 4:4.54508 5:0.102845  10:6.35385
         * 4 0:4.87746 7:-0.832591
         *
         *
         */


        Text key = new Text();
        VectorWritable value = new VectorWritable();


        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {

            //writing on the text file

            w++;

            wr.write(w + " "); //writing the id of the tweet, now I'm using an incremental id instead of key.toString()
            tweetIDtoIncremental.put(key.toString(), w + "");


            Vector vector = value.get(); //extracting the vector


            String vectorLine = vector.toString(); //splitting this line

//                    mylog.debug("VectorLine "+vectorLine);

            String token[] = vectorLine.split(":", 2); //cosi lo divido in due

            //token[0] is the id
//                    mylog.debug("token[0] primo split"+token[0]);

            //con il replace all dovrei riuscire a togliere le parentesi graffe
            String featureToken[] = token[1].replaceAll("[{}]", "").split(",");

            for (String e : featureToken) {

                wr.write(e + " "); //writing all feature
//                        mylog.debug(e+" ");

            }

            wr.newLine();


            if (i % 50000 == 0) {

                mylog.debug("Key: " + key.toString() + " Value:"
                        + value.toString());
            }


            i++;


        }


        mylog.debug("Read " + i + " vector, Wrote " + w + " vector");


        //closing buffer from sequence file and to vector file
        reader.close();
        wr.close();


        mylog.debug("Number of tweet id " + tweetIDtoIncremental.size() + " writing on file the tweetID");

        //reopen file for writing the id

        String filename = "src/main/java/com/luca/filipponi/tweetAnalysis/graphlab/tweetIdtoIncremental.txt";

        wr = new BufferedWriter(new FileWriter(new File(filename)));


        Set<String> keySet = tweetIDtoIncremental.keySet();

        mylog.debug("Number of tweet id " + keySet.size() + " (should be the same as previously written vector");


        int counter = 0;

        for (String s : keySet) {

            counter++;
            //print on file incremtalid and tweetId
            wr.write(s + " " + tweetIDtoIncremental.get(s));

            wr.newLine();


        }

        //cloing the buffer again
        wr.close();


        mylog.debug(counter + " TweetId written ");


    }


    public static void removeDuplicates(String inputDir, FileSystem fs, Configuration conf) throws IOException {

        //hashMap where I save all the value for tf sequence File
        List<String> tfList = new ArrayList<String>();

        mylog.debug("Reading tf-vector");

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                inputDir), conf);

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
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }


    }
}