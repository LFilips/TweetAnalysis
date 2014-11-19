package com.luca.filipponi.tweetAnalysis.ClusterEvaluator;

import com.luca.filipponi.tweetAnalysis.Analyzer.MyAnalyzer;
import com.luca.filipponi.tweetAnalysis.SequenceFileCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by luca on 28/06/14.
 */
public class TokenEvaluator {


    //using the same logger of cluster Evaluator Class
    static Logger mylog = Logger.getLogger(ClusterEvaluator.class);


    /**
     * Questo main, tramite il dicitonary ed il frequency file mi genera due file,
     * il primo con le 100 top word del dataset e l'altro con la frequenza di ogni parola in ordine decrescente,
     * in questo modo sono in grado di valutare quali sono i token utilizzati e quali sono i piu rappresentativi
     *
     * @param args
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */


    public static void main(String args[]) throws IOException, IllegalAccessException, InstantiationException, InterruptedException, ClassNotFoundException {


        PropertyConfigurator.configure("Logger/log4j.properties");


        //using an hashMap to combine the frequency file and the dictionary file to obtain the key word
        int maxNGramSize = 3;
        // int maxNGramSize = 3;
        boolean printAll = true;
        int n = 100; //number of entry to visualize during the run, visualizing all the word can create problem 700k row!!!
        int numWord = 100;

        boolean stop = true;


        final String inputDir = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/ClusterEvaluator/ngram=" + maxNGramSize; // input directory of


        //DIRECTORY DATASET ELEZIONI GRANDE
        final String initialVectorDir = "DatesetXasmosSequenceFile";

        // of
        // starting
        // vector
        // file
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
        //conf.set("fs.default.name", "hdfs://localhost:9000");
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
		 * has to appear at least n times to be considered
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


        int minLLRValue = 750;

        /*
        This flags works only when n-grams is >1 Very significant n-gram has large scores,
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

        int reduceTasks = 1;// the number of reducer (anche questo un po a caso
        // devi vedere in base al rapporto map/reduce)
        int chunkSize = 32;// the number of chunk stored in each node (per ora
        // l'hai messo a caso vedi bene)

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


        //creo un hashmap dove salvo la coppia IdWord:Value

        HashMap<String, String> wordIdOccurrency = new HashMap<String, String>(); //saving wordID:Occurrency
        HashMap<String, String> wordOccurrency = new HashMap<String, String>(); //saving word:wordID


        // opening sequenceFIle reader

        //nella parte successiva del codice utilizzo il dictionary ed il frequency file, per andare a mappare i valori all'interno
        // di due hash map, che poi usero per creare un array di liste di stringhe, per cui l'indice dell'array
        //indica la frequenza e la lista contiene tutte le stringhe che hanno quella frequenza.

        i = 0;
        int maxFrequency = 0;
        int numFile = 10;


        Class<?> keyClass;
        Class<?> valueClass;


        for (int j = 0; j < numFile; j++) {


            if (!new File(inputDir + "/frequency.file-" + j).exists())
                break; //se il file nn esiste esce dal ciclo
            reader = new SequenceFile.Reader(fs, new Path(inputDir + "/frequency.file-" + j), conf);

            //  BufferedWriter wr = new BufferedWriter(new FileWriter("src/main/java/com/luca/filipponi/tweetAnalysis/Visualisation/topic_wordsElezioni.csv"));

            System.out.println("Reading the frequency file " + j);


            // <?> indicates that I don't know the class to be modeled, perhaps
            // I can put Class<Writable>
            keyClass = reader.getKeyClass();
            valueClass = reader.getValueClass();


            System.out.println("Key class: " + keyClass.toString()
                    + " Value class: " + valueClass.toString());

            key = (Writable) (keyClass.newInstance());
            value = (Writable) (valueClass.newInstance());


            // allocate key-vale based on the writing function
            //   wordIdOccurrency.put(key.toString(), value.toString());

            while (reader.next(key, value)) {

                if (printAll == true && i < n)
                    System.out.println("Key: " + key.toString() + " Value:"
                            + value.toString());

                if (Integer.parseInt(value.toString()) > maxFrequency)
                    maxFrequency = Integer.parseInt(value.toString());


                wordIdOccurrency.put(key.toString(), value.toString());
                i++;

            }
            System.out.println("Record Read form frequency.file-" + j + " " + i);
            i = 0;
            reader.close();

        }

        System.out.println("MaxFrequency value " + maxFrequency);


        System.out.println("A questo dovrei aver creato la prima hashmap");

        System.out.println("HashMap <idWord:freq>:\n Size:" + wordIdOccurrency.size());


        //   System.out.println("Reading some value in the first hashmap:");
       /* for (int k = 0; k < 1000; k++) {
            System.out.println("key/value pair, word id:" + k + " Frequency :" + wordIdOccurrency.get(k));

        }*/

        for (int j = 0; j < numFile; j++) {

            if (!new File(inputDir + "/dictionary.file-" + j).exists())
                break; //se il file nn esiste esce dal ciclo
            reader = new SequenceFile.Reader(fs, new Path(inputDir + "/dictionary.file-" + j), conf);

            System.out.println("Reading the dictionary file number " + j);

            // <?> indicates that I'don't know the class to be modeled, perhaps
            // I can put Class<Writable>
            keyClass = reader.getKeyClass();
            valueClass = reader.getValueClass();

            // in this way I should create an instance of the correct class
            key = (Writable) (keyClass.newInstance());
            value = (Writable) (valueClass.newInstance());

            System.out.println("Key class: " + keyClass.toString()
                    + " Value class: " + valueClass.toString());

            //      wordIdOccurrency.put(key.toString(), value.toString());

            while (reader.next(key, value)) {

                //    System.out.println("Valore: "+i+" file: " +j);


                if (printAll == true && i < n)
                    System.out.println("Key: " + key.toString() + " Value:"
                            + value.toString() + " Frequency: " + wordIdOccurrency.get(value.toString()));

                //append in the 2th hashmap to create an ordered hashmap
                wordOccurrency.put(key.toString(), wordIdOccurrency.get(value.toString()));

                //  wordIdOccurrency.put(key.toString(), value.toString());

                i++;

            }
            System.out.println("Record Read form dictionary.file-" + j + " " + i);
            i = 0;
            reader.close();
            //wr.close();

        }


        System.out.println("Creating index frequency list");

        //declaring an array of List as big as the max word frequency
        List[] indexFrequency; //the +1 is needed because the numbers starts to 1
        indexFrequency = new ArrayList[maxFrequency + 1];


        int index;
        String word;
        Iterator it = wordOccurrency.entrySet().iterator();
        while (it.hasNext()) {

            Map.Entry pairs = (Map.Entry) it.next();
            //   System.out.println(pairs.getKey() + " = " + pairs.getValue());

            index = Integer.parseInt(pairs.getValue().toString());
            word = pairs.getKey().toString();


            if (indexFrequency[index] == null) {


                indexFrequency[index] = new ArrayList<String>();
            }


            boolean add = indexFrequency[index].add(word);

        }


        String directory = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/ClusterEvaluator/ngram=" + maxNGramSize;

        if (!new File(directory).exists()) {

            new File(directory).mkdir();

        }

        String frequencyword = directory + "/FrequencyWord.txt";

        //scrivo su file la frequenza di ogni parola in ordine crescente
        BufferedWriter wr = new BufferedWriter(new FileWriter(frequencyword));

        System.out.println("Writing " + frequencyword);

        for (i = (indexFrequency.length) - 1; i > 0; i--) {

            if (indexFrequency[i] != null) {
                wr.write("Frequency: " + i + ", Word List: { ");
                // System.out.print("Frequency: " + i + ", Word List: { ");

                for (Object s : indexFrequency[i]) {

                    if (indexFrequency[i].size() > 1) {
                        wr.write(s.toString() + ", ");
                        //  System.out.print(s.toString() + ", ");
                    } else {
                        wr.write(s.toString());
                        //  System.out.print(s.toString() + ", ");
                    }
                }
                // System.out.println(" }");
                wr.write(" }\n");
            }


        }


        //crea un file csv con le top n parole del dataset
        String topWord = directory + "/top" + numWord + "Word.csv";

        wr = new BufferedWriter(new FileWriter(topWord));


        System.out.println("Writing " + topWord);
        wr.write("text,size,topic\n");


        for (i = (indexFrequency.length) - 1; i > 0; i--) {

            if (indexFrequency[i] != null) {
                if (numWord == 0)
                    break;


                for (Object s : indexFrequency[i]) {

                    if (numWord > 0) {
                        numWord--;
                        //   System.out.println("Word: " + numWord + " " + s + "," + i + ",0\n");
                        wr.write(s + "," + i + ",0\n");
                    } else
                        break;

                }


            }


        }
        wr.close();

        /**
         *
         * Piccola routine per creare un file di log con qualche informazione sui file
         *
         *
         */


        String logFile = directory + "/log.txt";

        wr = new BufferedWriter(new FileWriter(logFile));
        System.out.println("Writing log.txt");
        wr.write("File di log relativo alla directory: " + directory);
        wr.write("\n");
        wr.write("Grandezza hash map wordIdOccurrency: " + wordIdOccurrency.size());
        wr.write("\n");
        wr.write("Grandezza hash map wordOccurrency: " + wordOccurrency.size());
        wr.write("\n");
        wr.write("Max document frequency:" + maxFrequency);

        wr.close();


    }


}
