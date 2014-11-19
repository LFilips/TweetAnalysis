package com.luca.filipponi.tweetAnalysis.SentimentClassifier;


import com.luca.filipponi.tweetAnalysis.SequenceFileCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//classifier


/**
 * Using this class to perform Naive Bayes
 * <p/>
 * <p/>
 * Created by luca on 22/07/14.
 */
public class NaiveBayesClassifier {


    public static VectorWritable vettore;
    public static Vector vectorToClassify;

    /**
     * Using the main class to write a seqFile from a training dataset and then using it for classification
     *
     * @param args
     */


    static Logger mylog = Logger.getLogger(NaiveBayesClassifier.class);


    public static Map<String, Integer> readDictionnary(Configuration conf, Path dictionnaryPath) {
        Map<String, Integer> dictionnary = new HashMap<String, Integer>();
        for (Pair<Text, IntWritable> pair : new SequenceFileIterable<Text, IntWritable>(dictionnaryPath, true, conf)) {
            dictionnary.put(pair.getFirst().toString(), pair.getSecond().get());
        }
        return dictionnary;
    }

    public static Map<Integer, Long> readDocumentFrequency(Configuration conf, Path documentFrequencyPath) {
        Map<Integer, Long> documentFrequency = new HashMap<Integer, Long>();
        for (Pair<IntWritable, LongWritable> pair : new SequenceFileIterable<IntWritable, LongWritable>(documentFrequencyPath, true, conf)) {
            documentFrequency.put(pair.getFirst().get(), pair.getSecond().get());
            System.out.println("First: " + pair.getFirst().get() + " Second:" + pair.getSecond().get());
        }
        return documentFrequency;
    }


    public static void main(String args[]) throws Exception {

        PropertyConfigurator.configure("Logger/log4j.properties");


        String trainingUnputFileName = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/SentimentDictionary.csv";
        String trainignoutputDirName = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/training";


        String testingInputFileName = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/testingSet.csv";
        String testingoutputDirName = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/testing";


        mylog.debug("Create tf-idf vectors from the trainging set, all the tweets in training are labeled");

        createTfidfWithLabel(trainingUnputFileName, trainignoutputDirName);


        mylog.debug("Create tf-idf vectors from the testing set, all the tweets in testing are unlabeled");


//        createTfidfWithoutLabel(testingInputFileName, testingoutputDirName);
//
//        mylog.debug("Vettore in memoria: "+vettore.toString());
//
//
//        /**
//         *
//         *
//         *
//         *
//         *
//         */
//
//        String modelPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model";
//        Configuration conf = new Configuration();
//
//
//
//
//        NaiveBayesModel model =NaiveBayesModel.materialize(new Path(modelPath), conf); //output path of Model
//        ComplementaryNaiveBayesClassifier classifier = new ComplementaryNaiveBayesClassifier(model);
//        Vector res=classifier.classifyFull(vettore.get());
//
//        mylog.debug("Result Vector: "+res.toString());


//
//
//            String modelPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model";
//            String labelIndexPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/labelindex";
//            String dictionaryPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/training/dictionary.file-0";
//            String documentFrequencyPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/training/frequency.file-0";
//            String tweetsPath = "/Users/luca/Desktop/tesi/tweetAnalysis/src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/testingSet.csv";
//
//            Configuration configuration = new Configuration();
//
//            // model is a matrix (wordId, labelId) => probability score
//            NaiveBayesModel model = NaiveBayesModel.materialize(new Path(modelPath), configuration);
//
//            StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);
//
//            // labels is a map label => classId
//            Map<Integer, String> labels = BayesUtils.readLabelIndex(configuration, new Path(labelIndexPath));
//            Map<String, Integer> dictionary = readDictionnary(configuration, new Path(dictionaryPath));
//            Map<Integer, Long> documentFrequency = readDocumentFrequency(configuration, new Path(documentFrequencyPath));
//
//
//            // analyzer used to extract word from tweet
//            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_48);
//
//            int labelCount = labels.size();
//
//            System.out.println("Label Count: "+labelCount+" Document count: "+documentFrequency.get(-1));
//
//
//            int documentCount = documentFrequency.get(-1).intValue();
//
//            System.out.println("Number of labels: " + labelCount);
//            System.out.println("Number of documents in training set: " + documentCount);
//            BufferedReader reader = new BufferedReader(new FileReader(tweetsPath));
//            while(true) {
//                String line = reader.readLine();
//                if (line == null) {
//                    break;
//                }
//
//                String[] tokens = line.split(",", 2);
//                String tweetId = tokens[0];
//                String tweet = tokens[1];
//
//                System.out.println("Tweet: " + tweetId + "," + tweet);
//
//                Multiset<String> words = ConcurrentHashMultiset.create();
//
//                // extract words from tweet
//                TokenStream ts = analyzer.tokenStream("text", new StringReader(tweet));
//                CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
//                ts.reset();
//                int wordCount = 0;
//                while (ts.incrementToken()) {
//                    if (termAtt.length() > 0) {
//                        String word = ts.getAttribute(CharTermAttribute.class).toString();
//                        Integer wordId = dictionary.get(word);
//                        // if the word is not in the dictionary, skip it
//                        if (wordId != null) {
//                            words.add(word);
//                            wordCount++;
//                        }
//                    }
//                }
//
//                // create vector wordId => weight using tfidf
//                Vector vector = new RandomAccessSparseVector(10000);
//                TFIDF tfidf = new TFIDF();
//                for (Multiset.Entry<String> entry:words.entrySet()) {
//                    String word = entry.getElement();
//                    int count = entry.getCount();
//                    Integer wordId = dictionary.get(word);
//                    Long freq = documentFrequency.get(wordId);
//                    double tfIdfValue = tfidf.calculate(count, freq.intValue(), wordCount, documentCount);
//                    vector.setQuick(wordId, tfIdfValue);
//                }
//                // With the classifier, we get one score for each label
//                // The label with the highest score is the one the tweet is more likely to
//                // be associated to
//                Vector resultVector = classifier.classifyFull(vector);
//                double bestScore = -Double.MAX_VALUE;
//                int bestCategoryId = -1;
//                for(Element element: resultVector.all()) {
//                    int categoryId = element.index();
//                    double score = element.get();
//                    if (score > bestScore) {
//                        bestScore = score;
//                        bestCategoryId = categoryId;
//                    }
//                    System.out.print("  " + labels.get(categoryId) + ": " + score);
//                }
//                System.out.println(" => " + labels.get(bestCategoryId));
//            }
//            analyzer.close();
//            reader.close();
//
//
//
//

        //TODO CODES from Vaibhav Srivastava

        /*

        Tecnicamente adessoè sufficienete usare la classe run del classifier avendo tolto quelle righe, in modo
        che non ci sia il problema del label


         */


        CustomTestNaiveBayesDriver bayesianTester = new CustomTestNaiveBayesDriver();


        //per creare il modello direttamente da codice

        TrainNaiveBayesJob classifierTrainer = new TrainNaiveBayesJob();


        //TODO sistemare le opzioni per l'avvio del trainer

        mylog.debug("Creating the model");


        String[] option = {
        };


        classifierTrainer.run(option);


        /**


         Usage:
         [--input <input> --output <output> --overwrite --overwrite --model <model>
         --testComplementary --runSequential --labelIndex <labelIndex> --help --tempDir
         <tempDir> --startPhase <startPhase> --endPhase <endPhase>]

         Job-Specific Options:

         --input (-i) input              Path to job input directory.

         --output (-o) output            The directory pathname for output.

         --overwrite (-ow)               If present, overwrite the output directory
         before running job

         --overwrite (-ow)               If present, overwrite the output directory
         before running job

         --model (-m) model              The path to the model built during training

         --testComplementary (-c)        test complementary?

         --runSequential (-seq)          run sequential?

         --labelIndex (-l) labelIndex    The path to the location of the label index

         --help (-h)                     Print out help

         --tempDir tempDir               Intermediate output directory

         --startPhase startPhase         First phase to run

         --endPhase endPhase             Last phase to run


         **/


        //aggiungo le opzioni che mi servono all'array seguendo l'ordine


        //cerca da solo il modello dentro la cartella
        String model = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model";
        String tfidfTest = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/training/tfidf-vectors";
        String labelIndex = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model/labelindex";
        String outputFolder = "src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/BayesianClassifierOutput";

/**
 *
 * ./mahout testnb -i cartellaAiVettori -m model -l labelindex -ow -o trainingVectorTest-result -c
 *
 *


 *
 */


//        String[] option = new String[]{
//                buildOption(DefaultOptionCreator.INPUT_OPTION),
//                tfidfTest,
//                buildOption(DefaultOptionCreator.OVERWRITE_OPTION),
//                buildOption(DefaultOptionCreator.OUTPUT_OPTION),
//                outputFolder,
//                buildOption("model"), //non so quale sia la macro per quest'opzione
//                model,
//                buildOption("labelindex"),
//                labelIndex,
////                buildOption(ClusterDumper.DICTIONARY_OPTION), //option for the dictionary
////                dictionaryFile,
////                buildOption(ClusterDumper.NUM_WORDS_OPTION), topTerms,// dovrebbe essere le prima n top words
////                // buildOption(ClusterDumper.SAMPLE_POINTS), "30",
////                buildOption(ClusterDumper.DICTIONARY_TYPE_OPTION),
////                "sequencefile", buildOption(ClusterDumper.EVALUATE_CLUSTERS)
//
//
//                };

//
//        String[] option={
//                "--input","src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/training/tfidf-vectors",
//                "--output","src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/BayesianClassifierOutput",
//                "--overwrite",
////                "--overwrite",
//                "--model","src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model",
//                "--testComplementary",
//                "--labelIndex","src/main/java/com/luca/filipponi/tweetAnalysis/SentimentClassifier/model/labelindex",
//                };
//
//
//        //print the option
//        System.out.println("String passed to CustomTestNaiveBayes.run: ");
//        for (String e : option) {
//            System.out.print(e + " ");
//        }
//        System.out.println();
//
//
////        bayesianTester.run(option);
//
//
//        System.out.println("Creating configuration file");
        Configuration conf = new Configuration();


//        System.out.println("Creating fs object");
        FileSystem fs = FileSystem.get(conf);
//
//        ToolRunner.run(conf, new CustomTestNaiveBayesDriver(), option);
//
//        //adesso vado ad analizzare il risultato
//
//
//
//            SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
//                    outputFolder+"/part-m-00000"), conf);
//
//            // provo a usare i metodi di reader per ottenre le classi ed iterare,
//            // anche perchè se è un sequence file sicuramente le classi fanno parte
//            // dell'ecosistema di haoop
//
//            int i = 0; // counter for the print
//            int n= 10000; //max number of record to print
//            Writable key;
//            Writable value;
//
//            try {
//                // <?> indicates that I'don't know the class to be modeled, perhaps
//                // I can put Class<Writable>
//                Class<?> keyClass = reader.getKeyClass();
//                Class<?> valueClass = reader.getValueClass();
//
//                // in this way I should create an instance of the correct class
//                key = (Writable) (keyClass.newInstance());
//                value = (Writable) (valueClass.newInstance());
//
//                System.out.println("Key class: " + keyClass.toString()
//                        + " Value class: " + valueClass.toString());
//
//                // allocate key-vale based on the writing function
//
//                while (reader.next(key, value)) {
//                    System.out.println("Key: " + key.toString() + " Value:"
//                            + value.toString());
//                    i++;
//                    if ( i > n)
//                        break; // exit from the cicle
//                }
//
//            } catch (InstantiationException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//            reader.close();
//

        mylog.debug("Inizia la classificazione dei tweet a partire dal modello creato precedentemente");


        NaiveBayesModel Sentimentmodel = NaiveBayesModel.materialize(new Path(model), new Configuration()); //output path of Model
        ComplementaryNaiveBayesClassifier classifier = new ComplementaryNaiveBayesClassifier(Sentimentmodel);

        /** MI prendo un vettore dai tfidf che ho creato **/


        SequenceFile.Reader sfreader = new SequenceFile.Reader(fs, new Path(tfidfTest
                + "/part-r-00000"), new Configuration());

        // provo a usare i metodi di reader per ottenre le classi ed iterare,
        // anche perchè se è un sequence file sicuramente le classi fanno parte
        // dell'ecosistema di haoop

        int i = 0; // counter for the print
        double max;
        double actualValue;
        int label = 0;
        Writable key2;
        VectorWritable value2;
        try {
            // <?> indicates that I'don't know the class to be modeled, perhaps
            // I can put Class<Writable>
            Class<?> keyClass = sfreader.getKeyClass();
            Class<?> valueClass = sfreader.getValueClass();

            // in this way I should create an instance of the correct class
            key2 = (Writable) (keyClass.newInstance());
            value2 = new VectorWritable();

//            System.out.println("Key class: " + keyClass.toString()
//                    + " Value class: " + valueClass.toString());

            // allocate key-vale based on the writing function

            while (sfreader.next(key2, value2)) {
//                System.out.println("Key: " + key2.toString() + " Value:"
//                        + value2.toString());
                vectorToClassify = value2.get(); //prendo il vettore dalla classe vector writable

                mylog.debug("Grandezza del vettore di input: " + vectorToClassify.size());
                mylog.debug("Vettore da classificare:" + vectorToClassify.toString());

                Vector probabilities = classifier.classifyFull(vectorToClassify); // this returns A vector of probabilities in 1 of n-1 encoding for your label. input will be the vector    {1:0.19424138174284086,24:0.19424138174284086,25:0.1810660431557166,44:0.19424138174284086,78:0.19424138174284086}

                mylog.debug("Grandezza del vettore delle probabilità complementari: " + probabilities.size());
                mylog.debug("Vettore delle probabilità complementari: " + probabilities.toString());

                //dovrei stampare la classe in base ai valori

                max = probabilities.get(0);

                for (int k = 0; k < probabilities.size(); k++) {

                    actualValue = probabilities.get(k);

                    if (actualValue > max) {
                        //cambio del massimo
                        max = actualValue;
                        label = k;


                    }


                }


                switch (label) {     //dovrei fare il check del label da solo in modo da farmi la confusion matrix

                    case 0:
                        mylog.debug("Negativo");
                        break;
                    case 1:
                        mylog.debug("Neutrale");
                        break;
                    case 2:
                        mylog.debug("Positivo");
                        break;


                }


            }


            //dopo aver testato il corretto funzionamento sul training set uso dei vettori nuovi, che utilizzano parole prima presenti
            //nel dizionario e poi no, per vedere come si comportano,





        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        sfreader.close();











    }


    /**
     * This method creates tfidf-vector with label starting from a csv file with the form:
     * <p/>
     * <p/>
     * neutrale,471685156584292353,@beppe_grillo intanto .. Piangi tu ... Per adesso io rido !!!!!
     * negativo,471685165635629056,RT @Kotiomkin: Incontro in volo tra Grillo e Salvini. Poi uno pranza con Farange e l'altro con Le Pen, mentre Renzi vede la Merkel. Che pal…
     * positivo,471685170698149888,RT @carlucci_cc: @valy_s renzie si preoccupa di chi gli garantisce voti...ma stanno scoprendo il prezzo di quei fottutissimi #80euro dagli …
     * neutrale,471685174426886144,Di #elezioni, di venditori di fumo e di altre schifezze... http://t.co/euFbtP7hQ1 … #Europee2014 via @ClashCityWorker
     * positivo,471685180873539584,@PaolaTavernaM5S Grazie x quello ke avete fatto e farete ancora ! By iscritto 5S
     * neutrale,471996242055409664,Selezionare un #EsploratoreModerno (tipo uno dei ruoli più belli al mondo) è in realtà un lavoro durissimo! @ExpediaIT
     *
     * @param inputFileName Path to csv File
     * @param outputDirName OutputDirectory for tf-idf vector
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */


    public static void createTfidfWithLabel(String inputFileName, String outputDirName) throws InterruptedException, IOException, ClassNotFoundException {


        Path path = new Path(outputDirName + "/InitialSeqFile/part-m-00000");
        System.out.println("Creating configuration file");
        Configuration conf = new Configuration();


        System.out.println("Creating fs object");
        FileSystem fs = FileSystem.get(conf);

        //creo un sequenceFile con coppia Chiave-Valore long-text (idtweet e contenuto)
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);

        int count = 0;
        BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
        Text key = new Text();
        Text value = new Text();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String[] tokens = line.split(",", 3);
            if (tokens.length != 3) {
                System.out.println("Skip line: " + line);
                System.out.println("Token size" + tokens.length);


                continue;
            }
//            String category = tokens[0];
//            String id = tokens[1];
//            String message = tokens[2];
            String label = tokens[0];
            String id = tokens[1];
            String message = tokens[2];


            //in base al sorgente,mahout parsa i label facendo split("/");
            key.set("/" + label + "/" + id);
            value.set(message);
            writer.append(key, value);
            count++;


//                String[] tokens = line.split(",", 2);
//                if (tokens.length != 2) {
//                    System.out.println("Skip line: " + line);
//                    System.out.println("Token size" + tokens.length);
//
//
//                    continue;
//                }
//                String category = tokens[0];
//                String id = tokens[1];
//                String message = tokens[1];
//                key.set("/" + category + "/" + id);
//                key.set(category);
//                value.set(message);
//                writer.append(key, value);
//                count++;


        }
        writer.close();
        System.out.println("Wrote " + count + " entries.");


        int minSupport = 1;

        int minDf = 1; // min document frequency for a word to be considered
        int maxDFPercent = 99;


        int maxNGramSize = 3;
        int minLLRValue = 750;


        int reduceTasks = 1;// the number of reducer

        int chunkSize = 64;// the number of chunk stored in each node

        float tfNorm = -1.0f;
        boolean tfLogNormalization = false;

        float tfidfNorm = 2;
        boolean tfidfLogNormalization = true;


        boolean sequentialAccessOutput = true;


        Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_48);

        String tokenizedDirectory = outputDirName + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER;
        Path tokenizedPath = new Path(outputDirName,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        System.out.println("Directory tokenizzata: " + tokenizedDirectory);
        if (!new File((outputDirName + "/" + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER)).exists()) {
            DocumentProcessor.tokenizeDocuments(new Path(outputDirName + "/InitialSeqFile"), // initial
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

        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER
                + "/part-m-00000", false, 100);

        // create the tf-vector and wordcount folder


        if (!new File(outputDirName + "/" + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER).exists()) {
            DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                    new Path(outputDirName),
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


        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/tf-vectors/part-r-00000", false, 100);
        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/dictionary.file-0", false, 100);

        if (maxNGramSize == 1) {
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/part-r-00000", false, 100);
        } else {
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/ngrams/part-r-00000", false, 30000);
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/subgrams/part-r-00000", false, 10000);
        }


        System.out.println("Calcolo i tf-idf vector");
        if (!(new File(outputDirName + "/tfidf-vectors").exists())) {


            mylog.debug("Calculate the Document Frequency");

            Pair<Long[], List<Path>> datasetFeature = TFIDFConverter.calculateDF(
                    new Path(outputDirName,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                    new Path(outputDirName), conf, chunkSize);

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

            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/df-count/part-r-00000", false, 100);
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/frequency.file-0", false, 100);


            TFIDFConverter.processTfIdf(new Path(outputDirName,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), new Path(
                            outputDirName), // output directory
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


        mylog.debug("Reading tf-idf");

        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/tfidf-vectors/part-r-00000", false, 100);


        //a questo punto dovrei fare la fase di training


    }


    public static void createTfidfWithoutLabel(String inputFileName, String outputDirName) throws InterruptedException, IOException, ClassNotFoundException {


        Path path = new Path(outputDirName + "/InitialSeqFile/part-m-00000");
        System.out.println("Creating configuration file");
        Configuration conf = new Configuration();


        System.out.println("Creating fs object");
        FileSystem fs = FileSystem.get(conf);

        //creo un sequenceFile con coppia Chiave-Valore long-text (idtweet e contenuto)
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);

        int count = 0;
        BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
        Text key = new Text();
        Text value = new Text();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String[] tokens = line.split(",", 2);
            if (tokens.length != 2) {
                System.out.println("Skip line: " + line);
                System.out.println("Token size" + tokens.length);


                continue;
            }
            String id = tokens[0];
            String message = tokens[1];
            key.set("/" + id + "/"); //scrivo in questo modo per avere un label esterno
            value.set(message);
            writer.append(key, value);
            count++;


//                String[] tokens = line.split(",", 2);
//                if (tokens.length != 2) {
//                    System.out.println("Skip line: " + line);
//                    System.out.println("Token size" + tokens.length);
//
//
//                    continue;
//                }
//                String category = tokens[0];
//                String id = tokens[1];
//                String message = tokens[1];
//                key.set("/" + category + "/" + id);
//                key.set(category);
//                value.set(message);
//                writer.append(key, value);
//                count++;


        }
        writer.close();
        System.out.println("Wrote " + count + " entries.");


        int minSupport = 1;

        int minDf = 1; // min document frequency for a word to be considered
        int maxDFPercent = 99;


        int maxNGramSize = 3;
        int minLLRValue = 750;


        int reduceTasks = 1;// the number of reducer

        int chunkSize = 64;// the number of chunk stored in each node

        float tfNorm = -1.0f;
        boolean tfLogNormalization = false;

        float tfidfNorm = 2;
        boolean tfidfLogNormalization = true;


        boolean sequentialAccessOutput = true;


        Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_48);

        String tokenizedDirectory = outputDirName + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER;
        Path tokenizedPath = new Path(outputDirName,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        System.out.println("Directory tokenizzata: " + tokenizedDirectory);
        if (!new File((outputDirName + "/" + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER)).exists()) {
            DocumentProcessor.tokenizeDocuments(new Path(outputDirName + "/InitialSeqFile"), // initial
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

        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName + "/"
                + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER
                + "/part-m-00000", false, 100);

        // create the tf-vector and wordcount folder


        if (!new File(outputDirName + "/" + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER).exists()) {
            DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                    new Path(outputDirName),
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


        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/tf-vectors/part-r-00000", false, 100);
        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/dictionary.file-0", false, 100);

        if (maxNGramSize == 1) {
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/part-r-00000", false, 100);
        } else {
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/ngrams/part-r-00000", false, 30000);
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/wordcount/subgrams/part-r-00000", false, 10000);
        }


        System.out.println("Calcolo i tf-idf vector");
        if (!(new File(outputDirName + "/tfidf-vectors").exists())) {


            mylog.debug("Calculate the Document Frequency");

            Pair<Long[], List<Path>> datasetFeature = TFIDFConverter.calculateDF(
                    new Path(outputDirName,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                    new Path(outputDirName), conf, chunkSize);

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

            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/df-count/part-r-00000", false, 100);
            SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                    + "/frequency.file-0", false, 100);


            TFIDFConverter.processTfIdf(new Path(outputDirName,
                            DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), new Path(
                            outputDirName), // output directory
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


        mylog.debug("Reading tf-idf");

        SequenceFileCreator.ReadSequenceFile(fs, conf, outputDirName
                + "/tfidf-vectors/part-r-00000", false, 100);


        //a questo punto dovrei fare la fase di training


        System.out.print("ReadSequenceFile method, reading ");

        // opening sequenceFIle reader
        SequenceFile.Reader sfreader = new SequenceFile.Reader(fs, new Path(outputDirName
                + "/tfidf-vectors/part-r-00000"), conf);

        // provo a usare i metodi di reader per ottenre le classi ed iterare,
        // anche perchè se è un sequence file sicuramente le classi fanno parte
        // dell'ecosistema di haoop

        int i = 0; // counter for the print

        Writable key2;
        Writable value2;
        try {
            // <?> indicates that I'don't know the class to be modeled, perhaps
            // I can put Class<Writable>
            Class<?> keyClass = sfreader.getKeyClass();
            Class<?> valueClass = sfreader.getValueClass();

            // in this way I should create an instance of the correct class
            key2 = (Writable) (keyClass.newInstance());
            value2 = (Writable) (valueClass.newInstance());

            System.out.println("Key class: " + keyClass.toString()
                    + " Value class: " + valueClass.toString());

            // allocate key-vale based on the writing function

            while (sfreader.next(key2, value2)) {
                System.out.println("Key: " + key2.toString() + " Value:"
                        + value2.toString());
                i++;
                if (i == 5) {

                    vettore = (VectorWritable) value2;


                }
            }

        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        sfreader.close();


    }


    /**
     * Appends 2 dashes to an option name.
     *
     * @param option The option to which the two dashes are appended.
     * @return An option with two dashes appended.
     */

    private static final String buildOption(final String option) {
        return String.format("--%s", option);

    }


}







