package com.luca.filipponi.tweetAnalysis;

import com.luca.filipponi.tweetAnalysis.ClusteringDumper.ClusterPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.util.Version;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

/**
 * Class for cleaning the text of the tweet, not used anymore, but in this class there is the code for canopies clustering
 * that can be used.
 *
 * @author luca
 */
public class Cleaner {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        //per adesso leggo dal dataset regolarmente e cerco di fare un metodo per la pulizia
        // dei tweet


        System.out.println("Tweet Cleaning");


        int printCounter = 100; //variable used for the limit of print for debug

        //nome del sequenceFile
        final String outputDir = "TweetClusters";
        final String sequenceFilePoint = "tweetClusters/initialVector/file1";
        //counter per i vettori nel sequenceFile

        int minSupport = 1;
        /*
         * the minimum frequency of the feature in the entire corpus to be considered for inclusion in the sparse vector
         * this mean that a word has to appear at least n times to be considered, due to the short length of the tweet
         * I've set this value to 1
         * 
         */


        int minDf = 1; //min document frequency for a word to be considered
        int maxDFPercent = 99;
        
        /*
         * The df parameters is used for eliminate word that appears to much or to little in the document
         * 
         * the minDF if for filter the too little the other is for the too much
         * 
         *  The max percentage of vectors for the DF. 
        Can be used to remove really high frequency features. Expressed as an integer between 0 and 100. Default 99
         * 
         * 
         */


        int maxNGramSize = 1; //era messo a due ma per il mio caso è meglio lasciare a uno
        int minLLRValue = 50;//era messo a 50, dovrebbe essere usato solo nel caso di n-gram > 1 
        
        /*
         * the are the value for the n-gram, that is used for correlate word togheter:
         * 
         * the maxNgramSize indicates the max number of gram, if for example I've the sentences:
         * 
         * 	"Hi my name is luca" the n-gram with n-gram= 2 is :
         * 
         * 	Hi my	Hi name		Hi is	Hi luca
         * 	My name	  My is		My luca ... and so one ( event the single word is considered the is always a word count step)	
         * 
         * 
         * the minLLRValue is the minValue of log likelihood ratio to used to prune ngrams, in this way are considered
         * only n-gram with a ratio >minLLValue (to avoid non-sense ngram)
         * 
         */


        int reduceTasks = 1;// the number of reducer (anche questo un po a caso devi vedere in base al rapposto map/reduce)
        int chunkSize = 1000;//the number of chunk stored in each node (per ora l'hai messo a caso vedi bene)


        float tfNorm = -1.0f;
        boolean tfLogNormalization = false;
        float tfidfNorm = 2;
        boolean tfidfLogNormalization = true;
        /*
         * The normalization parameters are very important:
         * 
         * in the first case, for the tfvector there is not normalization,  a tweet is vectorized in this way:
         * 
         * id "xyz" : "Hi jhon my name is jhon"
         * 
         * each word has a weight equal to the number of time it appears in the content
         * 
         * Jhon=2 and the other =1
         * 
         * so the tweet will be something like  id= {23:1, 56:2, 83:1, 92:1, ...} {the vector is ordered without duplicates}
         * 
         * this is ok when there is an tfidf step after the tf, beacuse the tfidf vectoring will ignore value less than 1
         * 
         * 
         * In the case of tfidf the normalization is used, becouse with the normalization we give less weight to more shot text,
         * that has more zero value, for a detailed description of normalizion see mahout in action
         * (in short the normalization is in a 3 dim [x,y,z] with p-norm value
         * 
         * 		x= x/(x^p + y^p + z^p)^1/p
         * 		y= y/(x^p + y^p + z^p)^1/p
         *		z= z/(x^p + y^p + z^p)^1/p
         * 
         * 
         * 
         * 
         */


        boolean sequentialAccessOutput = true;


        //k value for the k-means clustering
        int k = 3;
        int[] randVect = new int[k];
        //arrayList for the random centroid of k-means clustering
        ArrayList<Vector> centroid = new ArrayList<Vector>();
        
        /*
         * Using java random method to create a random number between zero and a max value, using this
         * k value for choosing the centroid (in this way each time there a different result !!!)
         * 
         */


        //for now I use these number beacause using random cahnge always the cluster 12935 9430 19302

        randVect[0] = 12935;
        randVect[1] = 9430;
        randVect[2] = 19302;
        
        
        /*
        Random rand=new Random();
        for(int i=0;i<k; i++)
        		randVect[i]=rand.nextInt(30000);
        */


        System.out.println("Random value for the centoid start");
        for (int i : randVect)
            System.out.print(i + " ");

        System.out.println();//blank line


        int exitCounter = 0; //used for debug print
        int writeCounter = 0; //variable used for count the number of key written in a sequenceFIle


        Path path = new Path(sequenceFilePoint);
        System.out.println("Creating configuration file");
        Configuration conf = new Configuration();    //import org.apache.hadoop.conf.Configuration;
        //oltre all'import ho dovuto aggiungere la librearia di logging 1.1.1 al progetto
        System.out.println("Creating fs object");
        FileSystem fs = FileSystem.get(conf);// import org.apache.hadoop.fs.FileSystem; vedi se vanno bene questi due import


        //creo un sequenceFile con coppia Chiave-Valore long-text (idtweet e contenuto)
        SequenceFile.Writer tweetWriter = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);


        System.out.println("Write key/value pair in a sequence file (tweetID/TweetText)");

        //open bufferedReader to file
        try {
            BufferedReader br = new BufferedReader(new FileReader("input/DomenicaCompleto.txt"));


            String sCurrentLine;


            try {
                while ((sCurrentLine = br.readLine()) != null) {

                    //skip the blank line
                    if (!(sCurrentLine.equals(""))) {


                        JSONObject tweetJson = new JSONObject(sCurrentLine);
                        //can throws json exception should catch, and continue executing


                        String tweetText = tweetJson.getString("text");
                        String tweetID = tweetJson.getString("id_str");

						/*
                         *sono dentro un ciclo che itera su tutti i tweet
						 *dato che ho gia inizializzato il writer del sequenceFile vado ad appendergli
						 *i valore del tweetId e del testo, la creazione del testo non sono
						 *sicuro che vada ene fatta in questo modo, dovrei controllare le javadoc
						 *di hadoop io o andare a stampare il contenuto
						 */


                        tweetWriter.append(new Text(tweetID), new Text(tweetText));
                        writeCounter++;


                    }
                }
                //chiusura del writer del sequenceFile
                tweetWriter.close();
                //a questo punto ho trasferito tutte i tweet in un sequence file e lo vado a leggere
                System.out.println("Tweet written in tweetClusters/points/file1,"
                        + " <key,value> <text(),text()> <tweetid,TweetContent>, Key/value pair:" + writeCounter);

                System.out.println("Reading tweetClusters/points/file1");

                Text tweetText = new Text();
                Text tweetID = new Text();

                SequenceFile.Reader tweetReader = new SequenceFile.Reader(fs, path, conf);

                //qui ho messo il printCounter direttamtne per uscre dal ciclo che senno ci vuole na vita
                while (tweetReader.next(tweetID, tweetText) && exitCounter < printCounter) {

                    System.out.println("TweetID: " + tweetID.toString() + " TweetText: " + tweetText.toString());
                    exitCounter++;

                }

                tweetReader.close();

                System.out.println("Now i've to create vector for each tweet in tweetClusters/points/file1");


                //Now i've to create a token for each word, per adesso ho il percorso del sequence file
                //salvato in path
                System.out.println("Creating the tokenized vector in "
                        + outputDir + DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);


                Path tokenizedPath = new Path(outputDir,
                        DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

                //very simple analyzer
                Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_48);

                DocumentProcessor.tokenizeDocuments(path, analyzer.getClass()
                        .asSubclass(Analyzer.class), tokenizedPath, conf);

                //chiusura dell'analyzer anche se nn so ancora perchè
                analyzer.close();
                System.out.println("Lettura delle tuple scritte dal tokenizer <text(),StringTuple()>");

                exitCounter = 0;

                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/tokenized-documents/part-m-00000"), conf);

                StringTuple tupla = new StringTuple();
                //sono text,text, alle brutte mi da l'
                while (tweetReader.next(tweetID, tupla)) {

                    if (exitCounter < printCounter) {
                        System.out.println("TweetID: " + tweetID.toString() + " TweetText: " + tupla.toString());
                    }

                    exitCounter++;

                }

                tweetReader.close();
                System.out.println("The TweetClusters/tokenized-documents/part-m-00000 file has"
                        + exitCounter + " key/Value pair");

                System.out.println("Creating the tf-vector and the dictionary file");

                //vedi bene tutti i parametri che potrebbe essere anche qui il problema
                DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                        new Path(outputDir),
                        DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
                        conf,
                        minSupport,
                        maxNGramSize,
                        minLLRValue, //minimun log-likelyhood ratio used only when n-gram > 1
                        tfNorm, //normalization value, if there is tfidf phase after have to be -1.0f
                        tfLogNormalization, //normalization bool,if there is tfidf phase after have to be false
                        reduceTasks,
                        chunkSize,
                        sequentialAccessOutput,
                        false); // named Vector


                System.out.println("Lettura del termFrequency vector in TweetClusters/tf-vectors/part-r-00000"
                        + " <Text(),VectorWritable>");

                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/tf-vectors/part-r-00000"), conf);

                VectorWritable vect = new VectorWritable();
                exitCounter = 0;
                while (tweetReader.next(tweetID, vect)) {

                    if (exitCounter < printCounter) {
                        System.out.println("TweetID: " + tweetID.toString() + " TweetText: " + vect.toString());
                    }

                    exitCounter++;

                }
                System.out.println("Il tf-vector ha" + exitCounter + "Key/value pair");

                System.out.println("Leggo il dictionary file in TweetClusters/dictionary.file-0");


                tweetReader.close();

                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/dictionary.file-0"), conf);

                Text word = new Text();
                IntWritable intwr = new IntWritable();
                exitCounter = 0; //some cicle are veeeery long, with this i can inspect value
                //and interrupt at some point
                while (tweetReader.next(tweetID, intwr)) {

                    if (exitCounter < printCounter) {

                        System.out.println("word: " + tweetID.toString() + " value: " + intwr.toString());

                    }
                    exitCounter++;

                }

                tweetReader.close();

                System.out.println("The dictionary file has " + exitCounter + "elements");

                System.out.println("Calculate the Document Frequency");


                Pair<Long[], List<Path>> datasetFeature =
                        TFIDFConverter.calculateDF(
                                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                                new Path(outputDir),
                                conf,
                                chunkSize);


                System.out.println("Print the pair<long[],List<Path>> datasetFeature from the calculate DF");


                Long[] longArray = datasetFeature.getFirst();
                List<Path> pathList = datasetFeature.getSecond();


                for (long y : longArray) {

                    System.out.println(" " + y);

                }

                for (Path p : pathList) {

                    System.out.println(" " + p.toString());

                }

                System.out.println("Printing the frequency.file-0");

                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/frequency.file-0"), conf);

                LongWritable longwr = new LongWritable();
                intwr = new IntWritable();
                exitCounter = 0; //some cicle are veeeery long, with this i can inspect value
                //and interrupt at some point

			     
			     /*
                  * in questo modo posso capire di che classe sono la coppia key value,
			      * probabilmente potrei anche creare delle vaiabili che dinamicamiche
			      * vengano create del tipo che viene restituito
			      */

                System.out.println("Key Class: " + tweetReader.getKeyClass());
                System.out.println("Value Class: " + tweetReader.getValueClass());

                while (tweetReader.next(intwr, longwr)) {

                    if (exitCounter < printCounter) {
                        System.out.println("Key: " + intwr.toString() + " value: " + longwr.toString());
                    }
                    exitCounter++;

                }

                tweetReader.close();

                System.out.println("The frequency file has " + exitCounter + " elements");

                System.out.println("Printing the df-count part-r-0000 file");

                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/df-count/part-r-00000"), conf);

                longwr = new LongWritable();
                intwr = new IntWritable();
                exitCounter = 0; //some cicle are veeeery long, with this i can inspect value
                //and interrupt at some point

				     
				     /*
				      * in questo modo posso capire di che classe sono la coppia key value,
				      * probabilmente potrei anche creare delle vaiabili che dinamicamiche
				      * vengano create del tipo che viene restituito
				      */

                System.out.println("Key Class: " + tweetReader.getKeyClass());
                System.out.println("Value Class: " + tweetReader.getValueClass());

                while (tweetReader.next(intwr, longwr)) {

                    if (exitCounter < printCounter) {
                        System.out.println("Key: " + intwr.toString() + " value: " + longwr.toString());
                    }
                    exitCounter++;

                }

                tweetReader.close();

                System.out.println("The df-count file has " + exitCounter + " elements");
                System.out.println("Calcolo i tf-idf vector");


                TFIDFConverter.processTfIdf(
                        new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                        new Path(outputDir), //output directory
                        conf, //configuration file
                        datasetFeature, //dataset frequency Pair
                        minDf, //min document frequency default value 1
                        maxDFPercent, //maxDocumentFrequency default value 99
                        tfidfNorm, //normalization value, for tf-idf this should be -1.0f otherwise >=1,
                        tfidfLogNormalization,
                        sequentialAccessOutput, //boolean sequentialAccessOutput
                        true, //named vector if true used the documentID (in this case the tweet id)
                        reduceTasks); //numero di reducer


                System.out.println("Stampo il tfidf vector");


                tweetReader = new SequenceFile.Reader(fs, new Path("TweetClusters/tfidf-vectors/part-r-00000"), conf);


                exitCounter = 0; //some cicle are veeeery long, with this i can inspect value
                //and interrupt at some point
				     
				     
				     /*
				      * in questo modo posso capire di che classe sono la coppia key value,
				      * probabilmente potrei anche creare delle vaiabili che dinamicamiche
				      * vengano create del tipo che viene restituito
				      */

                System.out.println("Key Class: " + tweetReader.getKeyClass());
                System.out.println("Value Class: " + tweetReader.getValueClass());
                //le variabile usate sono state prese da sopra e riutilizzate che erano
                //gia state istanziate
                word = new Text();

                vect = new VectorWritable();


                while (tweetReader.next(word, vect)) {
                    if (exitCounter < printCounter) {
                        System.out.println("Key: " + word.toString() + " value: " + vect.toString());
                    }
                    exitCounter++;
                    if (exitCounter == randVect[0] || exitCounter == randVect[1] || exitCounter == randVect[2]) {

                        centroid.add(vect.get()); //adding point for the centroid with vect.get obtaing vector froma  vectorWritable
                        System.out.println("Vector choosen for Centroid  Key: " + word.toString() + " value: " + vect.toString());
                    }
                }

                System.out.println("Numero di tf-idf vectors: " + exitCounter);
                tweetReader.close();


                Path vectorsFolder = new Path(outputDir, "tfidf-vectors"); //directory used for clustering input
                Path canopyCentroids = new Path(outputDir, "canopy-centroids");//directory for store
                Path kmeansOutput = new Path(outputDir, "k-means");//directory for k-mean clustering


                //calcolo dei centroidi tramite canopy, da rivedere
                HadoopUtil.delete(conf, canopyCentroids);//cancello la directory dei centroidi
                HadoopUtil.delete(conf, kmeansOutput);    //deleting the k-means directory outptu
			    	    	/*
			    	    	System.out.println("Calcolo dei centroidi tramite canopy");
			    	    	
			    	    	
			    	    	
			    	    	//devo ricontrollare tutti i parametri perche ho fatto qualche minchiata
			    	    	
			    	    	CanopyDriver.run(vectorsFolder, //input folder
			    	    					  canopyCentroids,
						    	    	      new EuclideanDistanceMeasure(), 
						    	    	      
						    	    	       * I've used tanimoto for the canopies but usually is better a cheaper distance measure.
						    	    	       * in this way I've create a soft clustering (with overlapping cluster) and then clusters will be
						    	    	       * refined with a more expensive clustering alg (such as k-means ecc )
						    	    	       * 
						    	    	       * Changed distance to Euclidean 
						    	    	       *
						    	    	       
						    	    	      50, 
						    	    	      10,
						    	    	      
						    	    	       * These values represent the distance threshold:
						    	    	       * 
						    	    	       * The algorithm uses a fast approximate distance metric and two distance thresholds
						    	    	       *  T1 > T2 for processing. The basic algorithm is to begin with a set of points and 
						    	    	       *  remove one at random. Create a Canopy containing this point and iterate through 
						    	    	       *  the remainder of the point set. At each point, if its distance from the first point is < T1,
						    	    	       *   then add the point to the cluster. If, in addition, the distance is < T2, 
						    	    	       *   then remove the point from the set. This way points that are very close to the 
						    	    	       *   original will avoid all further processing. 
						    	    	       *   The algorithm loops until the initial set is empty, accumulating a set of Canopies, 
						    	    	       *   each containing one or more points.
						    	    	       *    A given point may occur in more than one Canopy.
						    	    	       * 
						    	    	       * 
						    	    	       
						    	    	      false,
						    	    	      0, 
						    	    	      false);
			    	    
			    	    System.out.println("Centroidi Calcolati, calcolo i cluster efettivi con k-means");
			    	    
			    	    
	    	    	    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
	    	    	        new Path(outputDir,"canopy-centroids/clusters-0-final/part-r-00000"), conf);
	    	    	    
	    	    	    
	    	    	    System.out.println("Key Class: "+reader.getKeyClass());
					    System.out.println("Value Class: "+reader.getValueClass());
	
					    
					    ClusterWritable clust=new ClusterWritable();
					    tweetText=new Text();//using an old variable
					    //word e vect sono variabli che erano state instanziati per altre letture che sto
					    //riusando
					    
	    	    	    while (reader.next(tweetText, clust)) {
	    	    	       System.out.println(tweetText.toString() + " belongs to cluster "
	    	    	       + clust.toString());
	    	    	    }
	    	    	    reader.close();
			    	    
			    	    */

                //since I'me not using the canopy centroid I choose 5 randome cluster in the dataset

                //sequence file for storing the inizial random cluster
                Path sequenceFileKlusterPath = new Path(outputDir, "k-meansInitialRandomCluster/part-00000");


                SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                        sequenceFileKlusterPath,
                        Text.class, Kluster.class);


                for (int i = 0; i < k; i++) {
                    Vector vec = centroid.get(i);
                    Kluster cluster = new Kluster(vec, i, new TanimotoDistanceMeasure());
                    writer.append(new Text(cluster.getIdentifier()), cluster);
                }
                writer.close();


                KMeansDriver.run(conf,
                        vectorsFolder,
                        sequenceFileKlusterPath,
                        kmeansOutput,
                        0.001,
                        20,
                        true,
                        0.0,
                        false);


                System.out.println("Lettura del final Cluster");


                try {
                    ClusterPrinter.export("tweetClusters/k-means/clusters-8-final/part-r-00000", //directory del cluster
                            "tweetClusters/tfidf-vectors/part-r-00000", //directory dei vettori che si sono clusterizzati
                            "consoleOutput/TextualClustering/kmeans3.txt",
                            ClusterDumper.OUTPUT_FORMAT.TEXT);
                } catch (Exception e) {
                    System.out.println("Exception thrown durign exporting Cluster");
                    e.printStackTrace();
                }
			    	    	    
			    	    	  /*  
			    	    	    
			    	    	    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
			    	    	        new Path(outputDir,"k-means/clusters-8-final/part-r-00000"), conf);
			    	    	    
			    	    	    
			    	    	    System.out.println("Key Class: "+reader.getKeyClass());
							    System.out.println("Value Class: "+reader.getValueClass());
			
							    
							    ClusterWritable clust=new ClusterWritable();
							   
							    
							    //word e vect sono variabli che erano state instanziati per altre letture che sto
							    //riusando
							    
			    	    	    while (reader.next(intwr, clust)) {
			    	    	       System.out.println(intwr.toString() + " belongs to cluster "
			    	    	       + clust.readFields(in););
			    	    	    }
			    	    	    reader.close();
			    */

                System.out.println("STOP");

            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


        } catch (FileNotFoundException e) {
            System.out.println("File missing");
            e.printStackTrace();
        }
        //open buffer to sequence file in which I define the class of the key-value


    }


    /**
     * This method return the string of the tweet without link or strange character
     *
     * @param tweet
     * @return
     */


    public String clean(String tweet) {


        return null;


    }


}
