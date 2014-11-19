package com.luca.filipponi.tweetAnalysis.Analyzer;

//internal import

import com.luca.filipponi.tweetAnalysis.ClusteringDumper.ClusterPrinter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for clustering the Geographical information, use GeographicalClustering.java instead of this.
 */

public class Decoder {

    //i've comment all the main

	/*
    public static void main(String args[]) throws NoSuchFieldException, IllegalAccessException, Exception{
		//could be useful insert some parameters from the args
		
		//the first thing to do is to create a method that reads a json file and decodes all the information
		//then this can be integreate with a Map/Reduce, from now the file il read from an interal
		//folder of the project, later it will be inside the hadoop hdfs
		System.out.println("Inzio analisi del dataset");
		//JsonInput.txt is a random sample with tweet taken all across the complete dataset
		try (BufferedReader br = new BufferedReader(new FileReader("input/DomenicaCompleto.txt")))
		
		
		{
 
			String sCurrentLine;
			long tweetCounter=0; //count tweet
			long geoCounterNull=0;//counter for tweet with geo null
			
			ArrayList<Coordinates> coordinatesList=new ArrayList<Coordinates>();
			
			while ((sCurrentLine = br.readLine()) != null) {
				
				//skip the blank line
				if(!(sCurrentLine.equals(""))){
					
				
				
					JSONObject tweetJson = new JSONObject(sCurrentLine); //can throws json exception should catch
				
					
					tweetCounter++;
				
				
					String tweetText=tweetJson.getString("text");
					//print tweet text
					
					//Debug.print(tweetText);
					
					
					//GEO FIELD EXAMPLE, the following place field has info on the recognized city
	//"geo":{"type":"Point","coordinates":[41.89419072,12.47616179]},"coordinates":{"type":"Point","coordinates":[12.47616179,41.89419072]}
					
					if(!(tweetJson.isNull("geo"))){
						
					JSONObject geoJson=tweetJson.getJSONObject("geo");
					
					//extraxting the coordinates array
					JSONArray coordinatesArray=geoJson.getJSONArray("coordinates");
					
					Coordinates point=new Coordinates(coordinatesArray.getDouble(0),coordinatesArray.getDouble(1));
					
					//Debug.print(point.toString());
					
					
					
					coordinatesList.add(point);
					
					//Debug.print(sCurrentLine);
					
					//Debug.print(geoField);
					}
					else {
						//geo field null
						geoCounterNull++;
						
					}
						
				
				
				}
			}
			//file ended
			System.out.println("Number of tweet: "+tweetCounter+" Number of GeotaggedTweet: "+coordinatesList.size()+" Null geotag: "+geoCounterNull);
 
			printList(coordinatesList);
			
			
			//A QUESTO PUNTO DEVO PASSARE QUESTO ARRAY E FARLO CLUSTERIZZARE A MAHOUT PER VEDERE COME FUNZIONA
			int kMeansCluster=5;
			
			
			//clustering algorithm
			clustering(kMeansCluster,coordinatesList);
			
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} 
 
	}*/

    /**
     * Print an arrayList
     *
     * @param list
     */

    static void printList(ArrayList<Coordinates> list) {

        System.out.println("Value containes in List");

        for (Coordinates e : list) {

            System.out.println(e.toString());

        }


    }

    /**
     * method that invokes the k-means clustering algorithm in mahout,
     * this is all in memory isn't good
     *
     * @param k    number of cluster
     * @param list CoordinatesList
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws Exception
     */
    //method for clustering (int k is for cluster number, the list containes the tweet coordinates)
    static void clustering(int k, ArrayList<Coordinates> list) throws NoSuchFieldException, IllegalAccessException, Exception {

        System.out.println("Start Clustering");


        System.out.println("Creating a List<Vector> from coordinatesList");
        //creation of a Vector
        List<Vector> vectors = new ArrayList<Vector>();
        /**
         * Vector è un interfaccia di mahout.math, la quale essendo appunto un interfaccia
         * ha la dichiarazione dei metodi necessari per i vettori, che poi vengono
         * implementati dalle classe vere e proprie come RandomAccessSparseVector o SequentialAccessSparseVector
         */

        System.out.println("Create a List<Vector> from ArrayList<Coordinates>: the Vector object belongs to mahout class");
        System.out.println("Iterating throught the CoordinatesList adding double[2] to vector");

        //iterating throught coordinates list creating an ArrayList<Vector> from an ArrayList<Coordinates>
        for (Coordinates e : list) {
            double[] fr = e.getCoordinates(); //get a double[2] array see Coordinates Class
            Vector vec = new RandomAccessSparseVector(fr.length);
            vec.assign(fr);
            /**
             *  RandomAccessSparseVector Implements vector that only stores non-zero doubles
             *  the assign method Assign the other vector values to the receiver (??)
             *
             *
             */
            vectors.add(vec);
        }

        System.out.println("List<Vector> size: " + vectors.size() + " Size of vector[5]: " + vectors.get(5).size());


        File testData = new File("testdata");
        if (!testData.exists()) {
            testData.mkdir();
        }
        testData = new File("testdata/points");
        if (!testData.exists()) {
            testData.mkdir();
        }

        System.out.println("Creating configuration file");
        Configuration conf = new Configuration();    //import org.apache.hadoop.conf.Configuration;
        //oltre all'import ho dovuto aggiungere la librearia di logging 1.1.1 al progetto


        System.out.println("Creating fs object");
        FileSystem fs = FileSystem.get(conf);// import org.apache.hadoop.fs.FileSystem; vedi se vanno bene questi due import

        /**
         *aggiunta libreria di configurazione,
         *e dato che ogni volta mi dava un errore
         *ho aggiunto tutte le librerie in lib di hadoop e via
         *
         */


        writePointsToFile(vectors, "testdata/points/file1", fs, conf);
        /**
         *
         * method that create a sequence file in the specified path from an Array of vector (or List)
         *
         *
         */


        Path path = new Path("testdata/clusters/part-00000");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);
        //anche qui devi vede se gli import sono corretti perche ce ne erano diversi, io ho preso
        /**
         *
         *
         * The last two parameters is Key and Value class
         *
         *
         *
         */

        //nuovo codice preso da
        // https://github.com/tdunning/MiA/blob/mahout-0.7/src/main/java/mia/clustering/ch07/SimpleKMeansClustering.java

        /**
         *
         * K is the number of cluster for the k-means algorithm,
         * probably create the inizial centroid of the cluster
         *
         *
         *
         *
         */

        for (int i = 0; i < k; i++) {
            Vector vec = vectors.get(i);
            Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
        }
        writer.close();


        Path output = new Path("output");


        HadoopUtil.delete(conf, output);

        //vedi bene qui perche hai tolto degli argomenti per farlo matchare
        KMeansDriver.run(conf, new Path("testdata/points"), new Path("testdata/clusters"),
                output, 0.001, 10, true, 0.0, false);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("output/" + Kluster.CLUSTERED_POINTS_DIR
                        + "/part-m-00000"), conf);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
            System.out.println(value.toString() + " belongs to cluster "
                    + key.toString());
        }
        reader.close();


        System.out.println("CLUSTERING FINISHED, DUMPING RESULTS TO GRAPH FILE");

        //parameters for export the cluster on graphml, csv or text file
        //there is even json but you have to define the dictionary file

        final String seqFileDir = "output/clusters-9-final";
        final String pointsDir = "output/clusteredPoints";
        final String outputFilename = "consoleOutput/GraphCluster.csv";
        final ClusterDumper.OUTPUT_FORMAT format = ClusterDumper.OUTPUT_FORMAT.CSV;


        ClusterPrinter.export(seqFileDir, pointsDir, outputFilename, format);


    }

    /**
     * method for writing a List<Vector> in a sequence file, the sequence file has
     * in the key an incremental number starting from zero of LongWritable.Class (should be improved)
     * ad a VectorWritable class in the value that store the Vector
     *
     * @param points   vector to write
     * @param fileName sequence file name
     * @param fs
     * @param conf
     * @throws IOException
     */

    public static void writePointsToFile(List<Vector> points, String fileName, FileSystem fs, Configuration conf)
            throws IOException {


        System.out.println("Method writePointsToFile, writes a List<Vector> to a sequenceFile");

        Path path = new Path(fileName); //import org.apache.hadoop.fs.Path;


        //vedere bene questo sequence file.write che è deprecato ma per la prova sarebbe meglio lasciarlo cosi
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, VectorWritable.class);
            /*
             *
			 * questo metodo è deprecato dovrei usare:
			 * FileContext myFContext = FileContext.getFileContext();
			 * Create a FileContext using the default config read from the $HADOOP_CONFIG/core.xml, 
			 *  Unspecified key-values for config are defaulted from core-defaults.xml in the release jar.
			 *  
			 * SequenceFile.Writer writer=SequenceFile.createWriter(myFContext, conf, 
			 * path, LongWritable.class, VectorWritable.class, compressionType, codec, metadata, createFlag, opts)
			 * ma ci sono troppi parametri che non so cosa siano
			 * 
			 * 
			 */
        long recNum = 0;
        VectorWritable vec = new VectorWritable();
        for (Vector point : points) {
            vec.set(point);
            writer.append(new LongWritable(recNum++), vec);
            //dovresti vedere bene perche serve questa variabile di incremento
        }
        writer.close();


        System.out.println("Ending method writePoints to file, now there should be all the points written to file");
    }


    /**
     * Cut the list k, at the element k, it the list has 5 elements 1,2,3,4,5 and k is 2, the output list is
     * 1,2 doing side effect on the input list
     *
     * @param list
     * @param k
     * @return
     */
    public static void cutList(ArrayList<Integer> list, int k) {
        //fa side effect sulla lista di input e la taglia dopo k elementi
        int i = 0;
        for (int e : list) {
            i++;
            if (i > k) {

                list.remove(e);

            }

        }


    }


}

	
	




