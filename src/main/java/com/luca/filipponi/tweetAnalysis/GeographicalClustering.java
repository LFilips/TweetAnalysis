package com.luca.filipponi.tweetAnalysis;

import com.luca.filipponi.tweetAnalysis.ClusteringDumper.ClusterPrinter;
import com.luca.filipponi.tweetAnalysis.Plot2D.Plotter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class GeographicalClustering {
    /**
     * Class for execute the geographical clustering, the main do the following things:
     * Create the sequence file of the coordinates
     * Clustering using mahout k-means
     * export using ClusterDumper
     * Plotting the result with jchart
     *
     * @param args
     * @throws Exception
     */


    public static void main(String args[]) throws Exception {

        //Creating the sequenceFile from the dataset using the CreateCOordinatesSequenceFile
        String inputDataset = "input/DomenicaCompleto.txt";
        String clusteringInputFolder = "GeographicalClustering/input"; //used for store seqFile needed for Clustering
        String clusteringOutputFolder = "GeographicalClustering/output";//used as Clustering output Directory

        String seqFileCoordOutputDir = clusteringInputFolder + "/CoordinatesVector";
        String initialClusterSeqFileDir = clusteringInputFolder + "/Centroids"; //used as folder for intial centroid for kmeans

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        //crating the first sequence File
        SequenceFileCreator.createCoordinatesSequenceFile(inputDataset, seqFileCoordOutputDir);

        SequenceFileCreator.ReadSequenceFile(fs, conf, seqFileCoordOutputDir + "/part-r-00000", false, 10);


        //choose k centroid from the vector, form the first k value, for the geogrpahical clustering is okay
        int k = 10;

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(seqFileCoordOutputDir + "/part-r-00000"), conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(initialClusterSeqFileDir + "/part-00000"), Text.class, Kluster.class);

        //allocate key-vale based on the writing function
        LongWritable key = new LongWritable();
        VectorWritable value = new VectorWritable();

        for (int i = 0; i < k; i++) {

            reader.next(key, value);
            Kluster cluster = new Kluster(value.get(), i, new EuclideanDistanceMeasure());
            writer.append(new Text(cluster.getIdentifier()), cluster);
            System.out.println("Id: " + key.toString() + " Vector value:" + value.toString());
        }


        reader.close();
        writer.close();


        SequenceFileCreator.ReadSequenceFile(fs, conf, initialClusterSeqFileDir + "/part-00000", true, 0);

        //at this point i can execute the kmeans.clustering


        HadoopUtil.delete(conf, new Path(clusteringOutputFolder + "/k-means/k=" + k));

        KMeansDriver.run(conf, new Path(seqFileCoordOutputDir), new Path(initialClusterSeqFileDir),
                new Path(clusteringOutputFolder + "/k-means/k=" + k), 0.001, 15, true, 0.0, false);


        //i've to export a csv file and then plot it, but first i've to change the export without the dictionary
        //becouse there ins't the search for the cluster-x-final directory


        String clusteringDirectory = clusteringOutputFolder + "/k-means/k=" + k;


        System.out.println("Directory where search the clusteredPoints and x-final " + clusteringDirectory);
        File directory = new File(clusteringDirectory);
        ArrayList<String> names = new ArrayList<String>(Arrays.asList(directory.list()));
        String clusteringFinalDirectory = null;
        String pointsDirString = clusteringDirectory + "/clusteredPoints";
        //in questo modo l'array dovrebbe avere tutte le cartelle che sono dentro kmean

        for (String s : names) {

            if (s.endsWith("final")) {
                System.out.println("Selected dir: " + s);
                clusteringFinalDirectory = s;
            }
        }
        //now in clusteringFinalDirectory there is the x-final


        //exporting the cluster results in a directory

        final String outputFilename = "consoleOutput/GeographicalClustering/kmeans/k=" + k + "/Clustering.csv";
        final ClusterDumper.OUTPUT_FORMAT format = ClusterDumper.OUTPUT_FORMAT.CSV;


        System.out.println("Clustering finished, dumping result to a csv file and then plotting it");


        ClusterPrinter.export(clusteringDirectory + "/" + clusteringFinalDirectory, clusteringDirectory + "/clusteredPoints",
                outputFilename,
                format);


        Plotter.print("Clustering Visualization", outputFilename);


    }
}
