package com.luca.filipponi.tweetAnalysis;

import com.luca.filipponi.tweetAnalysis.Analyzer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.math.VectorWritable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class SequenceFileCreator {
    /**
     * this class is for create a sequence file from the dataset using the
     * mapReduce paradigm, there is two method, one for create a sequenceFile
     * for the coordinates and the other for the text
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String args[]) throws IOException,
            ClassNotFoundException, InterruptedException {

        String inputDataset = "input/DomenicaCompleto.txt";
        String CsvInputDataset = "input/utf8.txt";
        String seqFileCoordOutputDir = "CoordinatesMapReduce";
        String seqFileTweetOutputDir = "TweetMapReduce";
        String seqFileCsvTweetOutputDir = "TweetCsvMapReduce";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (!(new File(seqFileCoordOutputDir + "/part-r-00000").exists()))
            createCoordinatesSequenceFile(inputDataset, seqFileCoordOutputDir);

        ReadSequenceFile(fs, conf, seqFileCoordOutputDir + "/part-r-00000",
                false, 10);

        if (!(new File(seqFileTweetOutputDir + "/part-r-00000").exists()))
            createTweetSequenceFile(inputDataset, seqFileTweetOutputDir);

        ReadSequenceFile(fs, conf, seqFileTweetOutputDir + "/part-r-00000",
                false, 10);

        // if(!(new File(seqFileCsvTweetOutputDir+"/part-r-00000").exists()))
        createTweetFromCsvSequenceFile(CsvInputDataset,
                seqFileCsvTweetOutputDir);

        ReadSequenceFile(fs, conf, seqFileCsvTweetOutputDir + "/part-r-00000",
                true, 10);

    }

    /**
     * This method create a sequenceFile with <longWritable,VectorWritable> key
     * value, in which each key/value pair is a geoTagged tweet where the key in
     * the twetID (represented as a long value) and the value is a
     * VectorWritable, that is a vector composed of the lat and long value, this
     * is an example <Key,value>:
     * <460533985924628480,{0:45.19467361,1:9.86166615}
     *
     * @param dataset   is the string of the path to the datasetfile (should be a
     *                  tweet json format)
     * @param OutputDir is the string od the path of the desiredOutPut director (this
     *                  method erase all the content in such directory and then create
     *                  the seqfile)
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */

    public static void createCoordinatesSequenceFile(String dataset,
                                                     String OutputDir) throws ClassNotFoundException, IOException,
            InterruptedException {

        System.out
                .println("Creating the Coordinates sequencefile from the json dataset");

        Configuration conf = new Configuration();
        // FileSystem fs=FileSystem.get(conf);

        Job job = new Job();
        job.setJarByClass(SequenceFileCreator.class);
        job.setJobName("CoordiantesSequenceFileCreator");
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        HadoopUtil.delete(conf, new Path(OutputDir));

        FileInputFormat.addInputPath(job, new Path(dataset));

        FileOutputFormat.setOutputPath(job, new Path(OutputDir));

        job.setMapperClass(CoordinatesMapper.class);
        job.setReducerClass(CoordinatesReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        /*
         * Indicates the key and the value class for the mapper and the reducer,
		 * if the value are different use setMapOutputKeyClass() and
		 * setMapOutputValueClass() ...
		 */

        job.waitForCompletion(true);

        // vedo se aspetta che il job sia finito

        System.out.println("Sequence file scritto in : " + OutputDir);

    }

    /**
     * Create a sequenceFile with <key,Value> <text(),text()> where key=tweetId
     * and value=tweetText reading from the dataset of tweet in json format
     *
     * @param dataset   tweet dataset in json format
     * @param OutputDir directory where output the sequenceFile
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */

    public static void createTweetSequenceFile(String dataset, String OutputDir)
            throws ClassNotFoundException, IOException, InterruptedException {

        System.out
                .println("Creating the Tweet sequencefile from the json dataset");

        Configuration conf = new Configuration();
        // FileSystem fs=FileSystem.get(conf);

        Job job = new Job();
        job.setJarByClass(SequenceFileCreator.class);
        job.setJobName("TweetSequenceFileCreator");
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        HadoopUtil.delete(conf, new Path(OutputDir));

        FileInputFormat.addInputPath(job, new Path(dataset));

        FileOutputFormat.setOutputPath(job, new Path(OutputDir));

        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(TweetReducer.class); // the remover aggregates the
        // different part from
        // mapper

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /*
         * Indicates the key and the value class for the mapper and the reducer,
		 * if the value are different use setMapOutputKeyClass() and
		 * setMapOutputValueClass() ...
		 */

        job.waitForCompletion(true);

        // vedo se aspetta che il job sia finito

        System.out.println("Sequence file scritto in : " + OutputDir);

    }

    /**
     * TODO documentation
     *
     * @param dataset
     * @param OutputDir
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */

    public static void createTweetFromCsvSequenceFile(String dataset,
                                                      String OutputDir) throws ClassNotFoundException, IOException,
            InterruptedException {

        System.out.println("Creating the Tweet sequencefile from the csv dataset");

        Configuration conf = new Configuration();
        // FileSystem fs=FileSystem.get(conf);

        Job job = new Job();
        job.setJarByClass(SequenceFileCreator.class);
        job.setJobName("TweetFromCsvSequenceFileCreator");
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        HadoopUtil.delete(conf, new Path(OutputDir));

        FileInputFormat.addInputPath(job, new Path(dataset));

        FileOutputFormat.setOutputPath(job, new Path(OutputDir));

        job.setMapperClass(CsvTweetMapper.class);
        job.setReducerClass(CsvTweetReducer.class); // the remover aggregates
        // the different part from
        // mapper

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /*
		 * Indicates the key and the value class for the mapper and the reducer,
		 * if the value are different use setMapOutputKeyClass() and
		 * setMapOutputValueClass() ...
		 */

        job.waitForCompletion(true);

        // vedo se aspetta che il job sia finito

        System.out.println("Sequence file scritto in : " + OutputDir);

    }

    // sarebbe utile mettere un metodo per fare legere solo i primo n record in
    // modo da poter visionare i file ma senza
    // rallentare troppo

    /**
     * Method that prints all the <key/value> of a sequenceFile
     *
     * @param fs
     * @param conf
     * @param inputSeqFile location of the sequenceFile
     * @param printAll     if true all the <key,value> is printed, otherwise only n pair
     * @param n            number of pair to output
     * @throws IOException
     */

    public static void ReadSequenceFile(FileSystem fs, Configuration conf,
                                        String inputSeqFile, boolean printAll, int n) throws IOException {
        // prendo in input il sequencefile e la classe del key/value e stampo ad
        // output i valore

        System.out.print("ReadSequenceFile method, reading ");
        if (printAll == true)
            System.out
                    .println("all the <key,value> pair from: " + inputSeqFile);
        else
            System.out.println(n + " <key,value> pair from: " + inputSeqFile);

        // opening sequenceFIle reader
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                inputSeqFile), conf);

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
                System.out.println("Key: " + key.toString() + " Value:"
                        + value.toString());
                i++;
                if (printAll == false && i > n)
                    break; // exit from the cicle
            }

        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        reader.close();

    }

    /**
     * Create a sequenceFile with k centroid in the choosen location, the index
     * number is used for iterate the sequenceFile vector, when reached the n
     * key,value the vector is choosen, the return value indicates if all the
     * centroid is written, a calling example is
     * <p/>
     * ArrayList.add ... 5,8,9 centroidCreator(fs,conf,3,list,directoryCentroid,
     * ...
     * <p/>
     * <p/>
     * Pay attention to the list, is written a number od centroid equals to the
     * number of integer contained in the list if the list has 10 elements, 10
     * elements are created, even if k is <=10.
     *
     * @param k                is the number of centroid
     * @param list             is a list containing the index number of the vector that will
     *                         be the centroid center
     * @param centroidLocation is the directory where the part-r-00000 file will be written
     * @return the number of centroid written
     * @throws IOException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public static int tanimotoCentroidCreator(FileSystem fs, Configuration conf, int k,
                                              ArrayList<Integer> list, String centroidLocation,
                                              String sequenceFileVector) throws IOException,
            InstantiationException, IllegalAccessException {

        // apro un SequenceFile reader per il vettore
        // apro un SequenceFile writer per scrivere i cluster

        System.out.println("centroidCreator Method, k=" + k
                + " Centroid Location " + centroidLocation
                + " Vector SequenceFile" + sequenceFileVector);
        Path sequenceFileKlusterPath = new Path(centroidLocation);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                sequenceFileVector), conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                sequenceFileKlusterPath, Text.class, Kluster.class);

        int i = 0;
        int ret = 0;

        Writable key;
        VectorWritable value;

        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        // in this way I should create an instance of the correct class
        key = (Writable) (keyClass.newInstance());
        value = (VectorWritable) (valueClass.newInstance());

        System.out.println("Key class: " + keyClass.toString()
                + " Value class: " + valueClass.toString());

        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {

            if (list.contains(i)) {

                System.out.println("Adding the vector in index " + i
                        + " In the cluster :" + ret + " Vector value: "
                        + value.toString());
                // se l'indice della chiave,valore è contenuto nell'arrayList
                // inserito allora uso il vettore per scrivere il cluster
                Kluster cluster = new Kluster(value.get(), ret,
                        new TanimotoDistanceMeasure());
                writer.append(new Text(cluster.getIdentifier()), cluster);
                ret++;
            }
            i++;

        }

        reader.close();
        writer.close();

        return 0;

    }

    public static int cosineCentroidCreator(FileSystem fs, Configuration conf, int k,
                                            ArrayList<Integer> list, String centroidLocation,
                                            String sequenceFileVector) throws IOException,
            InstantiationException, IllegalAccessException {

        // apro un SequenceFile reader per il vettore
        // apro un SequenceFile writer per scrivere i cluster

        System.out.println("centroidCreator Method, k=" + k
                + " Centroid Location " + centroidLocation
                + " Vector SequenceFile" + sequenceFileVector);
        Path sequenceFileKlusterPath = new Path(centroidLocation);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                sequenceFileVector), conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                sequenceFileKlusterPath, Text.class, Kluster.class);

        int i = 0;
        int ret = 0;

        Writable key;
        VectorWritable value;

        // <?> indicates that I'don't know the class to be modeled, perhaps I
        // can put Class<Writable>
        Class<?> keyClass = reader.getKeyClass();
        Class<?> valueClass = reader.getValueClass();

        // in this way I should create an instance of the correct class
        key = (Writable) (keyClass.newInstance());
        value = (VectorWritable) (valueClass.newInstance());

        System.out.println("Key class: " + keyClass.toString()
                + " Value class: " + valueClass.toString());

        // allocate key-vale based on the writing function

        while (reader.next(key, value)) {

            if (list.contains(i)) {

                System.out.println("Adding the vector in index " + i
                        + " In the cluster :" + ret + " Vector value: "
                        + value.toString());
                // se l'indice della chiave,valore è contenuto nell'arrayList
                // inserito allora uso il vettore per scrivere il cluster
                Kluster cluster = new Kluster(value.get(), ret,
                        new CosineDistanceMeasure());
                writer.append(new Text(cluster.getIdentifier()), cluster);
                ret++;
            }
            i++;

        }

        reader.close();
        writer.close();

        return 0;

    }

}
