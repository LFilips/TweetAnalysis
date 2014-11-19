package com.luca.filipponi.tweetAnalysis.ClusteringDumper;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;


public class ClusterPrinter {
//class used for print in a readeble format the cluster output

    /**
     * Have to be changed !!!!!
     *
     * @param seqFileDirString directory where the cluster results are stored
     * @param pointsDirString  directory of the point created during the cluster in the folder of the cluster
     * @param outputFilename   name of the outputfile (can be .csv,.graphml,.txt)
     * @param format           specificy the format via the macro defined
     * @throws Exception
     */


    public static void export(final String seqFileDirString, final String pointsDirString,
                              final String outputFilename, final ClusterDumper.OUTPUT_FORMAT format) throws Exception {


        System.out.println("clusters-x-final directory: " + seqFileDirString +
                " clusterredPoint directory: " + pointsDirString +
                " outFileName: " + outputFilename +
                " Output Format: " + format);

        //creating path object from the final string value
        final Path seqFileDir = new Path(seqFileDirString);
        final Path pointsDir = new Path(pointsDirString);

        final ClusterDumper clusterDumper = new ClusterDumper(seqFileDir, pointsDir);

        String[] option = new String[]{
                buildOption(DefaultOptionCreator.INPUT_OPTION),
                seqFileDir.toString(),
                buildOption(DefaultOptionCreator.OUTPUT_OPTION),
                outputFilename, buildOption(ClusterDumper.OUTPUT_FORMAT_OPT),
                format.toString(),
                buildOption(ClusterDumper.POINTS_DIR_OPTION),
                pointsDir.toString(),
                buildOption(ClusterDumper.EVALUATE_CLUSTERS)};


        //print the option
        System.out.println("String passed to ClusterDumper.run: ");
        for (String e : option) {
            System.out.print(e + " ");
        }
        System.out.println();


        clusterDumper.run(option);

    }
    //devo aggiungere il numero delle top terms e la directory del file dictionary, e fare in modo che si capi da solo quela
    // è il file final 


    /**
     * export with dictionary export the sequenceFile of a clustering in csv,text, or graphml format.
     * Typically the clustering directory has two main directory, the clusteredPoints and the final, these are necessary
     * for exporting in a file.
     * Inside the method is define the num_word_option, that is the number of top terms of the clustering.
     * An example call is ClusterPrinter.exportWithDictionary(TextualClustering/output/k-means3/,
     * "dictionary.file-0" (metti il percorso)
     * "consoleOutput/TextualClustering/kmeans3randomCentroid.csv",
     * ClusterDumper.OUTPUT_FORMAT.CSV,
     * "100");
     *
     * @param clusteringDirectory The top folder of a clustering, the one with inside the cluster-x-final and the clusteredPoints
     * @param outputFilename      Path and fileName for the output file
     * @param dictionaryFile      the path to the dictionary.file-0
     * @param format              Format for the export of the cluster ( ClusterDumper.OUTPUT_FORMAT)
     * @param topTerms            The number of topTerms for each cluster to dump (have to be express in string form "100")
     * @throws Exception
     */

    public static void exportWithDictionary(final String clusteringDirectory, final String dictionaryFile,
                                            final String outputFilename, final ClusterDumper.OUTPUT_FORMAT format, String topTerms) throws Exception {

        //obtaining the clustered point and the x-final directory

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


        System.out.println("clusters-x-final directory: " + clusteringDirectory + "/" + clusteringFinalDirectory +
                " clusterredPoint directory: " + pointsDirString +
                " outFileName: " + outputFilename +
                " Output Format: " + format +
                "Number of top Words " + topTerms);

        //creating path object from the final string value
        final Path seqFileDir = new Path(clusteringDirectory + "/" + clusteringFinalDirectory, "part-r-00000");
        final Path pointsDir = new Path(pointsDirString);

        final ClusterDumper clusterDumper = new ClusterDumper(seqFileDir, pointsDir);

        String[] option = new String[]{
                buildOption(DefaultOptionCreator.INPUT_OPTION),
                seqFileDir.toString(),
                buildOption(DefaultOptionCreator.OUTPUT_OPTION),
                outputFilename, buildOption(ClusterDumper.OUTPUT_FORMAT_OPT),
                format.toString(),
                buildOption(ClusterDumper.POINTS_DIR_OPTION),
                pointsDir.toString(),
                buildOption(ClusterDumper.DICTIONARY_OPTION), //option for the dictionary
                dictionaryFile,
                buildOption(ClusterDumper.NUM_WORDS_OPTION), topTerms,// dovrebbe essere le prima n top words
                // buildOption(ClusterDumper.SAMPLE_POINTS), "30",
                buildOption(ClusterDumper.DICTIONARY_TYPE_OPTION),
                "sequencefile", buildOption(ClusterDumper.EVALUATE_CLUSTERS)};


        //print the option
        System.out.println("String passed to ClusterDumper.run: ");
        for (String e : option) {
            System.out.print(e + " ");
        }
        System.out.println();


        clusterDumper.run(option);

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