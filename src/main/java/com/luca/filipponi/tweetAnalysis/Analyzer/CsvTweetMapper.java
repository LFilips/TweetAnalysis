package com.luca.filipponi.tweetAnalysis.Analyzer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * The CoordinatesMapper is the Mapper class used for mapping the line of the input in
 * a key/value such that <LongWritable,VectorWritable>, should create a sequenceFile with these value from
 * the inputFile
 *
 * @author luca
 */
public class CsvTweetMapper extends Mapper<LongWritable, Text, Text, Text> {
    //input key, input value, output key, output value


    //quesat è la classe di mapper, con la quale devo fare il map da json a seuqenceFile, il reducer non serve


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString(); //prendo la linea che dovrebbe essere una linea di json

        System.out.print("Line =" + line + " ");

        String tokenized[] = line.split(";");


        //String tweetText=tweetJson.getString("text"); per questo mapReduce non mi interessa il testo dei tweet
        try {
            String tweetID = tokenized[1];

            String tweetText = tokenized[6];

            System.out.println("tweetID= " + tweetID + " tweetText= " + tweetText);

            Text outputKey = new Text(tweetID);
            Text outputValue = new Text(tweetText);

            //          key/value --->   TweetID/tweetText boht text() class

            //System.out.println("<Key,value>: <"+outputKey.toString()+","+outputValue.toString()+">");

            context.write(outputKey, outputValue);

        } catch (Exception e) {
            System.out.println("Exception in mapper, bad formatted line, skipping it");
            e.printStackTrace();

        }

    }

}

	

		

