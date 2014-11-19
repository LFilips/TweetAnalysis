package com.luca.filipponi.tweetAnalysis.Analyzer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/**
 * The CoordinatesMapper is the Mapper class used for mapping the line of the input in
 * a key/value such that <LongWritable,VectorWritable>, should create a sequenceFile with these value from
 * the inputFile
 *
 * @author luca
 */
public class TweetMapper extends Mapper<LongWritable, Text, Text, Text> {
    //input key, input value, output key, output value


    //quesat è la classe di mapper, con la quale devo fare il map da json a seuqenceFile, il reducer non serve


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString(); //prendo la linea che dovrebbe essere una linea di json

        //System.out.println("Line :"+line);

        //Analizzo la riga json
        if (!(line.equals(""))) {
            JSONObject tweetJson = new JSONObject(line);
            //can throws json exception should catch, and continue executing


            //String tweetText=tweetJson.getString("text"); per questo mapReduce non mi interessa il testo dei tweet
            String tweetID = tweetJson.getString("id_str");
            String tweetText = tweetJson.getString("text");

            Text outputKey = new Text(tweetID);
            Text outputValue = new Text(tweetText);

            //          key/value --->   TweetID/tweetText boht text() class

            //System.out.println("<Key,value>: <"+outputKey.toString()+","+outputValue.toString()+">");

            context.write(outputKey, outputValue);

        }

    }


}
