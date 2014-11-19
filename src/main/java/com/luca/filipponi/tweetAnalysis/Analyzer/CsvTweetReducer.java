package com.luca.filipponi.tweetAnalysis.Analyzer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CsvTweetReducer extends Reducer<Text, Text, Text, Text> {
    @Override


    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //il reducer nn deve fare niente, prende il risultato dal mapper e lo riscrive, sempre con key/value
        // text(),vectorWritable()

        Text tweetText = new Text();

        for (Text t : values)
            tweetText = t;

        context.write(key, tweetText);
    }


}

