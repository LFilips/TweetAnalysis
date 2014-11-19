package com.luca.filipponi.tweetAnalysis.Analyzer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;


/**
 * Reducer part for load the Geographical Information from tweet
 */

public class CoordinatesReducer extends Reducer<LongWritable, VectorWritable, LongWritable, VectorWritable> {
    @Override
    public void reduce(LongWritable key, Iterable<VectorWritable> values, Context context)
            throws IOException, InterruptedException {
        //il reducer nn deve fare niente, prende il risultato dal mapper e lo riscrive, sempre con key/value
        // text(),vectorWritable()

        VectorWritable vector = new VectorWritable();

        for (VectorWritable v : values)
            vector = v;

        context.write(key, vector);
    }
}