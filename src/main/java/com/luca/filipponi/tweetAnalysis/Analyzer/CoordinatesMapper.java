package com.luca.filipponi.tweetAnalysis.Analyzer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

/**
 * The CoordinatesMapper is the Mapper class used for mapping the line of the input in
 * a key/value such that <LongWritable,VectorWritable>, should create a sequenceFile with these value from
 * the inputFile
 *
 * @author luca
 */
public class CoordinatesMapper extends Mapper<LongWritable, Text, LongWritable, VectorWritable> {
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
            Long tweetID = Long.valueOf(tweetJson.getString("id_str")).longValue();


            //GEO FIELD EXAMPLE, the following place field has info on the recognized city
//"geo":{"type":"Point","coordinates":[41.89419072,12.47616179]},"coordinates":{"type":"Point","coordinates":[12.47616179,41.89419072]}

            if (!(tweetJson.isNull("geo"))) {

                JSONObject geoJson = tweetJson.getJSONObject("geo");

                //extraxting the coordinates array
                JSONArray coordinatesArray = geoJson.getJSONArray("coordinates");

                //creating coordinates object (useless)
                Coordinates point = new Coordinates(coordinatesArray.getDouble(0), coordinatesArray.getDouble(1));

                //creating a RandomAccessSparseVector from double[2]
                //Probably is better a SequentialAccessSparseVector for faster access (see on mahout in action)
                Vector vec = new RandomAccessSparseVector(point.getCoordinates().length);
                vec.assign(point.getCoordinates());


                VectorWritable vecWritable = new VectorWritable();
                LongWritable lonWritable = new LongWritable(tweetID);
                vecWritable.set(vec);

                //          key/value --->   TweetID/[lat,long]

                //System.out.println("<Key,value>: <"+lonWritable.toString()+","+vecWritable.toString()+"> del tweet:"+line);

                context.write(lonWritable, vecWritable);

            }

        }

    }


}
