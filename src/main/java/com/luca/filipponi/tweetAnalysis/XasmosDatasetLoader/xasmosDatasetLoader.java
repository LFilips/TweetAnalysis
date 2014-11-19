package com.luca.filipponi.tweetAnalysis.XasmosDatasetLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.sql.*;

/**
 * Created by luca Filipponi on 11/06/14.
 * <p/>
 * <p/>
 * Class used for Load the Dataset inside a Mysql Database, in a sequence File, which can be read from Mahout
 */
public class xasmosDatasetLoader {

    //using this class to connect with mySql db, load all the tweet and create a sequenceFile


    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/dbTwitter";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "";

    /**
     * Main method, takes the mysql input and writes a Sequence File in file located at DatesetXasmosSequenceFile/part-r-00000
     *
     * @param args
     */


    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            //STEP 4: Execute a query
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;

            int n = 1;
            int k = 0;
            int min = 0;
            int max = 100000;

            String sequenceFileXasmos = "DatesetXasmosSequenceFile/part-r-00000";

            Path path = new Path(sequenceFileXasmos);
            System.out.println("Creating configuration file");
            Configuration conf = new Configuration();


            System.out.println("Creating fs object");
            FileSystem fs = FileSystem.get(conf);

            //creo un sequenceFile con coppia Chiave-Valore long-text (idtweet e contenuto)
            SequenceFile.Writer tweetWriter = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);

            String tweetText;
            String tweetID;


            sql = "SELECT id_tweet,text FROM twitter group BY text";


            System.out.println("Executing query: " + sql);
            ResultSet rs = stmt.executeQuery(sql);


            //STEP 5: Extract data from result set
            while (rs.next()) {

                tweetID = rs.getString("id_tweet");
                tweetText = rs.getString("text");
                tweetWriter.append(new Text(tweetID), new Text(tweetText));
                k++;
            }

            rs.close(); //close the result set


            System.out.println("Rows obtained " + k);
            //STEP 6: Clean-up environment
            tweetWriter.close();
            stmt.close();
            conn.close();


            System.out.println("Reading the newly written seqFile in: " + path.toString());

            SequenceFile.Reader tweetReader = new SequenceFile.Reader(fs, path, conf);


            Text text = new Text();
            Text id = new Text();


            int counter = 0;
            //qui ho messo il printCounter direttamtne per uscre dal ciclo che senno ci vuole na vita
            while (tweetReader.next(text, id)) {

                System.out.println("TweetID: " + text.toString() + " TweetText: " + id.toString());
                counter++;

            }

            tweetReader.close();


        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try

    }//end main
}//end FirstExample



