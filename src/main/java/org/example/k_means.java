package org.example;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class k_means {

    static ArrayList<Point> centroidi = new ArrayList<>();
    private static int dimensione = 0;

    public static class kmeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        // inutile???
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();



        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Point punto = new Point(value);

            if(dimensione==0) {
                dimensione =  punto.getCoordinates().size();
                //System.out.println("dimensione del punto:"+dimensione+"\n");
                // la prima volta recupero anche i centroidi
                Configuration conf = context.getConfiguration();
                String centroidCoordinatesString = conf.get("centroidCoordinates");

                // Divide la stringa delle coordinate dei centroidi in un array di stringhe
                String[] centroidCoordinatesArray = centroidCoordinatesString.split(";");

                // Converte ciascuna stringa in un oggetto Text e inserisce nell'array
                for (int i = 0; i < centroidCoordinatesArray.length; i++) {
                    centroidi.add(new Point(new Text(centroidCoordinatesArray[i])));
                }

                //System.out.println("Stampa dei centroidi nuovi:");
                for (int i=0; i<centroidi.size();i++){
                    ArrayList<Double> coordinateCentroide = centroidi.get(i).getCoordinates();
                   // System.out.println("Coordinate del centroide "+i+": " + coordinateCentroide+"\n");
                }
            }

            ArrayList<Double> distanze = new ArrayList<>();


            for (int i=0; i<centroidi.size();i++){
                double dist = punto.calculateDistance(centroidi.get(i));
                distanze.add(dist);

                //System.out.println("Distanza dal cluster "+i+":"+dist+"\n");

                if(dist<punto.getMin_distance() || punto.getMin_distance()==-1){
                    punto.setMin_distance(dist);
                    punto.setCluster(i+1);
                }
            }



            System.out.println("Stampa classe punto. Coordinate del punto: " + punto.getCoordinates()+"\n");
            System.out.println(" Min_distance:"+punto.getMin_distance()+", cluster: " +punto.getCluster()+"\n");

            outputKey.set(String.valueOf(punto.getCluster()));
            outputValue.set(String.valueOf(punto.getCoordinates()));
            context.write(outputKey, outputValue);


        }
    }

    public static class kmeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            /* System.out.println("Chiave: "+ key.toString()+"\n");
            for (Text value : values) {
                System.out.println(value+" ");
            } */


            int cluster_number = Integer.parseInt(key.toString()) - 1 ;


            // inutile ???
            Point centroide_attuale = centroidi.get(cluster_number);
            StringTokenizer tokenizer = new StringTokenizer(values.toString(), ";");

            ArrayList<Double> zeros = new ArrayList<>(Collections.nCopies(dimensione, 0.0));

            Point new_centroid = new Point(zeros);

            int num_points = 0;

            double distanza_media = 0;

            while(tokenizer.hasMoreTokens()){
                Point punto = new Point(new Text(tokenizer.nextToken()));

                distanza_media  += punto.calculateDistance(centroide_attuale);

                new_centroid.sum(punto);
                num_points++;
            }

            int i = 0;

            while (i<dimensione){
                double average = new_centroid.getCoordinates().get(i)/num_points;
                new_centroid.setCoordinate(i,average);
                i++;
            }

            distanza_media = distanza_media/num_points;

            System.out.println("New centroid:");
            for (Double coordinata : new_centroid.getCoordinates()) {
                System.out.println(coordinata+"  ");
            }
            context.write(key, new Text(new_centroid.getCoordinates() +";"+distanza_media));

        }
    }


}
