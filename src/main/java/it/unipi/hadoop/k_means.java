package it.unipi.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;

public class k_means {



    public static class kmeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {

        private final IntWritable outputKey = new IntWritable();

       static ArrayList<PointWritable> centroidi = new ArrayList<>();

        private static int k = 0;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String centroidCoordinatesString = conf.get("centroidCoordinates");
            // Divide la stringa delle coordinate dei centroidi in un array di stringhe
            String[] centroidCoordinatesArray = centroidCoordinatesString.split(";");

            // Converte ciascuna stringa in un PointWritable e inserisce nell'array
            for (int i = 0; i < centroidCoordinatesArray.length; i++) {
                centroidi.add(new PointWritable(centroidCoordinatesArray[i]));
            }

            k = centroidi.size();

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            PointWritable punto = new PointWritable(value.toString());
            for (int i=0; i<k; i++){
                double dist = punto.calculateDistance(centroidi.get(i));
                //System.out.println("Distanza dal cluster "+i+":"+dist+"\n");
                if(dist<punto.getMin_distance() || punto.getMin_distance()==-1){
                    punto.setMin_distance(dist);
                    punto.setCluster(i+1);
                }
            }
            System.out.println("Stampa classe punto. Coordinate del punto: " + punto.getCoordinates()+"\n");
            System.out.println(" Min_distance:"+punto.getMin_distance()+", cluster: " +punto.getCluster()+"\n");

            outputKey.set(punto.getCluster());
            context.write(outputKey, punto);
        }
    }

    public static class kmeansCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int dimensione = conf.getInt("d",-1);
            ArrayList<Double> zeros = new ArrayList<>(Collections.nCopies(dimensione, 0.0));
            PointWritable agglomerate = new PointWritable(zeros);
            agglomerate.setWeight(0);

            System.out.println("COMBINER STARTED! dimensione = "+dimensione );
            for (PointWritable punto : values) {
                agglomerate.sum(punto);
                int weight = agglomerate.getWeight() + punto.getWeight();
                agglomerate.setWeight(weight);

            }

            System.out.println("weight:"+agglomerate.getWeight());

            context.write(key,agglomerate);
        }
    }

    public static class kmeansReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
        private static int dimensione = 0;

        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            dimensione = conf.getInt("d", -1);
        }

        @Override
        public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {

            System.out.println("Chiave: "+ key.toString()+"\n");

           /*if(dimensione==0) {
                Configuration conf = context.getConfiguration();
                dimensione = conf.getInt("d", -1);
            }*/

            // int cluster_number = key.get() - 1 ;

            System.out.println("dimensione nel reducer:"+dimensione);
            ArrayList<Double> zeros = new ArrayList<>(Collections.nCopies(dimensione, 0.0));

            PointWritable new_centroid = new PointWritable(zeros);

            int num_points = 0;
            for (PointWritable punto : values) {
                new_centroid.sum(punto);
                //modifica
                num_points += punto.getWeight();
            }

            System.out.println("num points : "+num_points);

            int i = 0;

            while (i<dimensione){
                Double average = new_centroid.getCoordinates().get(i)/num_points;
                System.out.println("new coordinate "+i+":"+average+"\n");
                System.out.println("coordinata singola"+new_centroid.getCoordinates().get(i));
                new_centroid.setCoordinate(i,average);
                i++;
            }

            System.out.println("New centroid:");
            for (Double coordinata : new_centroid.getCoordinates()) {
                System.out.println(coordinata+"  ");
            }

            context.write(key,new_centroid);
        }
    }


}
