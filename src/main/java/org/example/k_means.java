package org.example;

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
        private static int dimensione = 0;


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            PointWritable punto = new PointWritable(value.toString());

            // setup method potenzialmente !!!!
            if(dimensione==0) {
                dimensione =  punto.getCoordinates().size();
                //System.out.println("dimensione del punto:"+dimensione+"\n");
                // la prima volta recupero anche i centroidi
                Configuration conf = context.getConfiguration();
                String centroidCoordinatesString = conf.get("centroidCoordinates","");

                // Divide la stringa delle coordinate dei centroidi in un array di stringhe
                String[] centroidCoordinatesArray = centroidCoordinatesString.split(";");

                // Converte ciascuna stringa in un oggetto Text e inserisce nell'array
                for (int i = 0; i < centroidCoordinatesArray.length; i++) {

                    centroidi.add(new PointWritable(centroidCoordinatesArray[i]));

                }

                //System.out.println("Stampa dei centroidi nuovi:");
                for (int i=0; i<centroidi.size();i++){
                    ArrayList<Double> coordinateCentroide = centroidi.get(i).getCoordinates();
                   // System.out.println("Coordinate del centroide "+i+": " + coordinateCentroide+"\n");
                }
            }


            for (int i=0; i<centroidi.size();i++){
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

    public static class kmeansReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

        static ArrayList<PointWritable> centroidi = new ArrayList<>();
        private static int dimensione = 0;

        @Override
        public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {

            System.out.println("Chiave: "+ key.toString()+"\n");

            /* for (Text value : values) {
                System.out.println(value+" ");
            }*/

            if(dimensione==0) {
                //System.out.println("dimensione del punto:"+dimensione+"\n");
                // la prima volta recupero anche i centroidi
                Configuration conf = context.getConfiguration();
                String centroidCoordinatesString = conf.get("centroidCoordinates");

                // Divide la stringa delle coordinate dei centroidi in un array di stringhe
                String[] centroidCoordinatesArray = centroidCoordinatesString.split(";");

                // Converte ciascuna stringa in un oggetto Text e inserisce nell'array
                for (int i = 0; i < centroidCoordinatesArray.length; i++) {

                    centroidi.add(new PointWritable(centroidCoordinatesArray[i]));
                }

                dimensione =  centroidi.get(0).getCoordinates().size();

            }


            // int cluster_number = key.get() - 1 ;

            //System.out.println("size di centroidi: "+centroidi.size());

            ArrayList<Double> zeros = new ArrayList<>(Collections.nCopies(dimensione, 0.0));

            PointWritable new_centroid = new PointWritable(zeros);


            // cambiare
            int num_points = 0;

            for (PointWritable punto : values) {
                new_centroid.sum(punto);
                //modifica
                num_points += punto.getWeight();
            }

            //elimina
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

    public class kmeansCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int dimensione = conf.getInt("d",-1);
            ArrayList<Double> zeros = new ArrayList<>(Collections.nCopies(dimensione, 0.0));
            PointWritable agglomerate = new PointWritable(zeros);

            for (PointWritable punto : values) {
                agglomerate.sum(punto);
                int weight = agglomerate.getWeight() + punto.getWeight();
                agglomerate.setWeight(weight);
            }
            context.write(key,agglomerate);
        }
    }




}
