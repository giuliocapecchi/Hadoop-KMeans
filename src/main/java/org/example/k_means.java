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
    public static class kmeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        ArrayList<Point> centroidi = new ArrayList<>();

        private static int dimensione = 0;

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

            System.out.println("Chiave: "+ key.toString()+"\n");


            for (Text value : values) {
                System.out.println(value+" ");
            }


            /*
            String[] keyParts = key.toString().split(",");
            int i = Integer.parseInt(keyParts[0].trim());
            int k = Integer.parseInt(keyParts[1].trim());

            List<String> mList = new ArrayList<>();
            List<String> nList = new ArrayList<>();

            for (Text value : values) {
                String triplet = value.toString().trim();
                String firstElement = triplet.split(",")[0].trim();

                if (firstElement.equals("M")) {
                    mList.add(triplet);
                } else if (firstElement.equals("N")) {
                    nList.add(triplet);
                }
            }

            Comparator<String> comparator = new Comparator<String>() {
                @Override
                public int compare(String triplet1, String triplet2) {
                    double secondElement1 = Double.parseDouble(triplet1.split(",")[1].trim());
                    double secondElement2 = Double.parseDouble(triplet2.split(",")[1].trim());

                    return Double.compare(secondElement1, secondElement2);
                }
            };

            mList.sort(comparator);
            nList.sort(comparator);

            // Stampa delle liste ordinate

            System.out.print("chiave: ("+i + "," + k+")\n");
            System.out.println("Lista M ordinata:");
            for (String triplet : mList) {
                System.out.println(triplet);
            }

            System.out.println("Lista N ordinata:");
            for (String triplet : nList) {
                System.out.println(triplet);
            }


            // Esecuzione della moltiplicazione
            double somma = 0;

            for (String mTriplet : mList) {
                String[] mParts = mTriplet.split(",");
                int j = Integer.parseInt(mParts[1].trim());
                double mValue = Double.parseDouble(mParts[2].trim());

                for (String nTriplet : nList) {
                    String[] nParts = nTriplet.split(",");
                    int nJ = Integer.parseInt(nParts[1].trim());
                    double nValue = Double.parseDouble(nParts[2].trim());

                    if (j == nJ) {
                        double result = mValue * nValue;
                        somma += result;
                    }
                }
            }

            DoubleWritable risultato_out = new DoubleWritable();
            risultato_out.set(somma);
            context.write(key, new Text(Double.toString(risultato_out.get())));

            */

        }
    }


}
