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

        private static int dimensione = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Point punto = new Point(value);
            ArrayList<Double> coordinatePunto = punto.getCoordinates();
            System.out.println("Coordinate del punto: " + coordinatePunto);

            if(dimensione==0) {
                dimensione =  punto.getCoordinates().size();
                System.out.println("dimensione del punto:"+dimensione+"\n");
            }


/*
            String generated_key;
            String generated_value;

            int rows = 4;

            if (nome_matrice.equals("M")) {
                int k = 0;
                while (k < rows) {
                    if (Double.parseDouble(cell_value) == 0) {
                        k++;
                        continue;
                    }
                    generated_key = index_row + "," + k;
                    outputKey.set(generated_key);

                    generated_value = nome_matrice + "," + index_column + "," + cell_value;
                    outputValue.set(generated_value);

                    System.out.println(generated_key + ":" + generated_value);
                    context.write(outputKey, outputValue);

                    k++;

                }
            } else if (nome_matrice.equals("N")) {
                int i = 0;
                while (i < rows) {
                    if (Double.parseDouble(cell_value) == 0) {
                        i++;
                        continue;
                    }
                    generated_key = i + "," + index_column;
                    outputKey.set(generated_key);

                    generated_value = nome_matrice + "," + index_row + "," + cell_value;
                    outputValue.set(generated_value);

                    System.out.println(generated_key + ":" + generated_value);
                    context.write(outputKey, outputValue);
                    i++;
                }
            }
*/

        }
    }

    public static class kmeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
