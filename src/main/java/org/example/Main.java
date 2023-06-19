package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
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


public class Main {

    static ArrayList<String> centroidi = new ArrayList<>();
    static String path = "coordinates.txt";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: kmeans <input> <output> <k>");
            System.exit(1);
        }

        System.out.println("args[0]: <input>=" + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);
        System.out.println("args[2]: <k>=" + otherArgs[2]);

        int k = Integer.parseInt(otherArgs[2]);

        // Generazione casuale degli indici per i centroidi iniziali
        Random random = new Random();

        int numPoints = getNumPoints(); // Metodo per ottenere il numero di punti dal file di input

        ArrayList<Integer> indexes  = new ArrayList<>();
        ArrayList<Double>  average_distances = new ArrayList<>();
        ArrayList<Double>  epsilon = new ArrayList<>();

        for (int i = 0; i < k ; i++) {
            average_distances.add(-1.0);
        }

        while (indexes.size() < k) {
            int random_number = random.nextInt(numPoints);
            if (!indexes.contains(random_number)) {
                indexes.add(random_number);
            }
        }

        // Ordina la lista in maniera crescente
        Collections.sort(indexes);

        if(getLines(indexes)==1){
            System.out.println("Errore nella scelta casuale dei centroidi!!!\n");
            return;
        }

        System.out.println("numero di punti:"+numPoints);
        System.out.println("Stampa dei centroidi:");

        String centroidCoordinatesString = String.join(";", centroidi);
        System.out.println(centroidCoordinatesString);

        Job job = Job.getInstance(conf, "kmeans");

        job.setJarByClass(k_means.class);
        job.setMapperClass(k_means.kmeansMapper.class);
        job.setReducerClass(k_means.kmeansReducer.class);

        // Imposta le variabili di configurazione per il numero di cluster/centroidi e gli indici dei centroidi iniziali
        job.getConfiguration().setInt("k", k);
        job.getConfiguration().set("centroidCoordinates", centroidCoordinatesString);


        //job.setNumReduceTasks(3); // OCIO NON WORKA??????

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        job.waitForCompletion(true);

        // Esempio di recupero dei dati scritti nel contesto principale
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("output/part-r-00000");  // Path al file di output del job
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));

        String nuovi_centroidi = "";
        String line;

        while ((line = br.readLine()) != null) {
            // Divide la stringa delle coordinate dei centroidi in un array di stringhe
            String[] context_informations = line.split(";");

            int num_cluster = Integer.parseInt(context_informations[0].trim());
            String coordinateString = context_informations[1].substring(1, context_informations[1].length() - 1);
            double mean_distance = Double.parseDouble(context_informations[2].trim());

            nuovi_centroidi = nuovi_centroidi+coordinateString+";";

            //System.out.println("Dati scritti nel contesto: cluster: "+num_cluster+"\nMean distance:"+mean_distance+"\ncoordinate centroide nuovo:"+coordinateString+"\n");
            if(average_distances.get(0)==-1){
                average_distances.add(num_cluster-1,mean_distance);
            }else{

            }


        }
        br.close();

        nuovi_centroidi = nuovi_centroidi.substring(0, nuovi_centroidi.length() - 1);
        System.out.println("concatenazione"+nuovi_centroidi);



    }


    public static int getLines(ArrayList<Integer> lineIndexes) {
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {

            String line;
            int currentLine = 0;
            int i = 0 ;

            while ((line = reader.readLine()) != null) {

                if (currentLine == lineIndexes.get(i)) {
                    centroidi.add(line);
                    i++;
                    if(i == lineIndexes.size()) {
                        return 0;
                    }
                }

                currentLine++;
            }
            return 0;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return 1; // Riga non trovata
    }


    public static int getNumPoints() {
        int numPoints = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            while (reader.readLine() != null) {
                numPoints++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return numPoints;
    }



}