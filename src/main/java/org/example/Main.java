package org.example;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static org.example.PythonScriptExecutor.PythonExecutor;


public class Main {

    static ArrayList<String> centroidi = new ArrayList<>();
    static String path = "coordinates.txt";

    static String LogPath = "output/log_distances.txt";

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
        //
        double threshold = 0.1;
        int max_iter = 1000;
        //
        if(deleteOldFile(LogPath)==1)
            System.exit(1);


        // Generazione casuale degli indici per i centroidi iniziali
        Random random = new Random();

        int numPoints = getNumPoints(); // Metodo per ottenere il numero di punti dal file di input

        ArrayList<Integer> indexes  = new ArrayList<>();


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

        int numero_iterazioni;

        //PythonExecutor("./python_scripts/plot_generator.py",0, String.join(";", centroidi));


        for(numero_iterazioni=0; numero_iterazioni<max_iter;numero_iterazioni++){

            System.out.println("iterazione numero: "+numero_iterazioni);

            String centroidCoordinatesString = String.join(";", centroidi);
            System.out.println(centroidCoordinatesString);

            Job job = Job.getInstance(conf, "kmeans_iter_"+numero_iterazioni);

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
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+numero_iterazioni));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //System.exit(job.waitForCompletion(true) ? 0 : 1);

            job.waitForCompletion(true);

            // Esempio di recupero dei dati scritti nel contesto principale
            FileSystem fs = FileSystem.get(conf);
            Path outputPath = new Path("output"+numero_iterazioni+"/part-r-00000");  // Path al file di output del job
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));

            String nuovi_centroidi = "";
            String line;
            boolean finito = true;


            writeToFile(String.valueOf(numero_iterazioni), LogPath);


            StringBuilder sb = new StringBuilder();
            // Itera attraverso gli elementi dell'ArrayList
            for (String elemento : centroidi) {
                sb.append(elemento);  // Aggiungi l'elemento alla stringa
                sb.append(";");       // Aggiungi il separatore ";"
            }
            // Rimuovi l'ultimo ";" dalla stringa risultante se necessario
            if (!centroidi.isEmpty()) {
                sb.setLength(sb.length() - 1);
            }
            String old_centroidiStringhe = sb.toString();



            while ((line = br.readLine()) != null) {
                // Divide la stringa delle coordinate dei centroidi in un array di stringhe
                String[] context_informations = line.split(";");

                int num_cluster = Integer.parseInt(context_informations[0].trim());
                String coordinateString = context_informations[1].substring(1, context_informations[1].length() - 1);

                // for the new MapReduce centroids
                nuovi_centroidi = nuovi_centroidi+coordinateString+";";

                Point old_centroid = new Point(centroidi.get(num_cluster-1));
                Point new_centroid = new Point(coordinateString);
                double distance = old_centroid.calculateDistance(new_centroid);

                System.out.println("Centroid number "+num_cluster+" movement : "+distance);

                //Writing on the log
                writeToFile(num_cluster+":"+distance, LogPath);

                //update the centroids
                centroidi.set(num_cluster-1,coordinateString);

                if(distance > threshold)
                    finito = false;

            }
            br.close();

            nuovi_centroidi = nuovi_centroidi.substring(0, nuovi_centroidi.length() - 1);
            System.out.println("concatenazione"+nuovi_centroidi);

            if(finito)
                break;

            // if(dimensione == 2)
            PythonExecutor("./python_scripts/plot_generator.py",numero_iterazioni,old_centroidiStringhe);

        }

        System.out.println("K-means converged at iteration "+numero_iterazioni);
        System.exit(0);

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

    public static void writeToFile(String content,String Path){


        try (FileWriter fileWriter = new FileWriter(Path, true);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            bufferedWriter.write(content);
            bufferedWriter.newLine();


        } catch (IOException e) {
            System.out.println("An error occurred while appending to the file: " + e.getMessage());
        }
    }

    public static int deleteOldFile(String filePath){

        File file = new File(filePath);

        if (file.exists()) {
            if (file.delete()) {
                System.out.println("Log of the distances deleted successfully.");
                return 0;
            } else {
                System.out.println("Failed to delete the file.");
                return 1;
            }
        }else{
            return 0;
        }
    }



}