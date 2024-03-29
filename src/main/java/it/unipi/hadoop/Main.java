package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {

    static ArrayList<String> centroidi = new ArrayList<>();
    static String path = "coordinates.txt";
    static String LogPath = "output/log_distances.txt";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4 && otherArgs.length != 5) {
            System.err.println("Usage: kmeans <input> <output> <k> <d> (optional)<threshold>");
            System.exit(1);
        }

        System.out.println("args[0]: <input>=" + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);
        System.out.println("args[2]: <k>=" + otherArgs[2]);
        System.out.println("args[3]: <d>=" + otherArgs[3]);

        int max_iter = 100;
        double threshold = 0.1;

        if(otherArgs.length == 5) {
            System.out.println("args[4]: <threshold>=" + otherArgs[4]);
            threshold = Double.parseDouble(otherArgs[4]);
        }
        int k = Integer.parseInt(otherArgs[2]);
        if (k>100){
            System.out.println("K deve essere minore o uguale di 100");
            System.exit(1);
        }
        int d = Integer.parseInt(otherArgs[3]);

        if(deleteOldFile(LogPath)==1)
            System.exit(1);

        deleteOldPlots();

        // Generazione casuale degli indici per i centroidi iniziali
        Random random = new Random();

        // Metodo per ottenere il numero di punti dal file di input
        int numPoints = getNumPoints();

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
            System.out.println("Errore nella scelta casuale dei centroidi!\n");
            System.exit(1);
        }

        int numero_iterazioni;

        long startTime = System.currentTimeMillis();

        for(numero_iterazioni=0; numero_iterazioni<max_iter;numero_iterazioni++){

            System.out.println("iterazione numero: "+numero_iterazioni);

            String centroidCoordinatesString = String.join(";", centroidi);

            Job job = Job.getInstance(conf, "kmeans_iter_"+numero_iterazioni);

            // Un riduttore per ogni cluster
            job.setNumReduceTasks(k);

            // Imposta le variabili di configurazione per il numero di cluster/centroidi e gli indici dei centroidi iniziali
            job.getConfiguration().setInt("k", k);
            job.getConfiguration().setInt("d", d);
            job.getConfiguration().set("centroidCoordinates", centroidCoordinatesString);

            job.setJarByClass(k_means.class);
            job.setMapperClass(k_means.kmeansMapper.class);
            job.setReducerClass(k_means.kmeansReducer.class);
            job.setCombinerClass(k_means.kmeansCombiner.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(PointWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PointWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+numero_iterazioni));

            job.setInputFormatClass(TextInputFormat.class);

            if(!job.waitForCompletion(true)){
                System.out.println("Job failed at iteration : "+numero_iterazioni+"\n");
                System.exit(1);
            }

            boolean finito = true;
            // Scrittura nel log
            writeToFile(String.valueOf(numero_iterazioni+1), LogPath);

            // Costruzione stringa per i vecchi centroidi
            StringBuilder sb = new StringBuilder();
            // Itera attraverso gli elementi dell'ArrayList di stringhe
            for (String elemento : centroidi) {
                sb.append(elemento);
                sb.append(";");
            }

            // Rimuovi l'ultimo ";" dalla stringa risultante
            sb.setLength(sb.length() - 1);
            String old_centroidiStringhe = sb.toString();

            // Recupero dei dati scritti in output
            for(int i=0; i<k; i++){
                String path = "output"+numero_iterazioni+"/part-r-";
                if(i<10){
                    path = path + "0000"+i;
                }else if(i<100){
                    path = path + "000"+i;
                }

                FileSystem fs = FileSystem.get(conf);
                Path outputPath = new Path(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
                String line;

                while ((line = br.readLine()) != null) {
                    // Divide la stringa delle coordinate dei centroidi in un array di stringhe
                    String[] context_informations = line.split("\t");
                    int num_cluster = Integer.parseInt(context_informations[0].trim());
                    PointWritable old_centroid = new PointWritable(centroidi.get(num_cluster-1));
                    PointWritable new_centroid = new PointWritable(context_informations[1]);
                    double distance = old_centroid.calculateDistance(new_centroid);

                    System.out.println("Centroid number "+num_cluster+" movement : "+distance);
                    //Scrivo sul log
                    writeToFile(num_cluster+":"+distance, LogPath);
                    //Aggiorno i centroidi per la prossima iterazione
                    centroidi.set(num_cluster-1,context_informations[1]);
                    if(distance > threshold)
                        finito = false;
                }
                br.close();
            }

            // se le dimensioni sono più di due e i punti sono troppi non creo il plot
            if (d == 2 && numPoints < 1000) {
                PythonScriptExecutor.PythonExecutor("./python_scripts/plot_generator.py", numero_iterazioni, old_centroidiStringhe);
            }

            if(finito)
                break;
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        //Scrivo il tempo di esecuzione sul log
        writeToFile("Execution time: "+duration/1000+" seconds",LogPath);
        System.out.println("K-means converged at iteration "+numero_iterazioni+",with time: "+duration/1000);
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
                System.out.println(filePath+" deleted successfully.");
                return 0;
            } else {
                System.out.println("Failed to delete the file.");
                return 1;
            }
        }else{
            return 0;
        }
    }

    public static void deleteOldPlots() {
        String cartella = "plots"; // Sostituisci con il percorso corretto della cartella
        // Oggetto File per la cartella
        File directory = new File(cartella);
        // Verifica se la cartella esiste
        if (directory.exists() && directory.isDirectory()) {
            // Ottengo l'elenco dei file nella cartella
            File[] files = directory.listFiles();
            // Eliminazione di tutti i file
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        file.delete();
                    }
                }
                System.out.println("Deleted old plots");
            }
        } else {
            System.out.println("Error in deleting the old plots folder");
        }
    }

}