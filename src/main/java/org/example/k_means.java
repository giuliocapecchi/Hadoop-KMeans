package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class k_means {
    public static class kmeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        public static int dimensione = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
            if(dimensione==0) {
                dimensione =  tokenizer.countTokens();
                System.out.println("dimensione del punto:"+dimensione+"\n");
            }



            int i = 0;
            while(i<dimensione){
                String coordinata = tokenizer.nextToken().trim();
                punto.add(coordinata);
            }


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


        }
    }



}
