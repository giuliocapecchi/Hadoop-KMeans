package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class PythonScriptExecutor {

    public static void PythonExecutor(String scriptPath, int numero_iterazione, String centroids) {
        try {
            // Crea l'array di argomenti da passare allo script PythoN
            String[] centroidArray = centroids.split(";");
            String[] pythonArgs = new String[centroidArray.length + 3];
            pythonArgs[0] = "python3";
            pythonArgs[1] = scriptPath;
            pythonArgs[2] = Integer.toString(numero_iterazione);
            System.arraycopy(centroidArray, 0, pythonArgs, 3, centroidArray.length);


            Process process = Runtime.getRuntime().exec(pythonArgs);

            // Cattura l'output dello script Python
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Cattura l'output degli errori dello script Python
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String errorLine;
            while ((errorLine = errorReader.readLine()) != null) {
                System.err.println(errorLine);
            }

            // Attendi la terminazione dello script Python
            int exitCode = process.waitFor();
            System.out.println(scriptPath+" è terminato con codice di uscita: " + exitCode);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
