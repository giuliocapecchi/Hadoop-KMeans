package org.example;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class Point {
    private final ArrayList<Double> coordinate;

    private int cluster;
    private double min_distance;

    public Point(Text coordinate) {
        this.coordinate = parseCoordinates(coordinate);
    }

    public ArrayList<Double> getCoordinates() {
        return coordinate;
    }

    private ArrayList<Double> parseCoordinates(Text coordinate) {
        ArrayList<Double> coordinateList = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(coordinate.toString(), ",");

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            double coordinateValue = Double.parseDouble(token);
            coordinateList.add(coordinateValue);
        }

        return coordinateList;
    }
}
