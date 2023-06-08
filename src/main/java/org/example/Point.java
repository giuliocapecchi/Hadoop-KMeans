package org.example;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Point {
    private final ArrayList<Double> coordinate;

    public Point(String coordinate) {
        this.coordinate = parseCoordinates(coordinate);
    }

    public ArrayList<Double> getCoordinate() {
        return coordinate;
    }

    private ArrayList<Double> parseCoordinates(String coordinate) {
        ArrayList<Double> coordinateList = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(coordinate, ",");

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            double coordinateValue = Double.parseDouble(token);
            coordinateList.add(coordinateValue);
        }

        return coordinateList;
    }
}
