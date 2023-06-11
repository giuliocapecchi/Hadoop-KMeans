package org.example;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class Point {
    private final ArrayList<Double> coordinate;

    private int cluster=-1;
    private double min_distance=-1;

    public Point(Text coordinate) {
        this.coordinate = parseCoordinates(coordinate);
    }

    public Point(ArrayList<Double> coordinate) {
        this.coordinate = new ArrayList<>(coordinate);
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

    public double calculateDistance(Point otherPoint) {
        ArrayList<Double> otherCoordinates = otherPoint.getCoordinates();

        if (coordinate.size() != otherCoordinates.size()) {
            throw new IllegalArgumentException("The dimensions of the points are not the same.");
        }

        double squaredSum = 0.0;
        for (int i = 0; i < coordinate.size(); i++) {
            double diff = coordinate.get(i) - otherCoordinates.get(i);
            squaredSum += diff * diff;
        }

        return Math.sqrt(squaredSum);
    }

    public Point sum(Point other) {
        ArrayList<Double> sumCoordinates = new ArrayList<>();

        // Verifica che i due punti abbiano lo stesso numero di coordinate
        if (this.coordinate.size() != other.coordinate.size()) {
            throw new IllegalArgumentException("I due punti devono avere lo stesso numero di coordinate.");
        }

        // Somma delle coordinate
        for (int i = 0; i < this.coordinate.size(); i++) {
            double sum = this.coordinate.get(i) + other.coordinate.get(i);
            sumCoordinates.add(sum);
        }

        // Crea un nuovo oggetto Point con le coordinate sommate
        return new Point(sumCoordinates);
    }

    public double getMin_distance() {
        return min_distance;
    }

    public void setMin_distance(double min_distance) {
        this.min_distance = min_distance;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public void setCoordinate(int i,double value) {
        this.coordinate.set(i,value);
    }
}
