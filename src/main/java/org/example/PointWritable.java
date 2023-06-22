package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.ArrayList;
import java.util.StringTokenizer;


public class PointWritable implements Writable {

    private ArrayList<Double> coordinate;

    private int cluster=-1;
    private double min_distance=-1;

   private int weight = 1;

    // costruttore default
    public PointWritable(){
    }

    // costruttore di copia
    public PointWritable(PointWritable point){

        this.cluster = point.cluster;
        this.min_distance = point.min_distance;
        this.coordinate = point.coordinate;
    }

    public PointWritable(Text coordinate) {
        this.coordinate = parseCoordinates(coordinate);
    }

    public PointWritable(ArrayList<Double> coordinate) {
        this.coordinate = new ArrayList<>(coordinate);
    }

    public PointWritable(String coordinateString) {
        this.coordinate = parseCoordinates(coordinateString);
    }

    public void sum(PointWritable other) {

        // Verifica che i due punti abbiano lo stesso numero di coordinate
        if (this.coordinate.size() != other.coordinate.size()) {
            throw new IllegalArgumentException("I due punti devono avere lo stesso numero di coordinate.");
        }

        // Somma delle coordinate
        for (int i = 0; i < this.coordinate.size(); i++) {
            Double sum = this.coordinate.get(i) + other.coordinate.get(i);
            this.setCoordinate(i,sum);

        }

    }

    public double calculateDistance(PointWritable otherPoint) {

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

    private ArrayList<Double> parseCoordinates(Text coordinate) {
        ArrayList<Double> coordinateList = new ArrayList<>();

        String coordinateString = coordinate.toString().trim();

        // Rimuovi "[" e "]" dalla stringa di coordinate
        coordinateString = coordinateString.substring(1, coordinateString.length() - 1);


        StringTokenizer tokenizer = new StringTokenizer(coordinateString, ",");

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            double coordinateValue = Double.parseDouble(token);
            coordinateList.add(coordinateValue);
        }

        return coordinateList;
    }

    private ArrayList<Double> parseCoordinates(String coordinateString) {
        ArrayList<Double> coordinateList = new ArrayList<>();

        coordinateString = coordinateString.trim();

        StringTokenizer tokenizer = new StringTokenizer(coordinateString, ",");

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            System.out.println("token : "+token);
            double coordinateValue = Double.parseDouble(token);
            coordinateList.add(coordinateValue);
        }

        return coordinateList;
    }

    public ArrayList<Double> getCoordinates() {
        return coordinate;
    }

    public int getCluster() {
        return cluster;
    }

    public double getMin_distance() {
        return min_distance;
    }

    public void setCoordinate(int i,Double value) {
        this.coordinate.set(i,value);
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public void setMin_distance(double min_distance) {
        this.min_distance = min_distance;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }


    // write and read methods required by the Writable interface
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(coordinate.size());
        for (double coordinata : coordinate) {
            out.writeDouble(coordinata);
        }
        out.writeInt(cluster);
        out.writeDouble(min_distance);
        out.writeInt(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        coordinate = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            coordinate.add(in.readDouble());
        }
        cluster = in.readInt();
        min_distance = in.readDouble();
        weight = in.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (double coordinata : coordinate) {
            sb.append(coordinata).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

}
