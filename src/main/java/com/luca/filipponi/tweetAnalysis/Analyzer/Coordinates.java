package com.luca.filipponi.tweetAnalysis.Analyzer;


/**
 * Class used for represent a Coordinates Object
 */

public class Coordinates {

    double latitude;
    double longitude;

    public Coordinates(double lat, double lon) {

        this.latitude = lat;
        this.longitude = lon;

    }
    //add get and set method

    //return a double[2] array with coordinates
    public double[] getCoordinates() {

        double vet[] = new double[2];

        vet[0] = this.latitude;
        vet[1] = this.longitude;

        return vet;

    }

    //toString method for debugging purpose
    public String toString() {

        //return a string with lat e long value
        return "Lat:" + Double.toString(this.latitude) + " Lon:" + Double.toString(this.longitude);

    }

}
