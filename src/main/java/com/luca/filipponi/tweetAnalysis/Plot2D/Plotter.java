package com.luca.filipponi.tweetAnalysis.Plot2D;


import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYDotRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * The class plotter has method for plot 2D geographical clustering (latitude and longitude)
 *
 * @author luca
 */


public class Plotter extends ApplicationFrame {

    static String csvFile; //csv file with cluster
    static int clusterNumber;

    public Plotter(String s, String file) {
        super(s);
        csvFile = file;

        JPanel jpanel = createDemoPanel();
        jpanel.setPreferredSize(new Dimension(2000, 1500));

        setContentPane(jpanel);
    }

    /**
     * Auxiliary method for Plotter.print, inside there is the defining of the dot size
     *
     * @return JPanel
     */
    public static JPanel createDemoPanel() {

        JFreeChart jfreechart = ChartFactory.createScatterPlot("Geographical Clustering",
                "latitude", "longitude", dataset(), PlotOrientation.VERTICAL, true, true, true);


        XYPlot xyPlot = (XYPlot) jfreechart.getPlot();

        XYDotRenderer xydotrenderer = new XYDotRenderer();
        xyPlot.setRenderer(xydotrenderer);
        xydotrenderer.setDotHeight(2);
        xydotrenderer.setDotWidth(2);


        xyPlot.setDomainCrosshairVisible(true);
        xyPlot.setRangeCrosshairVisible(true);

        return new ChartPanel(jfreechart);
    }

    private static XYDataset dataset() {


        //qui dovrei mettercila lettura del sequence file di output per la stampa


        //qui ci metto la lettura del file txt di output del clustering
        //dovrei inizializzare un numero di series pari al numero di clustering
        try {
            int k = 0;
            XYSeriesCollection xySeriesCollection = new XYSeriesCollection();
            XYSeries series = null;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));

            String line;
            try {
                while ((line = br.readLine()) != null) {
                    if (line.equals("")) {
                        //end of the cluster there is only statistical data
                        break;
                    }

                    String[] parsedLine = line.split(",");
                    //in questo modo ho tutti i valore

                    for (String s : parsedLine) {
                        String[] parsedCoord = s.split("_");

                        if (parsedCoord.length == 1) {


                            if (series != null) {
                                xySeriesCollection.addSeries(series);

                            }
                            //creo un nuovo cluster
                            //trova una soluzione migliore per le ultime 4 righe
                            String clusterName = "Cluster".concat(Integer.toString(k));
                            System.out.println("Adding point of :" + clusterName);
                            series = new XYSeries(clusterName);
                            k++;


                        } else {

						/*
                        System.out.println("lunghezza della stringa:"+parsedCoord.length);
						for(int i=0;i<parsedCoord.length;i++){
							
							System.out.print("Indice: "+i+"valore: "+parsedCoord[i]+" ");
						}
						System.out.println();
						
						System.out.println("Valore elemento in [0]: "+parsedCoord[0]);
						*/
                            //double z=Double.parseDouble(parsedCoord[0]);
                            double x = Double.parseDouble(parsedCoord[2]);
                            double y = Double.parseDouble(parsedCoord[4]);
                    /*
                    System.out.print("valore x: "+x);
					System.out.print(" ");
					System.out.print("Valore y: "+y);
					System.out.println();
					
					*/
                            series.add(x, y);
                        }
                    }


                }
                xySeriesCollection.addSeries(series);


                br.close();
                return xySeriesCollection;
            } catch (IOException e) {
                System.out.println("Excption from realing a line in csv file");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error opening Clustering output in txt format, file missing");
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Create a 2D plot from a csv file (exported from mahout cluster dumper only and with [lat,long] value)
     *
     * @param windowName name of the plot windows
     * @param csvFile    csv file with clustering exported from mahout
     */


    public static void print(String windowName, String csvFile) {

        Plotter plot = new Plotter(windowName, csvFile);
        plot.pack();
        RefineryUtilities.centerFrameOnScreen(plot);
        plot.setVisible(true);


    }


}