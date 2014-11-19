package com.luca.filipponi.tweetAnalysis.JsonGraph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by luca on 28/07/14.
 */
public class JsonGraphWriter {


    public static void main(String args[]) throws IOException {


        //create a graoh object


//        BufferedWriter wr= new BufferedWriter(new FileWriter("src/main/java/D3Visualize/Graph.json"));

//        String node1="{\"name\":\"Umberto\"}";
//        String node2="{\"name\":\"Emanule\"}";
//        String node3="{\"name\":\"Luca\"}";
//        String node4="{\"name\":\"Jacopo\"}";
//        String node5="{\"name\":\"Roberto\"}";
//        String node6="{\"name\":\"Amedeo\"}";
//        String node7="{\"name\":\"Maurizio\"}";
//
//
//        List<String> nodeList=new ArrayList<String>();
//
//
//        nodeList.add(node1);
//        nodeList.add(node2);
//        nodeList.add(node3);
//        nodeList.add(node4);
//        nodeList.add(node5);
//        nodeList.add(node6);
//        nodeList.add(node7);
//
//
//
//        String edge1="{\"source\":0,\"target\":1,\"value\":5}";
//        String edge2="{\"source\":1,\"target\":2,\"value\":6}";
//        String edge3="{\"source\":1,\"target\":3,\"value\":2}";
//        String edge4="{\"source\":1,\"target\":4,\"value\":11}";
//        String edge5="{\"source\":1,\"target\":5,\"value\":6}";
//        String edge6="{\"source\":2,\"target\":3,\"value\":3}";
//
//
//
//        List<String> edgeList=new ArrayList<String>();
//
//
//        edgeList.add(edge1);
//        edgeList.add(edge2);
//        edgeList.add(edge3);
//        edgeList.add(edge4);
//        edgeList.add(edge5);
//        edgeList.add(edge6);
//
//
//
//
//        writeGraph(nodeList,edgeList,wr);
//


        BufferedWriter wr = new BufferedWriter(new FileWriter("src/main/java/D3Visualize/Graph2.json"));


        JsonGraph grafo = new JsonGraph();

        grafo.addNode("luca", "1");
        grafo.addNode("gianni", "2");
        grafo.addNode("apollo", "3");
        grafo.addNode("michele", "1");


        grafo.addEdge("0", "1", "3");
        grafo.addEdge("0", "2", "2");
        grafo.addEdge("2", "3", "5");
        grafo.addEdge("3", "1", "13");


        writeJsonGraph(grafo, wr);


    }

    /**
     * Writes a Json Graph object.
     *
     * @param graph JsonGraph Object to be write in a json file
     * @param wr    BufferedWriter, should point to a .json file
     * @throws java.io.IOException
     */


    public static void writeJsonGraph(JsonGraph graph, BufferedWriter wr) throws IOException {


        writeGraph(graph.getNodeList(), graph.getEdgeList(), wr);


    }


    public static void writeGraph(List<String> nodeList, List<String> edgeList, BufferedWriter wr) throws IOException {

        //apertura json
        wr.write("{");


        //apertura array di nodi
        wr.write("\"nodes\":[");

        addNodes(nodeList, wr);


        //chiusura array di nodi e apertura array di archi
        wr.write("],\"links\":[");


        addEdges(edgeList, wr);

        //chiusura array
        wr.write("]");
        //chiusura json
        wr.write("}");


        wr.close();

    }


    public static void addNodes(List<String> nodeList, BufferedWriter wr) throws IOException {

//        for( String e : nodeList){
//
//            wr.write(e);
//
//
//            wr.write(",");//separatore tra gli elementi
//
//        }


        for (int i = 0; i < nodeList.size(); i++) {

            wr.write(nodeList.get(i));

            if (!(i == nodeList.size() - 1)) //
                wr.write(",");//separatore tra gli elementi solo se non sta in ultima posizione


        }

    }

    public static void addEdges(List<String> edgeList, BufferedWriter wr) throws IOException {


        for (int i = 0; i < edgeList.size(); i++) {

            wr.write(edgeList.get(i));

            if (!(i == edgeList.size() - 1)) //
                wr.write(",");//separatore tra gli elementi solo se non sta in ultima posizione


        }


    }


}
