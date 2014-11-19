package com.luca.filipponi.tweetAnalysis.JsonGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luca on 28/07/14.
 */
public class JsonGraph {


    public List<String> nodeList;
    public List<String> edgeList;


    /**
     * Creates an empty JsonGraph object, initializing a list for nodes and a list for edge.
     */

    public JsonGraph() {

        //creates an empty jsonGraph
        edgeList = new ArrayList<String>();
        nodeList = new ArrayList<String>();


    }


    /**
     * Adds a node to the nodes list.
     * If the group of the node is null or "", group 1 is assigned.
     *
     * @param name  Name of the node (its label)
     * @param group Group of the node (each group has a different color)
     */

    public void addNode(String name, String group) {

        if (group == null || group.equals("")) {
            group = "1";
        }


        String node = "{\"name\":\"" + name + "\",\"group\":" + group + "}";

        nodeList.add(node);


    }


    /**
     * Add an edge to the edges list.
     *
     * @param source Node ID source
     * @param target Node ID target
     * @param weight Weight of the edge
     */

    public void addEdge(String source, String target, String weight) {


        String edge = "{\"source\":" + source + ",\"target\":" + target + ",\"value\":" + weight + "}";

        edgeList.add(edge);


    }


    public List<String> getNodeList() {
        return nodeList;
    }

    public List<String> getEdgeList() {
        return edgeList;
    }
}
