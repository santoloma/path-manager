package eu.virtuwind.monitoring.impl;

import eu.virtuwind.monitoring.impl.inventory.TopologyReader;
import org.graphstream.graph.implementations.SingleGraph;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Node;
import org.opendaylight.yang.gen.v1.urn.tbd.params.xml.ns.yang.network.topology.rev131021.network.topology.topology.Link;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyGraph {

    private SingleGraph graph = new SingleGraph("Topology");
    private DataBroker dataBroker;
    private Map<String, String> linkMapping = new HashMap<>();

    public TopologyGraph(DataBroker dataBroker) {
        this.dataBroker = dataBroker;
        System.out.println(graph.getId());
    }

    public SingleGraph getGraph() {
        return graph;
    }

    public String getLinkEndNode(String startNode) {
        return linkMapping.get(startNode);
    }


    public void addEdge(String edgeId, String fromNode, String toNode, boolean isDirected) {
        graph.addEdge(edgeId, fromNode, toNode, isDirected);
    }

    public void addNode(String nodeId) {
        graph.addNode(nodeId);
    }

    public void addGraphElements() {
        try {
            System.out.println("Adding graph elements!");
            List<Node> nodes = TopologyReader.getAllNodes(dataBroker);
            List<Link> links = TopologyReader.getAllLinks(dataBroker);

            for (Node node : nodes) {
                String nodeId = node.getNodeId().getValue().replaceAll("host:", "");
                addNode(nodeId);
                System.out.println("Adding a node: " + nodeId);
            }

            for (Link link : links) {
                String source = link.getSource().getSourceNode().getValue().replaceAll("host:", "");
                String destination = link.getDestination().getDestNode().getValue().replaceAll("host:", "");
                String srcTp = link.getSource().getSourceTp().getValue();
                String dstTp = link.getDestination().getDestTp().getValue();
                String edge = "";
                if (srcTp.contains("open")) {
                    edge = srcTp;
                    dstTp = dstTp.replaceAll("host:", "");
                } else {
                    edge = dstTp;
                    srcTp = srcTp.replaceAll("host:", "");
                }
                linkMapping.put(srcTp, dstTp);
                if (graph.getEdge(edge) == null) {
                    if (source.contains("open") && destination.contains("open")) {
                        addEdge(edge, source, destination, true);
                    } else {
                        addEdge(edge, source, destination, false);
                    }
                    System.out.println("Adding edge " + edge + " between " + source + " and " + destination);
                }
            }

            System.out.println("Map of "+linkMapping);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}