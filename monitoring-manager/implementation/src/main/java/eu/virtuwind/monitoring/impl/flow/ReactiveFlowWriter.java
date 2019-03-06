package eu.virtuwind.monitoring.impl.flow;

import eu.virtuwind.monitoring.impl.TopologyGraph;
import eu.virtuwind.monitoring.impl.graphstream.algorithm.Dijkstra;
import eu.virtuwind.monitoring.impl.inventory.InventoryReader;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.arp.rev140528.ArpPacketListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.arp.rev140528.ArpPacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.arp.rev140528.arp.packet.received.packet.chain.packet.ArpPacket;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.basepacket.rev140528.packet.chain.grp.PacketChain;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.basepacket.rev140528.packet.chain.grp.packet.chain.Packet;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.basepacket.rev140528.packet.chain.grp.packet.chain.packet.RawPacket;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.ethernet.rev140528.ethernet.packet.received.packet.chain.packet.EthernetPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * This class listens for ARP packets and writes a mac to mac flows.
 */
public class ReactiveFlowWriter implements ArpPacketListener {
    private InventoryReader inventoryReader;
    private FlowWriterService flowWriterService;
    private final static Logger LOG = LoggerFactory.getLogger(ReactiveFlowWriter.class);
    private TopologyGraph topologyGraph;
    private boolean delayElapsed;
    private SingleGraph graph;


    public ReactiveFlowWriter(TopologyGraph topologyGraph, InventoryReader inventoryReader, FlowWriterService flowWriterService) {
        this.inventoryReader = inventoryReader;
        this.flowWriterService = flowWriterService;
        this.topologyGraph = topologyGraph;
        System.out.println("ReactiveFlowWriter initiated.");

    }

    public void setDelayElapsed(boolean delayElapsed) {
        this.delayElapsed = delayElapsed;
    }


    @Override
    public void onArpPacketReceived(ArpPacketReceived packetReceived) {

        if (packetReceived == null || packetReceived.getPacketChain() == null) {
            return;
        }

        RawPacket rawPacket = null;

        EthernetPacket ethernetPacket = null;
        ArpPacket arpPacket = null;
        for (PacketChain packetChain : packetReceived.getPacketChain()) {
            Packet packet = packetChain.getPacket();
            if (packet instanceof RawPacket) {
                rawPacket = (RawPacket) packetChain.getPacket();
            } else if (packet instanceof EthernetPacket) {
                ethernetPacket = (EthernetPacket) packetChain.getPacket();
            } else if (packet instanceof ArpPacket) {
                arpPacket = (ArpPacket) packetChain.getPacket();
            }
        }
        if (rawPacket == null || ethernetPacket == null || arpPacket == null) {
            return;
        }

        if (!delayElapsed) {
            return;
        } else {
            try {
                MacAddress srcMac = ethernetPacket.getSourceMac();
                MacAddress destMac = ethernetPacket.getDestinationMac();
                // String srcIp = arpPacket.getSourceProtocolAddress();
                // String destIp = arpPacket.getDestinationProtocolAddress();
                graph = topologyGraph.getGraph();


                Dijkstra dijkstra = new Dijkstra(Dijkstra.Element.EDGE, null, null);
                dijkstra.init(graph);
                System.out.println(srcMac.getValue());
                Node node = graph.getNode(srcMac.getValue());
                System.out.println("Node: " + node);
                dijkstra.setSource(node);
                dijkstra.compute();
                Iterable<Edge> edges = dijkstra.getPathEdges(graph.getNode(destMac.getValue()));
                String srcInPort = topologyGraph.getLinkEndNode(srcMac.getValue());
                Iterator<Edge> it = edges.iterator();
                while (it.hasNext()) {

                    String outport = it.next().getId();
                    String switchId =  getSwitchId(outport);
                    if(!outport.equals(srcInPort)) {
                        System.out.println("Install a flow with match " + srcMac + ", " + destMac + ", " + "on switch " + switchId);
                        System.out.println("Action: output to " + outport);
                        writeNormalFlows(switchId, outport, srcMac, destMac);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }


    public String getSwitchId(String link){
        String[] splitLink = link.split(":");
        if(splitLink.length==3) {
            return splitLink[0] + ":" + splitLink[1];
        }else{
            return null;
        }
    }


    public void writeNormalFlows(String switchId, String dest, MacAddress srcMac, MacAddress destMac) {
        LOG.info("Inside of writeNormalFlows");
        flowWriterService.addMacToMacFlow(switchId, srcMac, destMac, dest);
    }

}
