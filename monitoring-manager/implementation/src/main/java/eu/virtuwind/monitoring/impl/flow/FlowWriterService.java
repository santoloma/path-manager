package eu.virtuwind.monitoring.impl.flow;


import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;

/**
 * A service that adds packet forwarding flows to configuration data store.
 */
public interface FlowWriterService {

    /**
     * Writes a flow that forwards packets to destPort if destination mac in
     * packet is destMac and source Mac in packet is sourceMac. If sourceMac is
     * null then flow would not set any source mac, resulting in all packets
     * with destMac being forwarded to destPort.
     *
     * @param sourceMac
     * @param destMac
     * @param destNodeConnector
     */
    public void addMacToMacFlow(String switchId, MacAddress sourceMac, MacAddress destMac, String destNodeConnector);

    /**
     * Writes a flow that forwards packets to destPort if destination mac in
     * packet is destMac and source Mac in packet is sourceMac. If sourceMac is
     * null then flow would not set any source mac, resulting in all packets
     * with destMac being forwarded to destPort.
     * This flow will either have an action to either to change source or destination MAC.
     * If changeDest is true, the action will be to change destination, if it is false, the action will be to
     * change source.
     *
     * @param sourceMac
     * @param destMac
     * @param destNodeConnector
     * @param changeDest
     * @param macToChangeTo
     */
    public void addMacToMacFlow(String switchId, MacAddress sourceMac, MacAddress destMac, String destNodeConnector, boolean changeDest, MacAddress macToChangeTo);


}