package eu.virtuwind.monitoring.impl.flow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import eu.virtuwind.monitoring.impl.util.InstanceIdentifierUtils;
import org.opendaylight.openflowplugin.api.OFConstants;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.SetDlDstActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.SetDlSrcActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.output.action._case.OutputActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.set.dl.dst.action._case.SetDlDstActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.set.dl.src.action._case.SetDlSrcActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.Action;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.AddFlowInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.AddFlowOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.FlowTableRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SalFlowService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowCookie;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.InstructionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.MatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.ApplyActionsCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActions;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.Instruction;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.InstructionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSourceBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatch;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatchBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of
 * FlowWriterService{@link org.opendaylight.l2switch.flow.FlowWriterService},
 * that builds required flow and writes to configuration data store using
 * provided {@link org.opendaylight.controller.md.sal.binding.api.DataBroker}.
 */
public class FlowWriterServiceImpl implements FlowWriterService {
    private static final Logger LOG = LoggerFactory.getLogger(FlowWriterServiceImpl.class);
    private final String FLOW_ID_PREFIX = "L2switch-";
    private SalFlowService salFlowService;
    private short flowTableId;
    private int flowPriority;
    private int flowIdleTimeout;
    private int flowHardTimeout;

    private AtomicLong flowIdInc = new AtomicLong();
    private AtomicLong flowCookieInc = new AtomicLong(0x2a00000000000000L);
    private final Integer DEFAULT_TABLE_ID = 0;
    private final Integer DEFAULT_PRIORITY = 10;
    private final Integer DEFAULT_HARD_TIMEOUT = 0;
    private final Integer DEFAULT_IDLE_TIMEOUT = 0;


    /**
     * Implementation of the service that adds packet forwarding flows to configuration data store.
     */
    public FlowWriterServiceImpl(SalFlowService salFlowService) {
        Preconditions.checkNotNull(salFlowService, "salFlowService should not be null.");
        this.salFlowService = salFlowService;
    }


    public void setFlowTableId(short flowTableId) {
        this.flowTableId = flowTableId;
    }

    public void setFlowPriority(int flowPriority) {
        this.flowPriority = flowPriority;
    }

    public void setFlowIdleTimeout(int flowIdleTimeout) {
        this.flowIdleTimeout = flowIdleTimeout;
    }

    public void setFlowHardTimeout(int flowHardTimeout) {
        this.flowHardTimeout = flowHardTimeout;
    }

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
    @Override
    public void addMacToMacFlow(String switchId, MacAddress sourceMac, MacAddress destMac, String destNodeConnector) {

        Preconditions.checkNotNull(sourceMac, "Source mac address should not be null.");
        Preconditions.checkNotNull(destMac, "Destination mac address should not be null.");
        Preconditions.checkNotNull(destNodeConnector, "Destination port should not be null.");

        // do not add flow if both macs are same.
        if (sourceMac != null && destMac.equals(sourceMac)) {
            LOG.info("In addMacToMacFlow: No flows added. Source and Destination mac are same.");
            return;
        }

        // get flow table key
        TableKey flowTableKey = new TableKey((short) flowTableId);

        // build a flow path based on node connector to program flow
        InstanceIdentifier<Flow> flowPath = buildFlowPath(switchId, flowTableKey);

        // build a flow that target given mac id
        Flow flowBody = createMacToMacFlow(flowTableKey.getId(), flowPriority, sourceMac, destMac,
                destNodeConnector);

        // commit the flow in config data
        writeFlowToConfigData(flowPath, flowBody);
    }


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
     * @param changeDest           - if true, set action to change destination; if false, set action to change source
     * @param macToChangeTo        - src or dest mac to replace the match
     */
    @Override
    public void addMacToMacFlow(String switchId, MacAddress sourceMac, MacAddress destMac, String destNodeConnector, boolean changeDest, MacAddress macToChangeTo) {

        Preconditions.checkNotNull(destMac, "Destination mac address should not be null.");
        Preconditions.checkNotNull(destNodeConnector, "Destination port should not be null.");

        // do not add flow if both macs are same.
        if (sourceMac != null && destMac.equals(sourceMac)) {
            LOG.info("In addMacToMacFlow: No flows added. Source and Destination mac are same.");
            return;
        }

        // get flow table key
        TableKey flowTableKey = new TableKey((short) flowTableId);

        // build a flow path based on node connector to program flow
        InstanceIdentifier<Flow> flowPath = buildFlowPath(switchId, flowTableKey);

        // build a flow that target given mac id
        Flow flowBody = createMacToMacFlow(flowTableKey.getId(), flowPriority, sourceMac, destMac,
                destNodeConnector, changeDest, macToChangeTo);

        // commit the flow in config data
        writeFlowToConfigData(flowPath, flowBody);
    }


    /**
     * @param node
     * @return
     */
    private InstanceIdentifier<Flow> buildFlowPath(String node, TableKey flowTableKey) {

        // generate unique flow key
        FlowId flowId = new FlowId(FLOW_ID_PREFIX + String.valueOf(flowIdInc.getAndIncrement()));
        FlowKey flowKey = new FlowKey(flowId);

        return InstanceIdentifierUtils.generateFlowInstanceIdentifier(node, flowTableKey, flowKey);
    }

    /**
     * @param tableId
     * @param priority
     * @param sourceMac
     * @param destMac
     * @param destPort
     * @param changeDest    - if true, set action to change destination; if false, set action to change source
     * @param macToChangeTo - src or dest mac to replace the match
     * @return {@link org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder}
     * builds flow that forwards all packets with destMac to given port
     */
    private Flow createMacToMacFlow(Short tableId, int priority, MacAddress sourceMac, MacAddress destMac,
                                   String destPort, boolean changeDest, MacAddress macToChangeTo) {

        // start building flow
        FlowBuilder macToMacFlow = new FlowBuilder() //
                .setTableId(tableId) //
                .setFlowName("mac2mac");

        // use its own hash code for id.
        macToMacFlow.setId(new FlowId(Long.toString(macToMacFlow.hashCode())));

        // create a match that has mac to mac ethernet match
        EthernetMatchBuilder ethernetMatchBuilder = new EthernetMatchBuilder() //
                .setEthernetDestination(new EthernetDestinationBuilder() //
                        .setAddress(destMac) //
                        .build());
        // set source in the match only if present
        if (sourceMac != null) {
            ethernetMatchBuilder.setEthernetSource(new EthernetSourceBuilder().setAddress(sourceMac).build());
        }
        EthernetMatch ethernetMatch = ethernetMatchBuilder.build();
        Match match = new MatchBuilder().setEthernetMatch(ethernetMatch).build();

        Uri destPortUri = new NodeConnectorId(new Uri(destPort));


        Action outputToNode = new ActionBuilder() //
                .setOrder(0)
                .setAction(new OutputActionCaseBuilder() //
                        .setOutputAction(new OutputActionBuilder() //
                                .setMaxLength(0xffff) //
                                .setOutputNodeConnector(destPortUri) //
                                .build()) //
                        .build()) //
                .build();

        // Create an Apply Action
        ApplyActionsBuilder applyActionsBuilder = new ApplyActionsBuilder();
        if (changeDest) {
            applyActionsBuilder.setAction(ImmutableList.of(setDlDstAction(macToChangeTo), outputToNode))
                    .build();
        } else {
            applyActionsBuilder.setAction(ImmutableList.of(setDlSrcAction(macToChangeTo), outputToNode))
                    .build();
        }


        ApplyActions applyActions = applyActionsBuilder.build();


        // Wrap our Apply Action in an Instruction
        Instruction applyActionsInstruction = new InstructionBuilder() //
                .setOrder(0)
                .setInstruction(new ApplyActionsCaseBuilder()//
                        .setApplyActions(applyActions) //
                        .build()) //
                .build();

        // Put our Instruction in a list of Instructions
        macToMacFlow.setMatch(match) //
                .setInstructions(new InstructionsBuilder() //
                        .setInstruction(ImmutableList.of(applyActionsInstruction)) //
                        .build()) //
                .setPriority(priority) //
                .setBufferId(OFConstants.OFP_NO_BUFFER) //
                .setHardTimeout(0) //
                .setIdleTimeout(0) //
                .setCookie(new FlowCookie(BigInteger.valueOf(flowCookieInc.getAndIncrement())))
                .setFlags(new FlowModFlags(false, false, false, false, false));

        return macToMacFlow.build();
    }


    /**
     * @param tableId
     * @param priority
     * @param sourceMac
     * @param destMac
     * @param destPort
     * @return {@link org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder}
     * builds flow that forwards all packets with destMac to given port
     */
    private Flow createMacToMacFlow(Short tableId, int priority, MacAddress sourceMac, MacAddress destMac,
                                   String destPort) {

        // start building flow
        FlowBuilder macToMacFlow = new FlowBuilder() //
                .setTableId(tableId) //
                .setFlowName("mac2mac");

        // use its own hash code for id.
        macToMacFlow.setId(new FlowId(Long.toString(macToMacFlow.hashCode())));

        // create a match that has mac to mac ethernet match
        EthernetMatchBuilder ethernetMatchBuilder = new EthernetMatchBuilder() //
                .setEthernetDestination(new EthernetDestinationBuilder() //
                        .setAddress(destMac) //
                        .build());
        // set source in the match only if present
        if (sourceMac != null) {
            ethernetMatchBuilder.setEthernetSource(new EthernetSourceBuilder().setAddress(sourceMac).build());
        }
        EthernetMatch ethernetMatch = ethernetMatchBuilder.build();
        Match match = new MatchBuilder().setEthernetMatch(ethernetMatch).build();

        Uri destPortUri = new NodeConnectorId(new Uri(destPort));

        Action outputToNode = new ActionBuilder() //
                .setOrder(0)
                .setAction(new OutputActionCaseBuilder() //
                        .setOutputAction(new OutputActionBuilder() //
                                .setMaxLength(0xffff) //
                                .setOutputNodeConnector(destPortUri) //
                                .build()) //
                        .build()) //
                .build();

        // Create an Apply Action
        ApplyActionsBuilder applyActionsBuilder = new ApplyActionsBuilder();

        applyActionsBuilder.setAction(ImmutableList.of(outputToNode))
                .build();


        ApplyActions applyActions = applyActionsBuilder.build();


        // Wrap our Apply Action in an Instruction
        Instruction applyActionsInstruction = new InstructionBuilder() //
                .setOrder(0)
                .setInstruction(new ApplyActionsCaseBuilder()//
                        .setApplyActions(applyActions) //
                        .build()) //
                .build();

        // Put our Instruction in a list of Instructions
        macToMacFlow.setMatch(match) //
                .setInstructions(new InstructionsBuilder() //
                        .setInstruction(ImmutableList.of(applyActionsInstruction)) //
                        .build()) //
                .setPriority(priority) //
                .setBufferId(OFConstants.OFP_NO_BUFFER) //
                .setHardTimeout(0) //
                .setIdleTimeout(0) //
                .setCookie(new FlowCookie(BigInteger.valueOf(flowCookieInc.getAndIncrement())))
                .setFlags(new FlowModFlags(false, false, false, false, false));

        return macToMacFlow.build();
    }

    /**
     * @param mac
     * @return an action to change the destination MAC
     */
    private Action setDlDstAction(MacAddress mac) {
        Preconditions.checkNotNull(mac, "The setDlDstAction requires a MAC address that is not null");
        ActionBuilder actionBuilder = new ActionBuilder();
        return actionBuilder
                .setOrder(0).setAction(new SetDlDstActionCaseBuilder()
                        .setSetDlDstAction(new SetDlDstActionBuilder()
                                .setAddress(mac)
                                .build())
                        .build())
                .build();
    }


    /**
     * @param mac
     * @return an action to change the source MAC
     */
    private Action setDlSrcAction(MacAddress mac) {
        Preconditions.checkNotNull(mac, "The setDlSrcAction requires a MAC address that is not null");
        ActionBuilder actionBuilder = new ActionBuilder();
        return actionBuilder
                .setOrder(0).setAction(new SetDlSrcActionCaseBuilder()
                        .setSetDlSrcAction(new SetDlSrcActionBuilder()
                                .setAddress(mac)
                                .build())
                        .build())
                .build();
    }


    /**
     *
     * @return an action to output to the controller

    private Action outputToController() {

    ActionBuilder actionBuilder = new ActionBuilder();
    return actionBuilder
    .setOrder(0).setAction(new OutputActionCaseBuilder() //
    .setOutputAction(new OutputActionBuilder() //
    .setMaxLength(65535) //
    .setOutputNodeConnector(new Uri("CONTROLLER")) //
    .build()) //
    .build()) //
    .build();
    }

     */

    /**
     * Starts and commits data change transaction which modifies provided flow
     * path with supplied body.
     *
     * @param flowPath
     * @param flow
     * @return transaction commit
     */
    private Future<RpcResult<AddFlowOutput>> writeFlowToConfigData(InstanceIdentifier<Flow> flowPath, Flow flow) {
        final InstanceIdentifier<Table> tableInstanceId = flowPath.<Table>firstIdentifierOf(Table.class);
        final InstanceIdentifier<Node> nodeInstanceId = flowPath.<Node>firstIdentifierOf(Node.class);
        final AddFlowInputBuilder builder = new AddFlowInputBuilder(flow);
        builder.setNode(new NodeRef(nodeInstanceId));
        builder.setFlowRef(new FlowRef(flowPath));
        builder.setFlowTable(new FlowTableRef(tableInstanceId));
        builder.setTransactionUri(new Uri(flow.getId().getValue()));
        return salFlowService.addFlow(builder.build());
    }
}