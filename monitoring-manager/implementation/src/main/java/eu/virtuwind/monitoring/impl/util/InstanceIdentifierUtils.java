/*
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package eu.virtuwind.monitoring.impl.util;


import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;

/* InstanceIdentifierUtils provides utility functions related to InstanceIdentifiers.
 */
public final class InstanceIdentifierUtils {

    private InstanceIdentifierUtils() {
        throw new UnsupportedOperationException("Utility class should never be instantiated");
    }

    /**
     * @param node
     * @return
     */
    public static InstanceIdentifier<Node> generateNodeInstanceIdentifier(final String node) {
        NodeId nodeId = new NodeId(node);
        return InstanceIdentifier.builder(Nodes.class).child(Node.class, new NodeKey(nodeId)).build();
    }

    /**
     * @param node
     * @param flowTableKey
     * @return
     */
    public static InstanceIdentifier<Table> generateFlowTableInstanceIdentifier(final String node,
                                                                                final TableKey flowTableKey) {
        return generateNodeInstanceIdentifier(node).builder().augmentation(FlowCapableNode.class)
                .child(Table.class, flowTableKey).build();
    }

    /**
     * @param node
     * @param flowTableKey
     * @param flowKey
     * @return
     */
    public static InstanceIdentifier<Flow> generateFlowInstanceIdentifier(final String node,
                                                                          final TableKey flowTableKey, final FlowKey flowKey) {
        return generateFlowTableInstanceIdentifier(node, flowTableKey).child(Flow.class, flowKey);
    }

}