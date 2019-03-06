/*
 * Copyright © 2015 George and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package eu.virtuwind.monitoring.impl;



import eu.virtuwind.monitoring.impl.flow.FlowWriterServiceImpl;
import eu.virtuwind.monitoring.impl.flow.ReactiveFlowWriter;
import eu.virtuwind.monitoring.impl.inventory.InventoryReader;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.sal.binding.api.BindingAwareBroker.ProviderContext;
import org.opendaylight.controller.sal.binding.api.BindingAwareProvider;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SalFlowService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yangtools.concepts.Registration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;



public class MonitoringProvider implements BindingAwareProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringProvider.class);
    private DataBroker dataBroker;
    private SalFlowService salFlowService;
    private NotificationProviderService notificationService;
    private RpcProviderRegistry rpcProviderRegistry;
    private PacketProcessingService packetProcessingService;
    private Registration reactFlowWriterReg;
    private TopologyGraph topologyGraph;
    private ReactiveFlowWriter reactiveFlowWriter;


    public MonitoringProvider(DataBroker dataBroker, RpcProviderRegistry rpcProviderRegistry,
                              NotificationProviderService notificationService) {
        this.dataBroker = dataBroker;
        this.notificationService = notificationService;
        this.rpcProviderRegistry = rpcProviderRegistry;
        this.salFlowService = rpcProviderRegistry.getRpcService(SalFlowService.class);
        this.packetProcessingService = rpcProviderRegistry.getRpcService(PacketProcessingService.class);


        System.out.println("The Module Has Loaded Up");
        setUpFlowWriters();


    }

    /**
     * Instantiates FlowWriterServiceImpl, which has the functions for creating flows,  and ReactiveFlowWriter, which listens for
     * ARP packets and uses the flowWriterService to install flows accordingly.
     */
    private void setUpFlowWriters() {
        FlowWriterServiceImpl flowWriterService = new FlowWriterServiceImpl(salFlowService);
        flowWriterService.setFlowTableId((short) 0);
        flowWriterService.setFlowPriority(10);
        flowWriterService.setFlowIdleTimeout(0);
        flowWriterService.setFlowHardTimeout(0);

        InventoryReader inventoryReader = new InventoryReader(dataBroker);
        topologyGraph = new TopologyGraph(dataBroker);
        reactiveFlowWriter = new ReactiveFlowWriter(topologyGraph, inventoryReader, flowWriterService);
        reactFlowWriterReg = notificationService.registerNotificationListener(reactiveFlowWriter);
        executeLater(1);

    }


    @Override
    public void onSessionInitiated(ProviderContext session) {

        LOG.info("MonitoringProvider Session Initiated");
    }

    @Override
    public void close() throws Exception {
        LOG.info("MonitoringProvider Closed");
        if (reactFlowWriterReg != null) {
            reactFlowWriterReg.close();
        }
    }


    public void executeLater(long minutes) {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println("Running");
                topologyGraph.addGraphElements();
                reactiveFlowWriter.setDelayElapsed(true);
            }
        }, minutes, TimeUnit.MINUTES);
    }

}
