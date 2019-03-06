package eu.virtuwind.monitoring.impl;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.sal.binding.api.NotificationProviderService;
import org.opendaylight.controller.sal.binding.api.RpcProviderRegistry;


public class MonitoringModule extends eu.virtuwind.monitoring.impl.AbstractMonitoringModule {
    public MonitoringModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public MonitoringModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver, eu.virtuwind.monitoring.impl.MonitoringModule oldModule, java.lang.AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public void customValidation() {
        // add custom validation form module attributes here.
    }

    @Override
    public java.lang.AutoCloseable createInstance() {
        DataBroker dataBrokerService = getDataBrokerDependency();
        RpcProviderRegistry rpcProviderRegistry = getRpcRegistryDependency();
        NotificationProviderService notificationService = getNotificationServiceDependency();
        MonitoringProvider provider = new MonitoringProvider(dataBrokerService, rpcProviderRegistry, notificationService);

        return provider;
    }

}
