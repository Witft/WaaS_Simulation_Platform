package org.workflowsim;

import org.cloudbus.cloudsim.container.core.PowerContainer;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.workflowsim.JustToTryPowerContainerVm;

public class JustToTryPowerContainer extends PowerContainer {

    public JustToTryPowerContainer(
            final int id,
            final int userId,
            final double mips,
            final int pesNumber,
            final int ram,
            final long bw,
            final long size,
            final String vmm,
            final ContainerCloudletScheduler cloudletScheduler,
            final double schedulingInterval) {
        super(id, userId, mips, pesNumber, ram, bw, size, vmm, cloudletScheduler, schedulingInterval);
    }

    public JustToTryPowerContainer(
            final int workflowId,
            final int id,
            final int userId,
            final double mips,
            final int pesNumber,
            final int ram,
            final long bw,
            final long size,
            final String vmm,
            final ContainerCloudletScheduler cloudletScheduler,
            final double schedulingInterval) {
        super(workflowId, id, userId, mips, pesNumber, ram, bw, size, vmm, cloudletScheduler, schedulingInterval);
    }
}
