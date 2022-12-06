package org.workflowsim;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisioner;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisioner;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.core.PowerContainerHost;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmScheduler;
import org.cloudbus.cloudsim.power.models.PowerModel;

import java.util.List;

public class JustToTryPowerContainerHost extends PowerContainerHost {

    /**
     * Instantiates a new host.
     *
     * @param id             the id
     * @param ramProvisioner the ram provisioner
     * @param bwProvisioner  the bw provisioner
     * @param storage        the storage
     * @param peList         the pe list
     * @param vmScheduler    the VM scheduler
     * @param powerModel
     */
    public JustToTryPowerContainerHost(int id, ContainerVmRamProvisioner ramProvisioner, ContainerVmBwProvisioner bwProvisioner, long storage, List<? extends ContainerVmPe> peList, ContainerVmScheduler vmScheduler, PowerModel powerModel) {
        super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler, powerModel);
    }

    @Override
    public double updateContainerVmsProcessing(double currentTime) {
        double smallerTime = Double.MAX_VALUE;

        for (ContainerVm containerVm : getVmList()) {
            double time = containerVm.updateVmProcessing(currentTime, getContainerVmScheduler().getAllocatedMipsForContainerVm(containerVm));
            if (time > 0.0 && time < smallerTime) {
                smallerTime = time;
            }
        }

        return smallerTime;
    }
}
