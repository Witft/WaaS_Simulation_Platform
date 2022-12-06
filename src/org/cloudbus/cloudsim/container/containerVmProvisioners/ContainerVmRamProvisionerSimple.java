package org.cloudbus.cloudsim.container.containerVmProvisioners;

import org.cloudbus.cloudsim.container.core.ContainerVm;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sareh on 10/07/15.
 */
public class ContainerVmRamProvisionerSimple extends ContainerVmRamProvisioner {

    /**
     * The RAM table.
     */
    private Map<String, Float> containerVmRamTable;

    /**
     * @param availableRam the available ram
     */
    public ContainerVmRamProvisionerSimple(int availableRam) {
        super(availableRam);
        setContainerVmRamTable(new HashMap<String, Float>());
    }

    @Override
    public boolean allocateRamForContainerVm(ContainerVm containerVm, float ram) {
        float maxRam = containerVm.getRam();

        if (ram >= maxRam) {
            ram = maxRam;
        }

        deallocateRamForContainerVm(containerVm);

        if (getAvailableRam() >= ram) {
            setAvailableRam(getAvailableRam() - ram);
            getContainerVmRamTable().put(containerVm.getUid(), ram);
            containerVm.setCurrentAllocatedRam(getAllocatedRamForContainerVm(containerVm));
            return true;
        }

        containerVm.setCurrentAllocatedRam(getAllocatedRamForContainerVm(containerVm));

        return false;
    }

    @Override
    public float getAllocatedRamForContainerVm(ContainerVm containerVm) {
        if (getContainerVmRamTable().containsKey(containerVm.getUid())) {
            return getContainerVmRamTable().get(containerVm.getUid());
        }
        return 0;
    }

    @Override
    public void deallocateRamForContainerVm(ContainerVm containerVm) {
        if (getContainerVmRamTable().containsKey(containerVm.getUid())) {
            float amountFreed = getContainerVmRamTable().remove(containerVm.getUid());
            setAvailableRam(getAvailableRam() + amountFreed);
            containerVm.setCurrentAllocatedRam(0);
        }

    }


    @Override
    public void deallocateRamForAllContainerVms() {
        super.deallocateRamForAllContainerVms();
        getContainerVmRamTable().clear();
    }

    @Override
    public boolean isSuitableForContainerVm(ContainerVm containerVm, float ram) {
        float allocatedRam = getAllocatedRamForContainerVm(containerVm);
        //检查containerVM自身的ram是否小于ram，如果小于，最多只能给其自身的ram大小
        //其它是普通的分配过程，检查可用的ram，够用就分配
        boolean result = allocateRamForContainerVm(containerVm, ram);
        //可能是因为只是尝试一下分配VM，实际上创建过程还没有开始，所以还要撤销containerVM
        deallocateRamForContainerVm(containerVm);
        if (allocatedRam > 0) {
            allocateRamForContainerVm(containerVm, allocatedRam);
        }
        return result;
    }


    /**
     * @return the containerVmRamTable
     */
    protected Map<String, Float> getContainerVmRamTable() {
        return containerVmRamTable;
    }

    /**
     * @param containerVmRamTable the containerVmRamTable to set
     */
    protected void setContainerVmRamTable(Map<String, Float> containerVmRamTable) {
        this.containerVmRamTable = containerVmRamTable;
    }

}
