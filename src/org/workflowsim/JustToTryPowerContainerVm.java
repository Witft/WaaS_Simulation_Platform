package org.workflowsim;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisioner;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisioner;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.container.schedulers.ContainerScheduler;
import org.wfc.core.WFCDeadline;
import org.wfc.scheduler.ContainerCloudletSchedulerJustToTry;

import java.util.List;

public class JustToTryPowerContainerVm extends PowerContainerVm {


    /**
     * Instantiates a new power vm.
     *
     * @param id                      the id
     * @param userId                  the user id
     * @param mips                    the mips
     * @param ram                     the ram
     * @param bw                      the bw
     * @param size                    the size
     * @param vmm                     the vmm
     * @param containerScheduler      the cloudlet scheduler
     * @param containerRamProvisioner
     * @param containerBwProvisioner
     * @param peList
     * @param schedulingInterval      the scheduling interval
     */
    public JustToTryPowerContainerVm(int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    /**
     * 重载构造函数，加上VM类型
     *
     * @param id                      the id
     * @param userId                  the user id
     * @param mips                    the mips
     * @param ram                     the ram
     * @param bw                      the bw
     * @param size                    the size
     * @param vmm                     the vmm
     * @param containerScheduler      the cloudlet scheduler
     * @param containerRamProvisioner
     * @param containerBwProvisioner
     * @param peList
     * @param schedulingInterval      the scheduling interval
     */
    public JustToTryPowerContainerVm(ContainerVmType type,int id, int userId, double mips, float ram, long bw, long size, String vmm, ContainerScheduler containerScheduler, ContainerRamProvisioner containerRamProvisioner, ContainerBwProvisioner containerBwProvisioner, List<? extends ContainerPe> peList, double schedulingInterval) {
        super(type, id, userId, mips, ram, bw, size, vmm, containerScheduler, containerRamProvisioner, containerBwProvisioner, peList, schedulingInterval);
    }

    @Override
    public boolean containerCreate(Container container) {
        //如果已经有容器，就把原来的容器都去掉
        while(getContainerList().size() > 0){
            Container c1 = getContainerList().get(0);
            WFCDeadline.lossJobNumWhenChangeContainer +=
                    c1.getContainerCloudletScheduler().getCloudletWaitingList().size()
                            + c1.getContainerCloudletScheduler().getCloudletExecList().size();
            containerDestroy(getContainerList().get(0));
        }
        if (getSize() < container.getSize()) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by storage");
            return false;
        }

        if (!getContainerRamProvisioner().allocateRamForContainer(container, container.getCurrentRequestedRam())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by RAM");
            return false;
        }

        if (!getContainerBwProvisioner().allocateBwForContainer(container, container.getCurrentRequestedBw())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by BW");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            return false;
        }

        if (!getContainerScheduler().allocatePesForContainer(container, container.getCurrentRequestedMips())) {
            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
                    " failed by MIPS");
            getContainerRamProvisioner().deallocateRamForContainer(container);
            getContainerBwProvisioner().deallocateBwForContainer(container);
            return false;
        }

//        /**
//         * 限制VM上只能有一个container，所以加一个条件
//         */
//        if(getContainerList().size() > 0){
//            Log.printConcatLine("[ContainerScheduler.ContainerCreate] Allocation of Container #", container.getId(), " to VM #", getId(),
//                    " failed by limit that one VM has only one container");
//            return false;
//        }

        setSize(getSize() - container.getSize());
        getContainerList().add(container);
        container.setVm(this);
        //因为一个VM上只有一个container，让container的性能和VM本身一致
        ContainerCloudletSchedulerJustToTry scheduler = (ContainerCloudletSchedulerJustToTry) container.getContainerCloudletScheduler();
        scheduler.setMips(getType().getMips());
        scheduler.setTotalMips(getType().getMips());
        return true;
    }
}
