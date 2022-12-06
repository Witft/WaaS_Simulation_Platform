package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;
import org.wfc.core.WFCReplication;
import org.wfc.scheduler.ContainerCloudletSchedulerJustToTry;
import org.workflowsim.ContainerVmType;
import org.workflowsim.JustToTryPowerContainerVm;

import java.text.DecimalFormat;
import java.util.*;

public class ContainerGreedyAlgorithm extends BaseSchedulingAlgorithm {

    /**
     * 创建新VM，返回id
     *
     * @param type
     * @param brokerId
     * @return
     */
    protected int tryToCreateVm(ContainerVmType type, int brokerId, int cloudletId) {
        ArrayList peList = new ArrayList();
        for (int p = 0; p < WFCConstants.WFC_NUMBER_VM_PES; p++) {
            peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double) WFCConstants.WFC_VM_MIPS * WFCConstants.WFC_VM_RATIO)));
        }
        int newVmId = IDs.pollId(ContainerVm.class);
        ContainerVm aNewVm = new JustToTryPowerContainerVm(type, newVmId, brokerId, WFCConstants.WFC_VM_MIPS, (float) WFCConstants.WFC_VM_RAM,
                WFCConstants.WFC_VM_BW, WFCConstants.WFC_VM_SIZE, WFCConstants.WFC_VM_VMM,
                new ContainerSchedulerTimeSharedOverSubscription(peList),
                //new ContainerSchedulerTimeSharedOverSubscription(peList),
                new ContainerRamProvisionerSimple(WFCConstants.WFC_VM_RAM),
                new ContainerBwProvisionerSimple(WFCConstants.WFC_VM_BW), peList,
                WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
//        getToCreateVmList().add(aNewVm);
        getToCreateVmMap().put(newVmId, aNewVm);
        getCloudletsLeadToNewVm().put(cloudletId, aNewVm);
        return newVmId;
    }

    @Override
    public void run() {

    }

    @Override
    public void run(int brokerId) {
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
        if (WFCConstants.PRINT_SCHEDULE_WAITING_LIST) {
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":还有" + numCloudlet + "个任务等待调度");
        }
        //在任务正式调度前，先默认它们没有找到适合的VM
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            cloudlet.setVmId(-1000);
        }
        int numOfCloudletsOnVm = 0;
        // 记录所有空闲的VM
        List<ContainerVm> idleVmList = new ArrayList<>();
        for (int i = 0; i < numVM; i++) {
            // 已经分配给该VM的任务数量
            int alreadyAllocatedCloudlet = 0;
            // 防止重复查看任务
            Set<Integer> hasCheckedIds = new HashSet<>();
            ContainerVm containerVm = (ContainerVm) getVmList().get(i);
            // 查看三处，容器上的任务，滞留的任务，WFCScheduler刚发送的任务，
            // 不查看本函数中刚分配的任务容器上的任务
            if (containerVm.getContainerList().size() > 0) {
                Container container = containerVm.getContainerList().get(0);
                ContainerCloudletSchedulerJustToTry containerCloudletScheduler = (ContainerCloudletSchedulerJustToTry) container.getContainerCloudletScheduler();
                for (ResCloudlet resCloudlet : containerCloudletScheduler.getCloudletExecList()) {
                    alreadyAllocatedCloudlet++;
                    hasCheckedIds.add(resCloudlet.getCloudletId());
                }
                for (ResCloudlet resCloudlet : containerCloudletScheduler.getCloudletWaitingList()) {
                    alreadyAllocatedCloudlet++;
                    hasCheckedIds.add(resCloudlet.getCloudletId());
                }
            }
            // 滞留的任务
            for (ContainerCloudlet cloudlet1 : getDetainedCloudletList()) {
                if (cloudlet1.getVmId() == containerVm.getId() && !hasCheckedIds.contains(cloudlet1.getCloudletId())) {
                    alreadyAllocatedCloudlet++;
                    hasCheckedIds.add(cloudlet1.getCloudletId());
                }
            }
            // 同一时间，刚发送出去的任务
            for (ContainerCloudlet cloudlet1 : getLastSentCloudlets()) {
                if (cloudlet1.getVmId() == containerVm.getId() && !hasCheckedIds.contains(cloudlet1.getCloudletId())) {
                    alreadyAllocatedCloudlet++;
                    hasCheckedIds.add(cloudlet1.getCloudletId());
                }
            }
            numOfCloudletsOnVm += alreadyAllocatedCloudlet;
            // 如果该VM是空闲的，要存储进List
            if (0 == alreadyAllocatedCloudlet) {
                idleVmList.add(containerVm);
            }
        }
        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()),
                ":当前有", numOfCloudletsOnVm, "个任务在", numVM, "个VM上");
        //对于每一个待调度的任务
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            //单独处理stage-in任务
            if (cloudlet.getWorkflowId() == 0 && numVM > 0) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(0);
                cloudlet.setVmId(containerVm.getId());
                break;
            }

            // 复制任务的代码
            // 防止重复复制，getHasSentReplicateCloudlets()记录的是已经发送过的要复制的任务
            // 前半句判断是否是主任务，后半句判断是否是从任务
//            if (!getHasSentReplicateCloudlets().contains(cloudlet) && !WFCReplication.getSlaveToMasterMap().containsKey(cloudlet)) {
//                getReplicateCloudlets().add(cloudlet);
//                if (WFCConstants.PRINT_REPLICATE_RETRY) {
//                    Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "即将复制任务：", cloudlet.getCloudletId());
//                }
//            }

            int fastestIndex = -1;
            double fastestTime = Double.MAX_VALUE;
            for (int j = 0; j < idleVmList.size(); j++) {
                ContainerVm containerVm = idleVmList.get(j);
                double pt = cloudlet.getTransferTime() +
                        cloudlet.getCloudletLength() / containerVm.getType().getMips();
                if (containerVm.getContainerList().size() == 0 ||
                        containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                    pt += WFCConstants.CONTAINER_INITIAL_TIME;
                }
                if (fastestIndex == -1 || pt < fastestTime) {
                    fastestIndex = j;
                    fastestTime = pt;
                }
            }
            if (fastestIndex >= 0) {
                ContainerVm containerVm = idleVmList.get(fastestIndex);
                cloudlet.setVmId(containerVm.getId());
                //更新idleVmList
                idleVmList.remove(containerVm);

                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(CloudSim.clock() + ":任务"
                            + cloudlet.getCloudletId() + "被分配给了虚拟机"
                            + cloudlet.getVmId());
                }
                continue;
            }
            // 如果没有找到空闲的VM，就租用新的VM
//            if(!getCloudletsLeadToNewVm().containsKey(cloudlet.getCloudletId())){
//                tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId, cloudlet.getCloudletId());
//            }
            int newVmId = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId, cloudlet.getCloudletId());
            cloudlet.setVmId(newVmId);
        }
    }
}
