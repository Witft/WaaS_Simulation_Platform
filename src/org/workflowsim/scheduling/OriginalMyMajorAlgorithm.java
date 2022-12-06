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

public class OriginalMyMajorAlgorithm extends JustToTryAlgorithm {

    private boolean useReplicate(Cloudlet cloudlet) {
        return WFCReplication.getMasterToSlaveMap().containsKey(cloudlet) ||
                WFCReplication.getSlaveToMasterMap().containsKey(cloudlet);
    }

    private double useNewVmPt(ContainerVmType type, Cloudlet cloudlet) {
        return WFCConstants.VM_INITIAL_TIME + WFCConstants.CONTAINER_INITIAL_TIME
                + cloudlet.getTransferTime()
                + cloudlet.getCloudletLength() / type.getMips();
    }

    /**
     * 计算如果推迟到下一次调度，以当前总体的工作负载和任务，估计任务在多久以后完成
     *
     * @param averageIdleTime
     * @param cloudlet
     * @return
     */
    private double postponeToNextSchedule(double averageIdleTime, ContainerCloudlet cloudlet) {
        return averageIdleTime + WFCConstants.CONTAINER_INITIAL_TIME + cloudlet.getTransferTime()
                + cloudlet.getCloudletLength() / WFCConstants.EXTRA_LARGE_MIPS;
    }

    /**
     * 创建新VM，返回id
     *
     * @param type
     * @param brokerId
     * @return
     */
    private ContainerVm tryToCreateVm(ContainerVmType type, int brokerId) {
//        if(getToCreateVmList().size() > 50){
//            return -1000;
//        }
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
        getToCreateVmList().add(aNewVm);
        return aNewVm;
    }


    @Override
    public void run(int brokerId) {
        //获取任务数量
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
        if (WFCConstants.PRINT_SCHEDULE_WAITING_LIST) {
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":还有" + numCloudlet + "个任务等待调度");
        }
//        setContainerList(new ArrayList<>());
        int numCloudletsOnVm = 0;

        //记录每个任务在最快的虚拟机上的处理时间
        Map<Integer, Double> leastPtMap = new HashMap<>();

        //在任务正式调度前，先默认它们没有找到适合的VM
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            cloudlet.setVmId(-1000);
            leastPtMap.put(cloudlet.getCloudletId(), cloudlet.getTransferTime() + cloudlet.getCloudletLength() / WFCConstants.EXTRA_LARGE_MIPS);
        }



        //对于每一个待调度的任务
        for (int i = 0; i < numCloudlet; i++) {
            boolean vmFound = false;
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            //！！！这里可能会出现问题：在WFCReplication.getMasterToSlaveMap()记录该任务的复制后，调用了本算法，但调度队列中依然没有复制后的那个任务
            //如果已经发送了复制该任务的请求，但是还没有完成该任务的复制，就要中断调度过程
            if (getHasSentReplicateCloudlets().contains(cloudlet) && !WFCReplication.getMasterToSlaveMap().containsKey(cloudlet)) {
                break;
            }

            // 记录每个VM上已经分配的任务数量
            Map<ContainerVm, Integer> vmCloudletNumMap = new HashMap<>();
            // 记录每个VM转为空闲状态还需要多长时间
            Map<ContainerVm, Double> idleTimeMap = new HashMap<>();
            double sumIdleTime = 0.0;
            // 其实每个VM上分配的任务数量不需要在每个任务分配时都检查四处，
            // 但是既然每次都要检查idletime，就顺便检查已分配的任务数量
            for (int j = 0; j < numVM; j++) {
                // 该VM多久以后达到空闲状态
                double idleTime = 0.0;
                // 已经分配给该VM的任务数量
                int alreadyAllocatedCloudlet = 0;
                // 防止重复查看任务
                Set<Integer> hasCheckedIds = new HashSet<>();
                ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                // 查看四处，容器上的任务，滞留的任务，
                // WFCScheduler刚发送的任务，本函数中刚分配的任务
                if (containerVm.getContainerList().size() > 0) {
                    Container container = containerVm.getContainerList().get(0);
                    ContainerCloudletSchedulerJustToTry containerCloudletScheduler = (ContainerCloudletSchedulerJustToTry) container.getContainerCloudletScheduler();
                    idleTime += containerCloudletScheduler.getRemainingTime();
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
                        idleTime += cloudlet1.getTransferTime() + cloudlet1.getCloudletLength() / containerVm.getType().getMips();
                        if (containerVm.getContainerList().size() == 0 || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet1.getWorkflowId()) {
                            idleTime += WFCConstants.CONTAINER_INITIAL_TIME;
                        }
                        hasCheckedIds.add(cloudlet1.getCloudletId());
                    }
                }
                // 同一时间，刚发送出去的任务
                for (ContainerCloudlet cloudlet1 : getLastSentCloudlets()) {
                    if (cloudlet1.getVmId() == containerVm.getId() && !hasCheckedIds.contains(cloudlet1.getCloudletId())) {
                        alreadyAllocatedCloudlet++;
                        idleTime += cloudlet1.getTransferTime() + cloudlet1.getCloudletLength() / containerVm.getType().getMips();
                        if (containerVm.getContainerList().size() == 0 || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet1.getWorkflowId()) {
                            idleTime += WFCConstants.CONTAINER_INITIAL_TIME;
                        }
                        hasCheckedIds.add(cloudlet1.getCloudletId());
                    }
                }
                // 本次函数中，刚刚分配给该VM的任务
                for (int k = 0; k < i; k++) {
                    ContainerCloudlet cloudlet1 = (ContainerCloudlet) getCloudletList().get(k);
                    if (cloudlet1.getVmId() == containerVm.getId() && !hasCheckedIds.contains(cloudlet1.getCloudletId())) {
                        alreadyAllocatedCloudlet++;
                        idleTime += cloudlet1.getTransferTime() + cloudlet1.getCloudletLength() / containerVm.getType().getMips();
                        if (containerVm.getContainerList().size() == 0 || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet1.getWorkflowId()) {
                            idleTime += WFCConstants.CONTAINER_INITIAL_TIME;
                        }
                        hasCheckedIds.add(cloudlet1.getCloudletId());
                    }
                }
                // 只是用于统计当前有多少任务在VM上
                if(i == 0){
                    numCloudletsOnVm += alreadyAllocatedCloudlet;
                }
                vmCloudletNumMap.put(containerVm, alreadyAllocatedCloudlet);
                idleTimeMap.put(containerVm, idleTime);
                sumIdleTime += idleTime;
            }
            if(i == 0){
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ":当前大约有", numCloudletsOnVm, "个任务在VM上");
            }

            // 所有VM，平均多久后达到空闲
            double averageIdleTime = sumIdleTime / numVM;
            // 判断是否采用复制的方式容错
            if (Math.min(averageIdleTime, WFCConstants.VM_INITIAL_TIME + WFCConstants.CONTAINER_INITIAL_TIME) +
                    leastPtMap.get(cloudlet.getCloudletId()) > (cloudlet.getDeadline() - CloudSim.clock()) / 2 &&
                    cloudlet.getWorkflowId() != 0
            ) {
                //防止重复复制，getHasSentReplicateCloudlets()记录的是已经发送过，要复制的任务
                if (!getHasSentReplicateCloudlets().contains(cloudlet) && !WFCReplication.getSlaveToMasterMap().containsKey(cloudlet)) {
                    getReplicateCloudlets().add(cloudlet);
                    if (WFCConstants.PRINT_REPLICATE_RETRY) {
                        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "即将复制任务：", cloudlet.getCloudletId());
                    }
                    break;
                }
            }

            //判断有多少VM已经冗余了（有可能不能在deadline前完成不是因为VM不够，而是deadline异常）
            int redundantVm = 0;
            // 开始挑选VM
            int cheapestIndex = -1;
            double leastCost = -1;
            // 如果每个VM都不满足截止时间，哪个VM最快完成（当前时间多久后）
            int fastestIndex = -1;
            // 现有的VM，最快多久以后能完成任务
            double fastestTime = Double.MAX_VALUE;
            for (int j = 0; j < numVM; j++) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                if (vmCloudletNumMap.get(containerVm) >= 2) {
                    continue;
                }
                //对于stage in任务，只要能完成即可
                if (cloudlet.getWorkflowId() == 0) {
                    cheapestIndex = j;
                    break;
                }
                // 计算任务在该VM上的最早完成时间(当前时间多久后)
                double eft = idleTimeMap.get(containerVm) + cloudlet.getTransferTime() + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                if (containerVm.getContainerList().size() == 0 || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                    eft += WFCConstants.CONTAINER_INITIAL_TIME;
                }
                if (fastestIndex == -1 || eft < fastestTime) {
                    fastestIndex = j;
                    fastestTime = eft;
                }
                // 如果完成时间不满足要求，就跳过
                // 如果该任务使用复制容错
                if (useReplicate(cloudlet)) {
                    if (eft > cloudlet.getDeadline() - CloudSim.clock()) {
                        if (vmCloudletNumMap.get(containerVm) == 0) {
                            redundantVm++;
                        }
                        continue;
                    }
                } else {
                    if (eft > (cloudlet.getDeadline() - CloudSim.clock()) / 2) {
                        if (vmCloudletNumMap.get(containerVm) == 0) {
                            redundantVm++;
                        }
                        continue;
                    }
                }
                if (cheapestIndex == -1 || containerVm.getCostOfCloudlet(cloudlet) < leastCost) {
                    cheapestIndex = j;
                    leastCost = containerVm.getCostOfCloudlet(cloudlet);
                }
            }
//            if(i == 0){
//                Log.printConcatLine(CloudSim.clock(), ":多余的VM数量：", redundantVm);
//            }


            //如果找到了满足截止时间的现有的VM
            if (cheapestIndex >= 0) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(cheapestIndex);
                cloudlet.setVmId(containerVm.getId());
                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                }
                continue;
            }
            //看看推迟到下一个调度周期能不能满足截止时间，如果可以，推迟
            //凭什么觉得等有VM空闲后，本任务就能执行？还得看看有多少任务在等待
            if (getVmList().size() + getToCreateVmList().size() >= WFCConstants.VM_MINIMUM_NUMBER
                    && postponeToNextSchedule(averageIdleTime, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
//                Log.printConcatLine(CloudSim.clock(), ":决定推迟任务", cloudlet.getCloudletId(), "到下个周期");
                continue;
            }
            // 把正在创建的VM纳入考虑范围，如果有某个正在创建的VM，假如其当前存在就可以完成任务，满足deadline，就不创建新VM
            boolean toCreateVmSatisfy = false;
            for(ContainerVm containerVm : getToCreateVmList()){
                double dummyFinishTime = Math.max(containerVm.getDummyIdleTime(), CloudSim.clock())
                        + cloudlet.getTransferTime()
                        + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                if(containerVm.getDummyWorkflowId() != cloudlet.getWorkflowId()){
                    dummyFinishTime += WFCConstants.CONTAINER_INITIAL_TIME;
                }
                if(dummyFinishTime <= cloudlet.getDeadline()){
                    containerVm.setDummyIdleTime(dummyFinishTime);
                    containerVm.setDummyWorkflowId(cloudlet.getWorkflowId());
                    toCreateVmSatisfy = true;
                    break;
                }
            }
            if(toCreateVmSatisfy){
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "要创建的VM如果存在就满足截止时间，所以不再创建VM");
                continue;
            }

            // 如果只是因为这个任务截止时间太短才不满足上述条件，那就不会为这个任务创建新VM
            // 而是为其选择推迟到下次调度或者分配到现有VM
            if(cloudlet.getTransferTime() + cloudlet.getCloudletLength() / ContainerVmType.EXTRA_LARGE.getMips()
                    > cloudlet.getDeadline() - CloudSim.clock()){
                if(fastestTime <= postponeToNextSchedule(averageIdleTime, cloudlet)){
                    ContainerVm containerVm = (ContainerVm) getVmList().get(fastestIndex);
                    cloudlet.setVmId(containerVm.getId());
                }
                continue;
            }

//            Log.printConcatLine(CloudSim.clock(), ":不能推迟任务", cloudlet.getCloudletId(),
//                    "，因为其截止时间为", cloudlet.getDeadline(), "属于工作流", cloudlet.getWorkflowId());

            //如果推迟到下一次调度也不能满足截止时间，就创建新VM
            ContainerVm newVm = null;
            boolean newVmSatisfyDeadline = false;
            if (useReplicate(cloudlet) && !getCloudletsLeadToNewVm().containsKey(cloudlet.getCloudletId())) {
                Log.printConcatLine(CloudSim.clock(), ":试图创建VM，因为任务", cloudlet.getCloudletId());
                if (useNewVmPt(ContainerVmType.SMALL, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个SMALL的VM
                    newVm = tryToCreateVm(ContainerVmType.SMALL, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.MEDIUM, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个MEDIUM的VM
                    newVm = tryToCreateVm(ContainerVmType.MEDIUM, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.LARGE, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个LARGE的VM
                    newVm = tryToCreateVm(ContainerVmType.LARGE, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.EXTRA_LARGE, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个EXTRA_LARGE的VM
                    newVm = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId);
                    newVmSatisfyDeadline = true;
                } else {
                    newVm = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId);
                }
                getCloudletsLeadToNewVm().put(cloudlet.getCloudletId(), newVm);
            } else if (!getCloudletsLeadToNewVm().containsKey(cloudlet.getCloudletId())) {
                Log.printConcatLine(CloudSim.clock(), ":试图创建VM，因为任务", cloudlet.getCloudletId());
                if (useNewVmPt(ContainerVmType.SMALL, cloudlet) * 2 - WFCConstants.VM_INITIAL_TIME
                        <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个SMALL的VM
                    newVm = tryToCreateVm(ContainerVmType.SMALL, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.MEDIUM, cloudlet) * 2 - WFCConstants.VM_INITIAL_TIME
                        <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个MEDIUM的VM
                    newVm = tryToCreateVm(ContainerVmType.MEDIUM, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.LARGE, cloudlet) * 2 - WFCConstants.VM_INITIAL_TIME
                        <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个LARGE的VM
                    newVm = tryToCreateVm(ContainerVmType.LARGE, brokerId);
                    newVmSatisfyDeadline = true;
                } else if (useNewVmPt(ContainerVmType.EXTRA_LARGE, cloudlet) * 2 - WFCConstants.VM_INITIAL_TIME
                        <= cloudlet.getDeadline() - CloudSim.clock()) {
                    //创建一个EXTRA_LARGE的VM
                    newVm = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId);
                    newVmSatisfyDeadline = true;
                } else {
                    newVm = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId);
                }
                getCloudletsLeadToNewVm().put(cloudlet.getCloudletId(), newVm);
            }
            //运行到这一行时不一定是本轮创建了新VM，可能是之间创建的


            //看看分配到新VM能不能满足截止时间，可以，就分配
            if (newVmSatisfyDeadline) {
                ContainerVm targetVm = getCloudletsLeadToNewVm().get(cloudlet.getCloudletId());
                targetVm.setDummyWorkflowId(cloudlet.getWorkflowId());
                targetVm.setDummyIdleTime(CloudSim.clock() + WFCConstants.CONTAINER_INITIAL_TIME
                        + cloudlet.getTransferTime() + cloudlet.getCloudletLength() / targetVm.getType().getMips());
                cloudlet.setVmId(newVm.getId());
                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(CloudSim.clock() + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                }
                continue;
            }
            //如果不可以，取三种方案中最快的(三个时间都是多久以后)
            // 0代表推迟到下一次调度，1代表分配到新VM，2代表现有VM
            int solution = 0;
            List<Double> solutionsTime = new ArrayList<>();
            solutionsTime.add(postponeToNextSchedule(averageIdleTime, cloudlet));
            solutionsTime.add(useNewVmPt(ContainerVmType.EXTRA_LARGE, cloudlet));
            if (fastestIndex >= 0) {
                solutionsTime.add(fastestTime);
            }
            for (int j = 0; j < solutionsTime.size(); j++) {
                if (solutionsTime.get(j) < solutionsTime.get(solution)) {
                    solution = j;
                }
            }
            switch (solution) {
                case 0:
                    break;
                case 1:
                    // 这个vmId可能是之前创建的
                    ContainerVm targetVm = getCloudletsLeadToNewVm().get(cloudlet.getCloudletId());
                    cloudlet.setVmId(targetVm.getId());
                    if(targetVm.getDummyWorkflowId() == 0){
                        targetVm.setDummyWorkflowId(cloudlet.getWorkflowId());
                        targetVm.setDummyIdleTime(CloudSim.clock() + WFCConstants.CONTAINER_INITIAL_TIME
                                + cloudlet.getTransferTime() + cloudlet.getCloudletLength() / targetVm.getType().getMips());
                    }
                    if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                        Log.printLine(CloudSim.clock() + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                    }
                    break;
                case 2:
                    ContainerVm containerVmTarget = (ContainerVm) getVmList().get(fastestIndex);
                    cloudlet.setVmId(containerVmTarget.getId());
                    if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                        Log.printLine(CloudSim.clock() + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                    }
                    break;
            }
        }//一个任务的分配结束

    }
}
