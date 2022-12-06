package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;
import org.wfc.scheduler.ContainerCloudletSchedulerJustToTry;
import org.workflowsim.ContainerVmType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OriginalSchedulingDynamicWorkloadsAlgorithm extends SchedulingDynamicWorkloadsAlgorithm{

    @Override
    public void run(int brokerId) {
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
        if (WFCConstants.PRINT_SCHEDULE_WAITING_LIST) {
            Log.printLine(CloudSim.clock() + ":还有" + numCloudlet + "个任务等待调度");
        }

        int numOfCloudletsOnVm = 0;

        //在任务正式调度前，先默认它们没有找到适合的VM
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            cloudlet.setVmId(-1000);
        }

        //对于每一个待调度的任务
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            //单独处理stage-in任务
            if(cloudlet.getWorkflowId() == 0 && numVM > 0){
                ContainerVm containerVm = (ContainerVm) getVmList().get(0);
                cloudlet.setVmId(containerVm.getId());
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
                // 查看四处，容器上的任务，滞留的任务，WFCScheduler刚发送的任务，
                // 本函数中刚分配的任务容器上的任务
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
                if(i == 0){
                    numOfCloudletsOnVm += alreadyAllocatedCloudlet;
                }
                vmCloudletNumMap.put(containerVm, alreadyAllocatedCloudlet);
                idleTimeMap.put(containerVm, idleTime);
                sumIdleTime += idleTime;
            }
            // 所有VM，平均多久后达到空闲
            double averageIdleTime = sumIdleTime / numVM;
            if(i == 0){
                Log.printConcatLine(CloudSim.clock(), "大约有 ", numOfCloudletsOnVm, " 个任务在VM上");
            }

            // 首先找有相同容器类型的空闲的VM，如果找不到，就找其它空闲VM
            // 如果没有空闲VM，就判断是否在下个调度周期用最慢类型的VM都能满足截止时间
            // 如果满足，就推迟到下个周期
            // 如果不满足，就租用新VM，找到截止时间完成前提下，成本最低的VM
            // 如果任何类型的新VM都不满足截止时间，就找个最快的VM类型
            boolean sameContainerVmFound = false;
            boolean idleVmFound = false;
            for (int j = 0; j < numVM; j++) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                if (vmCloudletNumMap.get(containerVm) == 0){
                    if(containerVm.getContainerList().size() > 0
                            && containerVm.getContainerList().get(0).getWorkflowId() == cloudlet.getWorkflowId()){
                        sameContainerVmFound = true;
                        idleVmFound = true;
                        break;
                    }
                    idleVmFound = true;
                }
            }
            //满足截止时间的前提下，成本最低的VM
            int cheapestIndex = -1;
            ContainerVmType leastCostType = ContainerVmType.EXTRA_LARGE;
            double leastCost = Double.MAX_VALUE;
            if(sameContainerVmFound){
                for (int j = 0; j < numVM; j++) {
                    ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                    if(vmCloudletNumMap.get(containerVm) > 0){
                        continue;
                    }
                    if(cloudlet.getTransferTime()
                            + cloudlet.getCloudletLength() / containerVm.getType().getMips()
                            <= cloudlet.getDeadline() - CloudSim.clock()){
                        double curCost = calculateCost(containerVm, cloudlet);
                        if(cheapestIndex == -1 || curCost < leastCost){
                            cheapestIndex = j;
                            leastCost = curCost;
                        }
                    }
                }
            }
            if(cheapestIndex >= 0){
                ContainerVm containerVm = (ContainerVm) getVmList().get(cheapestIndex);
                cloudlet.setVmId(containerVm.getId());
                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(CloudSim.clock() + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                }
                continue;
            }
            // 运行到这里说明没有找到相同容器类型且空闲，且满足截止时间的VM
            // 所以找其它空闲的VM
            if(idleVmFound){
                for (int j = 0; j < numVM; j++) {
                    ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                    if(vmCloudletNumMap.get(containerVm) > 0){
                        continue;
                    }
                    //如果满足截止时间
                    if (WFCConstants.CONTAINER_INITIAL_TIME
                            + cloudlet.getTransferTime()
                            + cloudlet.getCloudletLength() / containerVm.getType().getMips()
                            <= cloudlet.getDeadline() - CloudSim.clock()){
                        double curCost = calculateCost(containerVm, cloudlet);
                        if(cheapestIndex == -1 || curCost < leastCost){
                            cheapestIndex = j;
                            leastCost = curCost;
                        }
                    }
                }
            }
            if(cheapestIndex >= 0){
                ContainerVm containerVm = (ContainerVm) getVmList().get(cheapestIndex);
                cloudlet.setVmId(containerVm.getId());
                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(CloudSim.clock() + ":任务"
                            + cloudlet.getCloudletId() + "被分配给了虚拟机"
                            + cloudlet.getVmId());
                }
                continue;
            }
            // 运行到这里说明没有找到空闲且满足截止时间的VM
            // 判断是否可以推迟到下个调度周期，不可以就需要租用新VM
            // 这里是用 WFCConstants.MIN_TIME_BETWEEN_EVENTS 还是用 averageIdleTime？
            if(WFCConstants.MIN_TIME_BETWEEN_EVENTS + cloudlet.getTransferTime()
                    + cloudlet.getCloudletLength() / WFCConstants.SMALL_MIPS > cloudlet.getDeadline() - CloudSim.clock()){
                int newVmId = -1000;
                if (useNewVmPt(ContainerVmType.SMALL, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.SMALL, brokerId, cloudlet.getCloudletId());
                } else if (useNewVmPt(ContainerVmType.MEDIUM, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.MEDIUM, brokerId, cloudlet.getCloudletId());
                } else if (useNewVmPt(ContainerVmType.LARGE, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.LARGE, brokerId, cloudlet.getCloudletId());
                } else {
                    newVmId = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId, cloudlet.getCloudletId());
                }
                cloudlet.setVmId(newVmId);
            }//否则不调度，推迟到下一次调度
        }// 一个任务的调度完成
    }
}
