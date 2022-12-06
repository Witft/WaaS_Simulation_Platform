package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;
import org.wfc.scheduler.ContainerCloudletSchedulerJustToTry;
import org.workflowsim.ContainerVmType;

import java.text.DecimalFormat;
import java.util.*;

/**
 * 在
 */
public class FutureVMSchedulingDynamicAlgorithm extends SchedulingDynamicWorkloadsAlgorithm{
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
        // 记录每个workflow id对应的空闲的VM
        Map<Integer, List<ContainerVm>> idleVmOfWorkflowId = new HashMap<>();
        // 记录所有空闲的VM
        List<ContainerVm> idleVmList = new ArrayList<>();
        // 记录每个VM上已经分配的任务数量
        Map<ContainerVm, Integer> vmCloudletNumMap = new HashMap<>();
        // 记录每个VM转为空闲状态还需要多长时间
        Map<ContainerVm, Double> idleTimeMap = new HashMap<>();
        for (int i = 0; i < numVM; i++) {
            // 该VM多久以后达到空闲状态
            double idleTime = 0.0;
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
            vmCloudletNumMap.put(containerVm, alreadyAllocatedCloudlet);
            idleTimeMap.put(containerVm, idleTime);
            numOfCloudletsOnVm += alreadyAllocatedCloudlet;
            // 如果该VM是空闲的，要存储进List
            if (0 == alreadyAllocatedCloudlet) {
                idleVmList.add(containerVm);
                if (containerVm.getContainerList().size() > 0) {
                    if (!idleVmOfWorkflowId.containsKey(containerVm.getContainerList().get(0).getWorkflowId())) {
                        List<ContainerVm> newList = new ArrayList<>();
                        idleVmOfWorkflowId.put(containerVm.getContainerList().get(0).getWorkflowId(), newList);
                    }
                    idleVmOfWorkflowId.get(containerVm.getContainerList().get(0).getWorkflowId()).add(containerVm);
                }
            }
        }
        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()),
                ":当前有", numOfCloudletsOnVm, "个任务在", numVM, "个VM上");

        // 用来评价正在创建的VM的影响
        double dummyIdleTime = Math.max(getDummyIdleTime().get(0), CloudSim.clock());
        if (getToCreateVmMap().size() == 0) {
            dummyIdleTime = CloudSim.clock();
        }

        //对于每一个待调度的任务
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);

//            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":开始调度任务" + cloudlet.getCloudletId());

            //单独处理stage-in任务
            if (cloudlet.getWorkflowId() == 0 && numVM > 0) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(0);
                cloudlet.setVmId(containerVm.getId());
                break;
            }

            // 首先找有相同容器类型的空闲的VM，如果找不到，就找其它空闲VM
            // 如果没有空闲VM，就判断是否在下个调度周期用最慢类型的VM都能满足截止时间
            // 如果满足，就推迟到下个周期
            // 如果不满足，就租用新VM，找到截止时间完成前提下，成本最低的VM
            // 如果任何类型的新VM都不满足截止时间，就找个最快的VM类型

            // 首先找容器类型相同的VM
            // 满足截止时间的前提下，成本最低的VM
            int cheapestIndex = -1;
            double leastCost = Double.MAX_VALUE;
            if (idleVmOfWorkflowId.containsKey(cloudlet.getWorkflowId())) {
                List<ContainerVm> sameContainerVmList = idleVmOfWorkflowId.get(cloudlet.getWorkflowId());
                for (int j = 0; j < sameContainerVmList.size(); j++) {
                    ContainerVm containerVm = sameContainerVmList.get(j);
                    if (cloudlet.getTransferTime()
                            + cloudlet.getCloudletLength() / containerVm.getType().getMips()
                            <= cloudlet.getDeadline() - CloudSim.clock()) {
                        double curCost = calculateCost(containerVm, cloudlet);
                        if (cheapestIndex == -1 || curCost < leastCost) {
                            cheapestIndex = j;
                            leastCost = curCost;
                        }
                    }
                }
            }

            if (cheapestIndex >= 0) {
//                ContainerVm containerVm = (ContainerVm) getVmList().get(cheapestIndex);
                ContainerVm containerVm = idleVmOfWorkflowId.get(cloudlet.getWorkflowId()).get(cheapestIndex);
                cloudlet.setVmId(containerVm.getId());
                //更新对应VM上的排队的任务数量，增加1
                vmCloudletNumMap.put(containerVm, vmCloudletNumMap.get(containerVm) + 1);
                //更新对应VM的空闲时间
                double pt = cloudlet.getTransferTime() + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                if (0 == containerVm.getContainerList().size() || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                    pt += WFCConstants.CONTAINER_INITIAL_TIME;
                }
                idleTimeMap.put(containerVm, idleTimeMap.get(containerVm) + pt);
                //更新idleVmOfWorkflowId
                idleVmOfWorkflowId.get(cloudlet.getWorkflowId()).remove(containerVm);
                //更新idleVmList
                idleVmList.remove(containerVm);

                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                }
                continue;
            }

            // 运行到这里说明没有找到相同容器类型且空闲，且满足截止时间的VM，所以找其它空闲的VM
            int fastestIndex = -1;
            double fastestTime = Double.MAX_VALUE;
            for (int j = 0; j < idleVmList.size(); j++) {
                ContainerVm containerVm = idleVmList.get(j);
                //如果满足截止时间
                if (WFCConstants.CONTAINER_INITIAL_TIME
                        + cloudlet.getTransferTime()
                        + cloudlet.getCloudletLength() / containerVm.getType().getMips()
                        <= cloudlet.getDeadline() - CloudSim.clock()) {
                    double curCost = calculateCost(containerVm, cloudlet);
                    if (cheapestIndex == -1 || curCost < leastCost) {
                        cheapestIndex = j;
                        leastCost = curCost;
                    }
                } else {
//                    if (fastestIndex == -1 || WFCConstants.CONTAINER_INITIAL_TIME + cloudlet.getTransferTime()
//                        + cloudlet.getCloudletLength() / containerVm.getType().getMips() < fastestTime)
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
            }
            if (cheapestIndex >= 0) {
                ContainerVm containerVm = idleVmList.get(cheapestIndex);
                cloudlet.setVmId(containerVm.getId());
                //更新对应VM上的排队的任务数量，增加1
                vmCloudletNumMap.put(containerVm, vmCloudletNumMap.get(containerVm) + 1);
                //更新对应VM的空闲时间
                double pt = cloudlet.getTransferTime() + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                if (0 == containerVm.getContainerList().size() || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                    pt += WFCConstants.CONTAINER_INITIAL_TIME;
                }
                idleTimeMap.put(containerVm, idleTimeMap.get(containerVm) + pt);
                //更新idleVmOfWorkflowId
                if (containerVm.getContainerList().size() > 0) {
                    idleVmOfWorkflowId.get(containerVm.getContainerList().get(0).getWorkflowId()).remove(containerVm);
                }
                //更新idleVmList
                idleVmList.remove(containerVm);

                if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                    Log.printLine(CloudSim.clock() + ":任务"
                            + cloudlet.getCloudletId() + "被分配给了虚拟机"
                            + cloudlet.getVmId());
                }
                continue;
            }

            // 运行到这里说明没有找到空闲且满足截止时间的VM
            // 如果不可以推迟到下个调度周期，且deadline合理，且正在创建的VM不够，则创建新VM
            // 这里是用 WFCConstants.MIN_TIME_BETWEEN_EVENTS 还是用 averageIdleTime？
            int newVmId = -1000;
            ContainerVmType newVmType = null;
            if (cloudlet.getTransferTime() + cloudlet.getCloudletLength() / WFCConstants.EXTRA_LARGE_MIPS
                    <= cloudlet.getDeadline() - CloudSim.clock()
                    && WFCConstants.MIN_TIME_BETWEEN_EVENTS + cloudlet.getTransferTime()
                    + cloudlet.getCloudletLength() / WFCConstants.SMALL_MIPS > cloudlet.getDeadline() - CloudSim.clock()
                    && !creatingVMEnough(dummyIdleTime, cloudlet)
            ) {
                DecimalFormat df = new DecimalFormat("#.00");
                Log.printConcatLine(df.format(CloudSim.clock()), ":决定创建VM，因为任务", cloudlet.getCloudletId(),
                        "截止时间是", df.format(cloudlet.getDeadline()), "，属于工作流", cloudlet.getWorkflowId());
                if (useNewVmPt(ContainerVmType.SMALL, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.SMALL, brokerId, cloudlet.getCloudletId());
                    newVmType = ContainerVmType.SMALL;
                } else if (useNewVmPt(ContainerVmType.MEDIUM, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.MEDIUM, brokerId, cloudlet.getCloudletId());
                    newVmType = ContainerVmType.MEDIUM;
                } else if (useNewVmPt(ContainerVmType.LARGE, cloudlet) <= cloudlet.getDeadline() - CloudSim.clock()) {
                    newVmId = tryToCreateVm(ContainerVmType.LARGE, brokerId, cloudlet.getCloudletId());
                    newVmType = ContainerVmType.LARGE;
                } else {
                    newVmId = tryToCreateVm(ContainerVmType.EXTRA_LARGE, brokerId, cloudlet.getCloudletId());
                    newVmType = ContainerVmType.EXTRA_LARGE;
                }
//                cloudlet.setVmId(newVmId);

                // 更新averageDummyIdleTime
                // 这里的逻辑是，如果正在创建的VM从0变为1，那这一个VM不会继承之前的dummyIdleTime
                if (dummyIdleTime >= CloudSim.clock() && getToCreateVmMap().size() > 1) {
                    dummyIdleTime = (dummyIdleTime - CloudSim.clock()) * (getToCreateVmMap().size() - 1) / getToCreateVmMap().size() + CloudSim.clock();
                } else {
                    // 如果dummyIdleTime比当前时间还早，或者刚刚创建了唯一的一个创建中的VM
                    dummyIdleTime = CloudSim.clock();
                }
                getDummyIdleTime().set(0, dummyIdleTime);

                // 租用了新VM也不一定就会把任务放在新VM上，而是要比较放在新VM上，和分配到已有的VM上哪个时间少
                double allocateToNewVmTime = WFCConstants.VM_INITIAL_TIME + WFCConstants.CONTAINER_INITIAL_TIME
                        + cloudlet.getTransferTime() + cloudlet.getCloudletLength() / newVmType.getMips();
                double allocateToCurVmTime = fastestTime;
                if(allocateToCurVmTime <= allocateToNewVmTime){
                    ContainerVm containerVm = idleVmList.get(fastestIndex);
                    cloudlet.setVmId(containerVm.getId());
                    //更新对应VM上的排队的任务数量，增加1
                    vmCloudletNumMap.put(containerVm, vmCloudletNumMap.get(containerVm) + 1);
                    //更新对应VM的空闲时间
                    double pt = cloudlet.getTransferTime() + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                    if (0 == containerVm.getContainerList().size() || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                        pt += WFCConstants.CONTAINER_INITIAL_TIME;
                    }
                    idleTimeMap.put(containerVm, idleTimeMap.get(containerVm) + pt);
                    //更新idleVmOfWorkflowId
                    if (containerVm.getContainerList().size() > 0) {
                        idleVmOfWorkflowId.get(containerVm.getContainerList().get(0).getWorkflowId()).remove(containerVm);
                    }
                    //更新idleVmList
                    idleVmList.remove(containerVm);

                    if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                        Log.printLine(CloudSim.clock() + ":任务"
                                + cloudlet.getCloudletId() + "被分配给了虚拟机"
                                + cloudlet.getVmId());
                    }
                }else{
                    if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                        Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":任务" + cloudlet.getCloudletId() + "被分配给了要创建的虚拟机" + newVmId);
                    }
                    cloudlet.setVmId(newVmId);
                    // 更新dummy
                    dummyIdleTime = Math.max(dummyIdleTime, CloudSim.clock())
                            + (WFCConstants.CONTAINER_INITIAL_TIME
                            + cloudlet.getTransferTime()
                            + cloudlet.getCloudletLength() / newVmType.getMips())
                            / getToCreateVmMap().size();
                    getDummyIdleTime().set(0, dummyIdleTime);
                }
            } else {
                // 如果只是因为正创建的VM足够而不创建新vm，就更新averageDummyIdleTime
                if(creatingVMEnough(dummyIdleTime, cloudlet)
                        && cloudlet.getTransferTime() + cloudlet.getCloudletLength() / WFCConstants.EXTRA_LARGE_MIPS
                        <= cloudlet.getDeadline() - CloudSim.clock()
                        && WFCConstants.MIN_TIME_BETWEEN_EVENTS + cloudlet.getTransferTime()
                        + cloudlet.getCloudletLength() / WFCConstants.SMALL_MIPS > cloudlet.getDeadline() - CloudSim.clock()){
                    dummyIdleTime = Math.max(dummyIdleTime, CloudSim.clock())
                            + (cloudlet.getTransferTime()
                            + cloudlet.getCloudletLength() / ContainerVmType.EXTRA_LARGE.getMips())
                            / getToCreateVmMap().size();
                    getDummyIdleTime().set(0, dummyIdleTime);
                }
                //如果因为截止时间很宽松，或者截止时间不合理，或者正创建的VM足够而不创建新vm，就比较一下用现有VM和推迟
                double delayToNextTime = WFCConstants.MIN_TIME_BETWEEN_EVENTS + WFCConstants.CONTAINER_INITIAL_TIME
                        + cloudlet.getTransferTime() + cloudlet.getCloudletLength() / WFCConstants.SMALL_MIPS;
                double allocateToCurVmTime = fastestTime;
                if (allocateToCurVmTime <= delayToNextTime) {
                    ContainerVm containerVm = idleVmList.get(fastestIndex);
                    cloudlet.setVmId(containerVm.getId());
                    //更新对应VM上的排队的任务数量，增加1
                    vmCloudletNumMap.put(containerVm, vmCloudletNumMap.get(containerVm) + 1);
                    //更新对应VM的空闲时间
                    double pt = cloudlet.getTransferTime() + cloudlet.getCloudletLength() / containerVm.getType().getMips();
                    if (0 == containerVm.getContainerList().size() || containerVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId()) {
                        pt += WFCConstants.CONTAINER_INITIAL_TIME;
                    }
                    idleTimeMap.put(containerVm, idleTimeMap.get(containerVm) + pt);
                    //更新idleVmOfWorkflowId
                    if (containerVm.getContainerList().size() > 0) {
                        idleVmOfWorkflowId.get(containerVm.getContainerList().get(0).getWorkflowId()).remove(containerVm);
                    }
                    //更新idleVmList
                    idleVmList.remove(containerVm);

                    if (WFCConstants.PRINT_SCHEDULE_RESULT) {
                        Log.printLine(CloudSim.clock() + ":任务"
                                + cloudlet.getCloudletId() + "被分配给了虚拟机"
                                + cloudlet.getVmId());
                    }
                }
            }
        }// 一个任务的调度完成
    }

    protected boolean creatingVMEnough(double dummyIdleTime, Cloudlet cloudlet) {
        // averageDummyIdleTime是一个绝对时间点
        double dummyFinishTime = Math.max(dummyIdleTime, CloudSim.clock())
                + cloudlet.getTransferTime()
                + cloudlet.getCloudletLength() / ContainerVmType.EXTRA_LARGE.getMips();
        return dummyFinishTime <= cloudlet.getDeadline();
    }
}
