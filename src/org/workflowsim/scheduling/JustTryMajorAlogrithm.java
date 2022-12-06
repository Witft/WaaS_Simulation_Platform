package org.workflowsim.scheduling;

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
import org.workflowsim.ContainerVmType;
import org.workflowsim.JustToTryPowerContainerVm;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JustTryMajorAlogrithm extends JustToTryAlgorithm{
    @Override
    public void run(int brokerId){
        //获取任务数量
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
        if(WFCConstants.PRINT_SCHEDULE_WAITING_LIST){
            Log.printLine("还有" + numCloudlet + "个任务等待调度");
        }
        //记录每个VM上已经分配的任务数量
        Map<Integer, List<Integer>> cloudletAllocatedMap = new HashMap<>();
        setContainerList(new ArrayList<Container>());
        double needNewVmNum = 0;

        //在任务正式调度前，先默认它们没有找到适合的VM
        for(int i = 0; i < numCloudlet; i++){
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            cloudlet.setVmId(-1000);
//            Log.printConcatLine(CloudSim.clock(), "马上调度任务：", cloudlet.getCloudletId());
        }


        //对于每一个待调度的任务
        for(int i = 0; i < numCloudlet; i++){
            boolean vmFound = false;
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            if(cloudlet.getCloudletId() % 2 == 0 && !getHasSentReplicateCloudlets().contains(cloudlet) && !WFCReplication.getSlaveToMasterMap().containsKey(cloudlet)){
                getReplicateCloudlets().add(cloudlet);
                if(WFCConstants.PRINT_REPLICATE_RETRY){
                    Log.printConcatLine(CloudSim.clock(), "即将复制任务：", cloudlet.getCloudletId());
                }
                break;
            }
            int fastestIndex = -1;
            double fastestTime = -1;
            for(int j = 0; j < numVM; j++){
                ContainerVm containerVm = (ContainerVm) getVmList().get(j);
                //如果刚刚决定要删除VM，就不再为其分配
                if(getToDestroyVmList().contains(containerVm)){
                    continue;
                }
                //查看cloudletAllocatedMap中是否有该VM，如果有，直接取值
                //如果没有，查看两处，一是滞留的任务中是否有该VM的，二是如果该VM有容器，获取容器上的所有任务，累加后添加到cloudletAllocatedMap
                //统计完后，判断是否大于2，如果大于等于，不分配任务，如果小于2，分配任务
                //分配任务到容器上还是存到detainedList中？取决于调度算法判断的要更换容器的时机

                //已经分配给该VM的任务数量
                int alreadyAllocatedCloudlet = 0;
                if(cloudletAllocatedMap.containsKey(containerVm.getId())){
                    alreadyAllocatedCloudlet = cloudletAllocatedMap.get(containerVm.getId()).size();
                }else{
                    List<Integer> allocatedCloudletIds = new ArrayList<>();
                    cloudletAllocatedMap.put(containerVm.getId(), allocatedCloudletIds);
                    //这里需要查看三处
                    //查看第一处：滞留的任务
                    for(ContainerCloudlet cloudlet1 : getDetainedCloudletList()){
                        if(cloudlet1.getVmId() == containerVm.getId() && !cloudletAllocatedMap.get(containerVm.getId()).contains(cloudlet1.getCloudletId())){
                            alreadyAllocatedCloudlet++;
                            cloudletAllocatedMap.get(containerVm.getId()).add(cloudlet1.getCloudletId());
                        }
                    }
                    //查看第二处：容器上的任务
                    if(containerVm.getContainerList().size() > 0){
                        Container container = containerVm.getContainerList().get(0);
                        ContainerCloudletScheduler containerCloudletScheduler = container.getContainerCloudletScheduler();
                        //如果没有加入过这个id
                        if(containerCloudletScheduler.getCloudletExecList().size() > 0
                                && !cloudletAllocatedMap.get(containerVm.getId()).contains(containerCloudletScheduler.getCloudletExecList().get(0).getCloudletId())){
                            alreadyAllocatedCloudlet++;
                            cloudletAllocatedMap.get(containerVm.getId()).add(containerCloudletScheduler.getCloudletExecList().get(0).getCloudletId());
                        }
//                        if(containerCloudletScheduler.getCloudletWaitingList().size() > 0
//                                && !cloudletAllocatedMap.get(containerVm.getId()).contains(containerCloudletScheduler.getCloudletWaitingList().get(0).getCloudletId())){
//                            alreadyAllocatedCloudlet++;
//                            cloudletAllocatedMap.get(containerVm.getId()).add(containerCloudletScheduler.getCloudletExecList().get(0).getCloudletId());
//                        }
                        //这个for由上面修改而来，区别是不假设只有一个等待的任务
                        for(ResCloudlet resCloudlet : containerCloudletScheduler.getCloudletWaitingList()){
                            if(!cloudletAllocatedMap.get(containerVm.getId()).contains(resCloudlet.getCloudletId())){
                                alreadyAllocatedCloudlet++;
                                cloudletAllocatedMap.get(containerVm.getId()).add(resCloudlet.getCloudletId());
                            }
                        }
                    }
                    //查看第三处：同一时间，刚发送出去的任务
                    if(getLastSentCloudlets().size() > 0){
                        for(ContainerCloudlet cloudlet1 : getLastSentCloudlets()){
                            if(cloudlet1.getVmId() == containerVm.getId() && !cloudletAllocatedMap.get(containerVm.getId()).contains(cloudlet1.getCloudletId())){
                                cloudletAllocatedMap.get(containerVm.getId()).add(cloudlet1.getCloudletId());
                                alreadyAllocatedCloudlet++;
                            }
                        }
                    }
                }
                //统计完成已分配任务数量后
                if(alreadyAllocatedCloudlet < 2){
                    if(fastestIndex < 0){
                        if(containerVm.getContainerList().size() == 0){
                            fastestIndex = j;
                            fastestTime = WFCConstants.CONTAINER_INITIAL_TIME;
                        }else{
                            ContainerCloudletScheduler scheduler = containerVm.getContainerList().get(0).getContainerCloudletScheduler();
                            int totalLength = 0;
                            totalLength += scheduler.getCloudletExecList().size() > 0 ? scheduler.getCloudletExecList().get(0).getRemainingCloudletLength() : 0;
//                            totalLength += scheduler.getCloudletWaitingList().size() > 0 ? scheduler.getCloudletWaitingList().get(0).getRemainingCloudletLength() : 0;
                            //这个for由上面修改而来，区别是不假设只有一个等待的任务
                            for(ResCloudlet resCloudlet : scheduler.getCloudletWaitingList()){
                                totalLength += resCloudlet.getRemainingCloudletLength();
                            }
                            fastestIndex = j;
                            fastestTime = totalLength / containerVm.getMips() + WFCConstants.CONTAINER_INITIAL_TIME;
                        }
                    }else{
                        //比较剩余时间是不是更小
                        if(containerVm.getContainerList().size() == 0){
                            if(WFCConstants.CONTAINER_INITIAL_TIME < fastestTime){
                                fastestIndex = j;
                                fastestTime = WFCConstants.CONTAINER_INITIAL_TIME;
                            }
                        }else{
                            ContainerCloudletScheduler scheduler = containerVm.getContainerList().get(0).getContainerCloudletScheduler();
                            int totalLength = 0;
                            totalLength += scheduler.getCloudletExecList().size() > 0 ? scheduler.getCloudletExecList().get(0).getRemainingCloudletLength() : 0;
//                            totalLength += scheduler.getCloudletWaitingList().size() > 0 ? scheduler.getCloudletWaitingList().get(0).getRemainingCloudletLength() : 0;
                            //这个for由上面修改而来，区别是不假设只有一个等待的任务
                            for(ResCloudlet resCloudlet : scheduler.getCloudletWaitingList()){
                                totalLength += resCloudlet.getRemainingCloudletLength();
                            }
                            if (totalLength / containerVm.getMips() + WFCConstants.CONTAINER_INITIAL_TIME < fastestTime){
                                fastestIndex = j;
                                fastestTime = totalLength / containerVm.getMips() + WFCConstants.CONTAINER_INITIAL_TIME;
                            }
                        }
                    }
                }
//                if(alreadyAllocatedCloudlet < 2){
//                    cloudlet.setVmId(containerVm.getId());
//                    cloudletAllocatedMap.put(containerVm.getId(), alreadyAllocatedCloudlet + 1);
//                    vmFound = true;
//                    break;
//                }
            }
            if(fastestIndex < 0){
                vmFound = false;
            }else{
                vmFound = true;
                ContainerVm containerVm = (ContainerVm) getVmList().get(fastestIndex);
                cloudlet.setVmId(containerVm.getId());
                cloudletAllocatedMap.get(containerVm.getId()).add(cloudlet.getCloudletId());
            }

            //如果没有为该任务找到对应的VM，就返回一个不可能的VMid
            //这样在WFCScheduler的processCloudletUpdate()中调用的时候，就可以发现这个任务没有成功提交
            if(vmFound == true){
                if(WFCConstants.PRINT_SCHEDULE_RESULT){
                    Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":任务" + cloudlet.getCloudletId() + "被分配给了虚拟机" + cloudlet.getVmId());
                }
            }else{
                needNewVmNum += 0.5;
                cloudlet.setVmId(-1000);
//                if(WFCConstants.PRINT_SCHEDULE_RESULT){
//                    Log.printLine(CloudSim.clock() + ":任务" + cloudlet.getCloudletId() + "没有找到合适的虚拟机");
//                }
            }

        }
        cloudletAllocatedMap.clear();


        //以下是根据待运行的任务动态创建VM的功能
        needNewVmNum = Math.ceil(needNewVmNum) - getToCreateVmList().size();
        if(WFCConstants.VM_MINIMUM_NUMBER - getVmList().size() - getToCreateVmList().size() > Math.ceil(needNewVmNum)){
            needNewVmNum = WFCConstants.VM_MINIMUM_NUMBER - getVmList().size() - getToCreateVmList().size();
        }
        if(WFCConstants.ENABLE_DYNAMIC_VM_CREATE && Math.ceil(needNewVmNum) > 0){
            for(int i = 0; i < Math.ceil(needNewVmNum); i++){
                //下面创建一个新的VM
                ArrayList peList = new ArrayList();
                for (int p = 0; p < WFCConstants.WFC_NUMBER_VM_PES ; p++) {
                    peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double)WFCConstants.WFC_VM_MIPS * WFCConstants.WFC_VM_RATIO)));
                }
                ContainerVm aNewVm = new JustToTryPowerContainerVm(ContainerVmType.LARGE, IDs.pollId(ContainerVm.class), brokerId, WFCConstants.WFC_VM_MIPS, (float)WFCConstants.WFC_VM_RAM,
                        WFCConstants.WFC_VM_BW, WFCConstants.WFC_VM_SIZE,  WFCConstants.WFC_VM_VMM,
                        new ContainerSchedulerTimeSharedOverSubscription(peList),
                        //new ContainerSchedulerTimeSharedOverSubscription(peList),
                        new ContainerRamProvisionerSimple(WFCConstants.WFC_VM_RAM),
                        new ContainerBwProvisionerSimple(WFCConstants.WFC_VM_BW), peList,
                        WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                getToCreateVmList().add(aNewVm);
//            Log.printConcatLine(CloudSim.clock(), "试图创建VM", aNewVm.getId());
            }



            //动态销毁VM的起点1
//            if(cloudletFirst.getCloudletId() >= 20 && cloudletFirst.getCloudletId() <= 25
//                    && getVmList().size() > 1 && WFCConstants.ENABLE_DYNAMIC_VM_DESTROY){
//                //把vmList中最后一个VM删除掉
//                ContainerVm anOldVm = (ContainerVm)getVmList().get(getVmList().size() - 1);
//                //既然马上就要删除了，那之后的调度中就不要再用了
//                getVmList().remove(getVmList().size() - 1);
//                getToDestroyVmList().add(anOldVm);
//                Log.printLine(CloudSim.clock() + ": 想要删除VM" + anOldVm.getId());
//            }
        }
    }
}
