package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.core.PowerContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 写这个类只是为了实现调度算法对任务的分配VM和分配Container功能
 * 因为原来的WFCScheduler.processCloudletUpdate()中只是简单地将每个任务对应与其ID相同的Container，并没有真正用到调度算法
 * 这里实现一种最简单的调度算法
 */
public class JustToTryAlgorithm extends BaseSchedulingAlgorithm{

    private List<? extends Container> containerList;

    public <T extends Container> List<T> getContainerList(){
        return (List<T>)containerList;
    }

    public void setContainerList(List<? extends Container> list) {
        this.containerList = list;
    }

    /**
     * 不用这个方法
     * @throws Exception
     */
    @Override
    public void run() throws Exception {

    }

    @Override
    public void run(int brokerId){
//        System.out.println("调用了调度算法的run()，时间是" + CloudSim.clock());
        //获取任务数量
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
//        Log.printLine("知道了有" + numVM + "个虚拟机");
        int numContainer = 0;
        //记录每个容器上已经分配的任务数量
        Map<Integer, Integer> cloudletAllocatedMap = new HashMap<>();
        setContainerList(new ArrayList<Container>());
        //得到所有的container
        for(int i = 0; i < numVM; i++){
            ContainerVm vm = (ContainerVm)getVmList().get(i);
            getContainerList().addAll(vm.getContainerList());
        }
        numContainer = getContainerList().size();
        for(int i = 0; i < numCloudlet; i++){
            boolean containerFound = false;
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);

            for(int j = 10; j < numContainer; j++){
                Container container = getContainerList().get(j);
                ContainerCloudletScheduler containerCloudletScheduler = container.getContainerCloudletScheduler();
                //计算已经分配给该容器的任务数量
                int alreadyAllocatedCloudlet = 0;
                if(cloudletAllocatedMap.containsKey(container.getId())){
                    alreadyAllocatedCloudlet = cloudletAllocatedMap.get(container.getId());
                }else{
                    alreadyAllocatedCloudlet = containerCloudletScheduler.getCloudletWaitingList().size() + containerCloudletScheduler.getCloudletExecList().size();
                }
                //每个容器最多有一个正在运行的任务和一个正在等待的任务
                if(alreadyAllocatedCloudlet < 2){
                    cloudlet.setContainerId(container.getId());
                    cloudletAllocatedMap.put(container.getId(), alreadyAllocatedCloudlet + 1);
                    containerFound = true;
                    break;
                }
            }
            //如果没有为该任务找到对应的容器，就返回一个不可能的容器id
            //这样在WFCScheduler的processCloudletUpdate()中调用的时候，就可以发现这个任务没有成功提交
            if(containerFound == false){
                cloudlet.setContainerId(-1000);
            }
            //这部分可以留在WFCScheduler中进行
//            int vmId = getContainersToVmsMap().get(allocatedContainerId);
        }
        cloudletAllocatedMap.clear();


        if(getCloudletList().size() > 0){
            ContainerCloudlet cloudletFirst = (ContainerCloudlet)getCloudletList().get(0);
            if(cloudletFirst.getCloudletId() >= 7 && cloudletFirst.getCloudletId() <= 14){
                //下面创建一个新的VM
                ArrayList peList = new ArrayList();
                for (int p = 0; p < WFCConstants.WFC_NUMBER_VM_PES ; p++) {
                    peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double)WFCConstants.WFC_VM_MIPS * WFCConstants.WFC_VM_RATIO)));
                }
                ContainerVm aNewVm = new PowerContainerVm(IDs.pollId(ContainerVm.class), brokerId, WFCConstants.WFC_VM_MIPS, (float)WFCConstants.WFC_VM_RAM,
                        WFCConstants.WFC_VM_BW, WFCConstants.WFC_VM_SIZE,  WFCConstants.WFC_VM_VMM,
                        new ContainerSchedulerTimeSharedOverSubscription(peList),
                        //new ContainerSchedulerTimeSharedOverSubscription(peList),
                        new ContainerRamProvisionerSimple(WFCConstants.WFC_VM_RAM),
                        new ContainerBwProvisionerSimple(WFCConstants.WFC_VM_BW), peList,
                        WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                getToCreateVmList().add(aNewVm);
            }

            if(cloudletFirst.getCloudletId() >= 17 && cloudletFirst.getCloudletId() <= 24){
                //下面销毁上面代码创建的VM
                //把vmList中最后一个VM删除掉
                ContainerVm anOldVm = (ContainerVm)getVmList().get(getVmList().size() - 1);
                getToDestroyVmList().add(anOldVm);
            }
        }
    }
}
