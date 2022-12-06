package org.wfc.scheduler;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.lists.ContainerVmList;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.PredicateType;
import org.cloudbus.cloudsim.lists.CloudletList;

import java.text.DecimalFormat;
import java.util.*;

import org.wfc.core.WFCConstants;
import org.wfc.core.WFCDeadline;
import org.workflowsim.*;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.scheduling.*;
import org.workflowsim.utils.Parameters;

/**
 * ////////////////////////////////////////////////////////////////////////
 * 正在写记录上一次update()函数中，发送的任务列表
 * ///////////////////////////////////////////////////////////////////////
 */


/**
 * Created by sareh on 15/07/15.
 */

public class WFCScheduler extends SimEntity {

    private int workflowEngineId;

    /**
     * The vm list.
     */
    protected List<? extends ContainerVm> vmList;

    /**
     * The vms created list.
     */
    protected List<? extends ContainerVm> vmsCreatedList;
    /**
     * The containers created list.
     */
    protected List<? extends Container> containersCreatedList;

    /**
     * The cloudlet list.
     */
    protected List<? extends ContainerCloudlet> cloudletList;
    /**
     * The container list
     */

    protected List<? extends Container> containerList;

    /**
     * The cloudlet submitted list.
     */
    protected List<? extends ContainerCloudlet> cloudletSubmittedList;

    /**
     * The cloudlet received list.
     */
    protected Set<? extends ContainerCloudlet> cloudletReceivedList;

    /**
     * The cloudlets submitted.
     */
    protected int cloudletsSubmitted;

    /**
     * The vms requested.
     */
    protected int vmsRequested;

    /**
     * The vms acks.
     */
    protected int vmsAcks;
    /**
     * The containers acks.
     */
    protected int containersAcks;
    /**
     * The number of created containers
     */

    protected int containersCreated;

    /**
     * The vms destroyed.
     */
    protected int vmsDestroyed;

    /**
     * The datacenter ids list.
     */
    protected List<Integer> datacenterIdsList;

    /**
     * The datacenter requested ids list.
     */
    protected List<Integer> datacenterRequestedIdsList;

    /**
     * The vms to datacenters map.
     */
    protected Map<Integer, Integer> vmsToDatacentersMap;
    /**
     * The containers to datacenters map. 这里修改过，vms -> containers
     */
    protected Map<Integer, Integer> containersToVmsMap;//修改过，原来是containersToDatacentersMap

    /**
     * The datacenter characteristics list.
     */
    protected Map<Integer, ContainerDatacenterCharacteristics> datacenterCharacteristicsList;

    /**
     * The datacenter characteristics list.
     */
    protected double overBookingfactor;

    protected int numberOfCreatedVMs;

    /**
     * 下面的成员变量都是自己添加
     */

    /**
     * 加一个List，任务分配到即将销毁的容器时，需要暂存一下映射关系，等新容器创建后把任务接力过去
     */
    protected List<? extends ContainerCloudlet> detainedCloudlets;

    /**
     * 加一个Map，记录正在创建新容器的VM，其未来的容器的workflowId
     */
    protected Map<Integer, Integer> vmToCreatingContainer;

    /**
     * 即将删除的VM列表
     */
    protected List<? extends ContainerVm> toDestroyVmList;

    /**
     * 储存正在创建的VM的列表
     */
    private List<? extends ContainerVm> toCreateVmList;

    /**
     * 为了加速寻找正在创建的VM，使用Map
     */
    private Map<Integer, ContainerVm> toCreateVmMap;

    /**
     * 储存已经发送给WFCDatacenter的正在创建的VM
     */
    private Map<Integer, ContainerVm> sendCreateVmListToDatacenter;

    //上一次运行processCloudletUpdate()的时间
    protected double lastUpdateOrContainerCreateTime;

    //上一次运行processCloudletUpdate()时向WFCDatacenter发送的任务
    protected List<? extends ContainerCloudlet> lastSentCloudlets;

    protected Set<? extends ContainerCloudlet> hasSentReplicateCloudlets;

    // 储存导致新VM产生的任务的id，对应新VM的id
    protected Map<Integer, ContainerVm> cloudletsLeadToNewVm;

    // 上一次运行processCloudletUpdate()的时间
    protected double lastUpdateTime;

    /**
     * 用来加速vm查询的
     */
    protected Map<Integer, ContainerVm> vmsCreatedMap;

    /**
     * 正在创建的VM的平均“伪空闲时间”，是一个绝对时间点
     */
    protected List<Double> averageDummyIdleTime;

    /**
     * 存储资源调度算法中因其部署新容器的任务
     */
    protected Set<Integer> alreadyNewContainerCloudletId;

    /**
     * 存储资源调度算法中因其租用新VM的任务
     */
    protected Set<Integer> alreadyNewVmCloudletId;



    /**
     * Created a new DatacenterBroker object.
     *
     * @param name name to be associated with this entity (as required by Sim_entity class from
     *             simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WFCScheduler(String name, double overBookingfactor) throws Exception {
        super(name);

        setVmList(new ArrayList<>());
        setContainerList(new ArrayList<>());
        setVmsCreatedList(new ArrayList<>());
        setContainersCreatedList(new ArrayList<>());
        setCloudletList(new ArrayList<>());
        setCloudletSubmittedList(new ArrayList<>());
        setCloudletReceivedList(new HashSet<>());

        // 自己加一些初始化列表的语句
        setDetainedCloudlets(new ArrayList<>());
        setVmToCreatingContainer(new HashMap<>());
        setToDestroyVmList(new ArrayList<>());
        setToCreateVmMap(new HashMap<>());
        setLastSentCloudlets(new ArrayList<>());
        setLastUpdateOrContainerCreateTime(-1000);
        setHasSentReplicateCloudlets(new HashSet<>());
        setVmsCreatedMap(new HashMap<>());
        setSendCreateVmListToDatacenter(new HashMap<>());
        setCloudletsLeadToNewVm(new HashMap<>());
        setLastUpdateTime(-1000);

        cloudletsSubmitted = 0;
        setVmsRequested(WFCConstants.WFC_NUMBER_VMS);
        setVmsAcks(WFCConstants.WFC_NUMBER_VMS);
        setContainersAcks(0);
        setContainersCreated(WFCConstants.WFC_NUMBER_CONTAINER);
        setVmsDestroyed(WFCConstants.WFC_NUMBER_VMS);
        setOverBookingfactor(overBookingfactor);
        setDatacenterIdsList(new LinkedList<>());
        setDatacenterRequestedIdsList(new ArrayList<>());
        setVmsToDatacentersMap(new HashMap<Integer, Integer>());
        setContainersToVmsMap(new HashMap<Integer, Integer>());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());
        setNumberOfCreatedVMs(WFCConstants.WFC_NUMBER_VMS);
        List<Double> aveDummy = new ArrayList<>();
        aveDummy.add(10.0);
        setAverageDummyIdleTime(aveDummy);
        setAlreadyNewContainerCloudletId(new HashSet<>());
        setAlreadyNewVmCloudletId(new HashSet<>());
    }

    public void setAlreadyNewVmCloudletId(Set<Integer> alreadyNewVmCloudletId) {
        this.alreadyNewVmCloudletId = alreadyNewVmCloudletId;
    }

    public Set<Integer> getAlreadyNewVmCloudletId() {
        return alreadyNewVmCloudletId;
    }

    public void setAlreadyNewContainerCloudletId(Set<Integer> alreadyNewContainerCloudletId) {
        this.alreadyNewContainerCloudletId = alreadyNewContainerCloudletId;
    }

    public Set<Integer> getAlreadyNewContainerCloudletId() {
        return alreadyNewContainerCloudletId;
    }

    public void setAverageDummyIdleTime(List<Double> averageDummyIdleTime) {
        this.averageDummyIdleTime = averageDummyIdleTime;
    }

    public List<Double> getAverageDummyIdleTime() {
        return averageDummyIdleTime;
    }

    public void setToCreateVmMap(Map<Integer, ContainerVm> toCreateVmMap) {
        this.toCreateVmMap = toCreateVmMap;
    }

    public Map<Integer, ContainerVm> getToCreateVmMap() {
        return toCreateVmMap;
    }

    public void setLastUpdateTime(double lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public double getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setVmsCreatedMap(Map<Integer, ContainerVm> vmsCreatedMap) {
        this.vmsCreatedMap = vmsCreatedMap;
    }

    public Map<Integer, ContainerVm> getVmsCreatedMap() {
        return vmsCreatedMap;
    }

    public double getLastUpdateOrContainerCreateTime() {
        return lastUpdateOrContainerCreateTime;
    }

    public void setLastUpdateOrContainerCreateTime(double time) {
        lastUpdateOrContainerCreateTime = time;
    }

    public <T extends ContainerCloudlet> List<T> getLastSentCloudlets() {
        return (List<T>) lastSentCloudlets;
    }

    public <T extends ContainerCloudlet> void setLastSentCloudlets(List<T> lastSentCloudlets) {
        this.lastSentCloudlets = lastSentCloudlets;
    }

    public <T extends ContainerCloudlet> void setHasSentReplicateCloudlets(Set<T> hasSentReplicateCloudlets){
        this.hasSentReplicateCloudlets = hasSentReplicateCloudlets;
    }

    public <T extends ContainerCloudlet> Set<T> getHasSentReplicateCloudlets(){
        return (Set<T>) hasSentReplicateCloudlets;
    }

    public void setCloudletsLeadToNewVm(Map<Integer, ContainerVm> cloudletsLeadToNewVm){
        this.cloudletsLeadToNewVm = cloudletsLeadToNewVm;
    }

    public Map<Integer, ContainerVm> getCloudletsLeadToNewVm(){
        return cloudletsLeadToNewVm;
    }

    /**
     * 添加一个函数，用来定期检查一个VM是否在达到整数个租用周期后已空闲，如果空闲则销毁
     */
    public void checkVmIdleByRentInterval(SimEvent ev) {
//        Log.printLine(CloudSim.clock() + ":定期检查VM，删除---------------------------------");
        int vmId = (int) ev.getData();
        //list改map
//        ContainerVm vm = ContainerVmList.getById(getVmsCreatedList(), vmId);
        ContainerVm vm = getVmsCreatedMap().get(vmId);
        if(null == vm){
            return;
        }
        //判断是否发生VM宕机
        if (WFCConstants.ENABLE_VM_SHUTDOWN) {
            Random random = new Random();
            if (random.nextDouble() < WFCConstants.VM_FAILURE_RATE) {
                //下一个租用周期发生宕机
                send(this.getId(), random.nextDouble() * WFCConstants.RENT_INTERVAL, CloudSimTags.VM_SHUTDOWN, vmId);
            }
        }
        if (!WFCConstants.ENABLE_DYNAMIC_VM_DESTROY) {
            return;
        }
        if(getVmsCreatedMap().size() < WFCConstants.VM_MINIMUM_NUMBER){
            send(this.getId(), WFCConstants.RENT_INTERVAL, CloudSimTags.CHECK_IDLE_VM, vmId);
            return;
        }

        for (ContainerCloudlet cloudlet : getDetainedCloudlets()) {
            if (cloudlet.getVmId() == vmId) {
                send(this.getId(), WFCConstants.RENT_INTERVAL, CloudSimTags.CHECK_IDLE_VM, vmId);
                return;
            }
        }
        // 如果VM的容器上还有任务，则不会销毁
        if (vm.getContainerList().size() > 0) {
            for (Container container : vm.getContainerList()) {
                ContainerCloudletScheduler containerCloudletScheduler = container.getContainerCloudletScheduler();
                if (containerCloudletScheduler.getCloudletExecList().size() + containerCloudletScheduler.getCloudletWaitingList().size() > 0) {
                    send(this.getId(), WFCConstants.RENT_INTERVAL, CloudSimTags.CHECK_IDLE_VM, vmId);
                    return;
                }
            }
        }
        //第3个要检查的：如果同一时间恰好processCloudletUpdate()被调用了，就需要检查发送到WFCDatacenter的任务
        if (getLastUpdateOrContainerCreateTime() == CloudSim.clock()) {
            for (ContainerCloudlet cloudlet : getLastSentCloudlets()) {
                if (cloudlet.getVmId() == vmId) {
                    send(this.getId(), WFCConstants.RENT_INTERVAL, CloudSimTags.CHECK_IDLE_VM, vmId);
                    return;
                }
            }
        }
        //4.检查该VM上是否有容器正在创建
        if (vmToCreatingContainer.containsKey(vmId)) {
            send(this.getId(), WFCConstants.RENT_INTERVAL, CloudSimTags.CHECK_IDLE_VM, vmId);
            return;
        }
        //经过上面4个检查，如果都没有，就可以删除VM
        //如果只用这个函数删除VM，getToDestroyVmList()方法应该就没用了
//        Log.printLine(CloudSim.clock() + ":通过定期检查，发现VM" + vm.getId() + "需要删除，当前时间减去创建时间的结果是：" + (CloudSim.clock() - vm.getCreatingTime()));
        List<ContainerVm> toImmediatelyDestroyVmList = new ArrayList<>();
        toImmediatelyDestroyVmList.add(vm);
        //防止processCloudletUpdate()之后的同一时间，分配了正准备删除的VM
        getToDestroyVmList().add(vm);
        destroyVmsInDatacenter(toImmediatelyDestroyVmList);
    }


    /**
     * This method is used to send to the broker the list with virtual machines that must be
     * created.
     *
     * @param list the list
     * @pre list !=null
     * @post $none
     */
    public void submitVmList(List<? extends ContainerVm> list) {
        getVmList().addAll(list);
    }

    /**
     * This method is used to send to the broker the list of cloudlets.
     *
     * @param list the list
     * @pre list !=null
     * @post $none
     */
    public void submitCloudletList(List<? extends ContainerCloudlet> list) {
        getCloudletList().addAll(list);
    }

    /**
     * Specifies that a given cloudlet must run in a specific virtual machine.
     *
     * @param cloudletId ID of the cloudlet being bount to a vm
     * @param vmId       the vm id
     * @pre cloudletId > 0
     * @pre id > 0
     * @post $none
     */
    public void bindCloudletToVm(int cloudletId, int vmId) {
        CloudletList.getById(getCloudletList(), cloudletId).setVmId(vmId);
//        Log.printConcatLine("The Vm ID is ",  CloudletList.getById(getCloudletList(), cloudletId).getVmId(), "should be", vmId);
    }

    /**
     * Specifies that a given cloudlet must run in a specific virtual machine.
     *
     * @param cloudletId  ID of the cloudlet being bount to a vm
     * @param containerId the vm id
     * @pre cloudletId > 0
     * @pre id > 0
     * @post $none
     */
    public void bindCloudletToContainer(int cloudletId, int containerId) {
        CloudletList.getById(getCloudletList(), cloudletId).setContainerId(containerId);
    }
    /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */


    /**
     * Process an event
     *
     * @param ev a simEvent obj
     */
    @Override
    public void processEvent(SimEvent ev) {

        if (WFCConstants.CAN_PRINT_SEQ_LOG)
            Log.printLine("ContainerDataCenterBroker=WFScheduler=>ProccessEvent()=>ev.getTag():" + ev.getTag());

        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            // Resource characteristics answer
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                processResourceCharacteristics(ev);
                break;
            // VM Creation answer
            case CloudSimTags.VM_CREATE_ACK:
                processVmCreate(ev);
                break;
            case CloudSimTags.VM_DESTROY_ACK:
                processVmDestroy(ev);
                break;
            // New VM Creation answer
            case containerCloudSimTags.VM_NEW_CREATE:
                processNewVmCreate(ev);
                break;
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev);
                break;
            case WorkflowSimTags.CLOUDLET_CHECK:
                processCloudletReturn(ev);
                break;
            case WorkflowSimTags.CLOUDLET_UPDATE:
                if (!WFCDeadline.endSimulation) {
                    processCloudletUpdate(ev);
                }
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processCloudletReturn(ev);
                break;
            case containerCloudSimTags.CONTAINER_CREATE_ACK:
                processContainerCreate(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            //添加一个事项，用来定期检查VM是否空闲，是否需要删除
            case CloudSimTags.CHECK_IDLE_VM:
                if (!WFCDeadline.endSimulation) {
                    checkVmIdleByRentInterval(ev);
                }
                break;
            case CloudSimTags.VM_SHUTDOWN:
                if (!WFCDeadline.endSimulation) {
                    processVmShutDownBefore(ev);
                }
                break;
            case CloudSimTags.VM_SHUTDOWN_ACK:
                processVmShutDownAfter(ev);
                break;
            // other unknown tags are processed by this method
            default:
                processOtherEvent(ev);
                break;
        }
    }

    private void processVmShutDownBefore(SimEvent ev) {
        int vmId = (int) ev.getData();
        if(getVmsCreatedMap().size() == 0 || WFCDeadline.endSimulation){
            return;
        }
        if(WFCConstants.PRINT_VM_SHUTDOWN){
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":processVmShutDownBefore(): 虚拟机" + vmId + "宕机");
        }

        //这里的可以合并到processVmShutDownAfter()中处理
//        if (getLastUpdateOrContainerCreateTime() == CloudSim.clock()) {
//            //如果同一时间，之前发送过任务，就要检查一下是否发送到了宕机的VM
//            for (ContainerCloudlet cloudlet : getLastSentCloudlets()) {
//                if (cloudlet.getVmId() == vmId) {
//                    //下面的processVmShutDownAfter()就会处理这里添加的任务
//                    getDetainedCloudlets().add(cloudlet);
//                }
//            }
//        }
        //还要避免同一时间，之后的组件以为VM还能用，分配了任务，或者创建了容器
        //list改map
//        ContainerVm vm = ContainerVmList.getById(getVmsCreatedList(), vmId);
        ContainerVm vm = getVmsCreatedMap().get(vmId);
        if(null == vm){
//            Log.printLine(CloudSim.clock() + ":processVmShutDownBefore(): 即将宕机的虚拟机并不存在");
            return;
        }
        Log.printConcatLine(CloudSim.clock(), ":还剩下 ", getVmsCreatedList().size() - 1, " 个虚拟机");
        getToDestroyVmList().add(vm);
        sendNow(getDatacenterIdsList().get(0), CloudSimTags.VM_SHUTDOWN, vmId);
    }

    protected void processVmShutDownAfter(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int vmId = data[1];
        int tags = data[2];
        //一个VM宕机后，应该处理的：
        //vm上还有的任务，设定为失败，重新执行
        //detainedList上面的该VM的任务，没有失败，重新分配
        //同一时间，刚调用processCloudletUpdate()发送到WFCDatacenter的任务，没有失败，重新分配（processVmShutDownBefore()解决）
        if(tags == CloudSimTags.TRUE){
//            ContainerVm vm = ContainerVmList.getById(getVmList(), vmId);
            ContainerVm vm = getVmsCreatedMap().get(vmId);
            if(!getToDestroyVmList().contains(vm)){
                return;
            }
            getToDestroyVmList().remove(vm);
            getVmsCreatedList().remove(vm);
            getVmsCreatedMap().remove(vmId);

            //同一时间之前发送出去了的任务算是失败

            //延迟的，重新调度，不算失败
            List<ContainerCloudlet> toRemoveFormDetained = new ArrayList<>();
            for(ContainerCloudlet cloudlet : getDetainedCloudlets()){
                if(cloudlet.getVmId() == vmId){
                    cloudlet.setContainerId(-1000);
                    getCloudletSubmittedList().remove(cloudlet);
                    toRemoveFormDetained.add(cloudlet);
                    //防止和上面加了同一个任务
                    if(!getCloudletList().contains(cloudlet)){
                        getCloudletList().add(cloudlet);
                    }
                }
                //10.20:下面的代码注释掉，重写为上面的
//                if (cloudlet.getVmId() == vmId && !getCloudletList().contains(cloudlet)) {
//                    cloudlet.setContainerId(-1000);
//                    if(!getCloudletList().contains(cloudlet)){
//                        getCloudletList().add(cloudlet);
//                        toRemoveFormDetained.add(cloudlet);
//                    }
//                    getCloudletSubmittedList().remove(cloudlet);
//                }
            }
            getDetainedCloudlets().removeAll(toRemoveFormDetained);
            sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);

            //vm上如果还有任务，设定为失败，重新执行
            if(vm.getContainerList().size() > 0){
                ContainerCloudletScheduler cloudletScheduler = vm.getContainerList().get(0).getContainerCloudletScheduler();
                List<ContainerCloudlet> toSendEngineCloudlets = new ArrayList<>();
                if (cloudletScheduler.getCloudletExecList().size() > 0) {
                    ContainerCloudlet cloudlet = (ContainerCloudlet) cloudletScheduler.getCloudletExecList().get(0).getCloudlet();
                    //有可能这个任务在上面两种情况中已经出现过了，同时又再这里出现
                    //如果任务是当前同一时间到达VM上的容器上
                    if(!getCloudletList().contains(cloudlet) && !toSendEngineCloudlets.contains(cloudlet)){
                        toSendEngineCloudlets.add(cloudlet);
                        WFCDeadline.withinContainerFailJobNum++;
                    }
                }
                if (cloudletScheduler.getCloudletWaitingList().size() > 0) {
//                    ContainerCloudlet cloudlet = (ContainerCloudlet) cloudletScheduler.getCloudletWaitingList().get(0).getCloudlet();
                    for(ResCloudlet resCloudlet : cloudletScheduler.getCloudletWaitingList()){
                        ContainerCloudlet cloudlet = (ContainerCloudlet)resCloudlet.getCloudlet();
                        //有可能这个任务在上面几种情况中已经出现过了，同时又再这里出现
                        //如果任务是当前同一时间到达VM上的容器上
                        if(!getCloudletList().contains(cloudlet) && !toSendEngineCloudlets.contains(cloudlet)){
                            toSendEngineCloudlets.add(cloudlet);
                            WFCDeadline.withinContainerFailJobNum++;
                        }
                    }
                }
                for(ContainerCloudlet cloudlet : toSendEngineCloudlets){
                    try {
                        cloudlet.setCloudletStatus(Cloudlet.FAILED);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    getCloudletReceivedSet().add(cloudlet);
                    getCloudletSubmittedList().remove(cloudlet);
                    getCloudletsLeadToNewVm().remove(cloudlet.getCloudletId());
                    schedule(this.workflowEngineId, 0, CloudSimTags.CLOUDLET_RETURN, cloudlet);
                    cloudletsSubmitted--;
                    Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": ", getName().substring(27), ": Cloudlet ", cloudlet.getCloudletId(), " returned, ", getCloudletReceivedSet().size(), " / ", WFCDeadline.numOfAllCloudlets);
//                    Log.printConcatLine(CloudSim.clock(), "WFCScheduler向WFCEngine发送了任务" + cloudlet.getCloudletId());
                }
            }
        }
    }


    /**
     * 问题 1：这个函数上可能出现一个问题，如果之前WFCScheduler连续两次调用了create Container方法，这个函数会连续两次收到容器创建的确认
     * 如果连续两次容器创建都是在同一个VM上，会造成第一个容器被销毁，第一次发来的容器确认并不代表容器是被成功创建了
     * <p>
     * 问题 2：这个函数调用的时候，有可能容器所在的虚拟机已经销毁了
     * <p>
     * 问题 3：可能此时容器所在的VM已经宕机
     *
     * @param ev
     */
    public void processContainerCreate(SimEvent ev) {
//        Log.printLine(CloudSim.clock() + "调用了容器创建的确认+++++++++++++++++++++");
        if (getLastUpdateOrContainerCreateTime() != CloudSim.clock()) {
            getLastSentCloudlets().clear();//考虑到调用check之前，在同一时间多次调用processCloudletUpdate()
            setLastUpdateOrContainerCreateTime(CloudSim.clock());
        }
        int[] data = (int[]) ev.getData();
        int vmId = data[0];
        //这个containerId不能用，有可能是错的，用下面的container
        int containerId = data[1];
        int result = data[2];

        if (result == CloudSimTags.TRUE) {
            if (vmId == -1) {
                Log.printConcatLine("Error : Where is the VM");
            } else {
                ContainerVm targetVm = getVmsCreatedMap().get(vmId);
                if (null == targetVm) {
//                    Log.printLine("收到了容器创建的确认，却找不到其所在的VM");
                    return;
                }
                //记录下这个VM创建容器的过程已经完成了
                vmToCreatingContainer.remove(targetVm.getId());
                //把滞留的任务拿出来，分配给容器
                if (getDetainedCloudlets().size() > 0) {
                    List<ContainerCloudlet> toRemoveDetained = new ArrayList<>();
                    int cloudletAllocatedNum = 0;//记录已经分配给该容器的任务数量
                    // 获取这个新创建的容器所在的VM的容器，这样应该可以解决连续两次在同一VM上创建容器的问题
                    Container container = targetVm.getContainerList().get(0);
//                    Container container = ContainerList.getById(getContainerList(), containerId);
//                    ContainerCloudletScheduler containerCloudletScheduler = container.getContainerCloudletScheduler();
                    for (ContainerCloudlet cloudlet : getDetainedCloudlets()) {
                        //如果区分容器类型的开关打开，就需要多一个判断条件，才允许任务分配到VM上
                        // 把分配数量的限制取消掉了，因为分配数量的上限应该由调度算法保证，而不是这里做限制
                        if (cloudlet.getVmId() == vmId
                                && (!WFCConstants.ENABLE_SPECIFY_WORKFLOW_ID || cloudlet.getWorkflowId() == container.getWorkflowId())) {
                            cloudlet.setContainerId(container.getId());
                            toRemoveDetained.add(cloudlet);
                            cloudletAllocatedNum++;
                            double delay = 0.0;
                            if (Parameters.getOverheadParams().getQueueDelay() != null) {
                                delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
                            }
                            //这里的处理比较简单，因为其它相关的处理在processCloudletUpdate()中已经做过了
                            schedule(getVmsToDatacentersMap().get(vmId), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                            WFCDeadline.wfcSchedulerSendJobNum++;
                            getLastSentCloudlets().add(cloudlet);
                        }
                    }
                    getDetainedCloudlets().removeAll(toRemoveDetained);
                }
                getContainersToVmsMap().put(containerId, vmId);
                // 似乎没有用，所以注释掉
//                getContainersCreatedList().add(ContainerList.getById(getContainerList(), containerId));

                //ContainerVm p = ContainerVmList.getById(getVmsCreatedList(), vmId);


                int hostId = targetVm.getHost().getId();
                if (WFCConstants.PRINT_CONTAINER_CREATE) {
                    Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The Container #", containerId,
                            ", is created on Vm #", vmId
                            , ", On Host#", hostId);
                }
                setContainersCreated(getContainersCreated() + 1);
            }
        } else {
            //Container container = ContainerList.getById(getContainerList(), containerId);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Failed Creation of Container #", containerId);
        }

        incrementContainersAcks();
        //如果创建完成所有的容器，就提交任务
        //这里的条件需要修改一下，否则在动态创建容器的过程中，会重复提及任务
//        if (getContainersAcks() == getContainerList().size()) {
//            //Log.print(getContainersCreatedList().size() + "vs asli"+getContainerList().size());
//            submitCloudlets();
//            getContainerList().clear();
//        }

        if (getContainersAcks() == WFCConstants.WFC_NUMBER_CONTAINER) {
            submitCloudlets();
            getContainerList().clear();
        }
    }

    /**
     * Process the return of a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processResourceCharacteristics(SimEvent ev) {
        ContainerDatacenterCharacteristics characteristics = (ContainerDatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) {
            getDatacenterCharacteristicsList().clear();
            setDatacenterRequestedIdsList(new ArrayList<Integer>());
            //为什么不在其它的Datacenter中创建虚拟机？
            createVmsInDatacenter(getDatacenterIdsList().get(0));
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        
        /*setDatacenterIdsList(CloudSim.getCloudResourceList());
        setDatacenterCharacteristicsList(new HashMap<Integer, ContainerDatacenterCharacteristics>());

        //Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Cloud Resource List received with ",
//                getDatacenterIdsList().size(), " resource(s)");

        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
        
        */

        setDatacenterCharacteristicsList(new HashMap<>());
        Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloud Resource List received with "
                + getDatacenterIdsList().size() + " resource(s)");
        for (Integer datacenterId : getDatacenterIdsList()) {
            sendNow(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
        }
    }

    //这个我没有用到
    protected void processNewVmCreate(SimEvent ev) {
        Map<String, Object> map = (Map<String, Object>) ev.getData();
        int datacenterId = (int) map.get("datacenterID");
        int result = (int) map.get("result");
        ContainerVm containerVm = (ContainerVm) map.get("vm");
        int vmId = containerVm.getId();
        if (result == CloudSimTags.TRUE) {
            getVmList().add(containerVm);
            getVmsToDatacentersMap().put(vmId, datacenterId);
            getVmsCreatedList().add(containerVm);
            getVmsCreatedMap().put(vmId, containerVm);
//            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
//                    " has been created in Datacenter #", datacenterId, ", Host #",
//                    ContainerVmList.getById(getVmsCreatedList(), vmId).getHost().getId());
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been created in Datacenter #", datacenterId, ", Host #",
                    getVmsCreatedMap().get(vmId).getHost().getId());

        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }
    }

    /**
     * Process the ack received due to a request for VM creation.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    protected void processVmCreate(SimEvent ev) {
//        Log.printLine("called VmCreate in WFCScheduler");
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];
//        if(vmId > WFCConstants.WFC_NUMBER_VMS){
//            Log.printLine(CloudSim.clock() + "WFCScheduler收到了WFCDatacenter返回的创建VM的反馈");
//        }

//        List<ContainerVm> testList = getVmList();
//        if(vmId == 2){
//            ContainerVm testVm = (ContainerVm)getVmList().get(1);
//        }


        if (result == CloudSimTags.TRUE) {
            getVmsToDatacentersMap().put(vmId, datacenterId);
            sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
            if (WFCConstants.ENABLE_DYNAMIC_VM_DESTROY) {
                // 第一次安排检查，应该考虑到此时VM的初始化时间已经算在了租用周期里，所以要减去
                send(this.getId(), WFCConstants.RENT_INTERVAL - WFCConstants.VM_INITIAL_TIME, CloudSimTags.CHECK_IDLE_VM, vmId);
                // 判断是否发生VM宕机
                if(WFCConstants.ENABLE_VM_SHUTDOWN){
                    Random random = new Random();
                    if(random.nextDouble() < WFCConstants.VM_FAILURE_RATE){
                        //下一个租用周期发生宕机
                        send(this.getId(), random.nextDouble() *
                                (WFCConstants.RENT_INTERVAL - WFCConstants.VM_INITIAL_TIME), CloudSimTags.VM_SHUTDOWN, vmId);
                    }
                }
            }

            ContainerVm vm = getToCreateVmMap().get(vmId);

            //有可能是第一批创建，所以getToCreateVmMap()中找不到
            if(null == vm){
                vm = ContainerVmList.getById(getVmList(), vmId);
            }

//            if(null == vm){
//                Log.printConcatLine(CloudSim.clock(), ":确认创建的虚拟机并不存在");
//                return;
//            }
            getVmsCreatedList().add(vm);
            getVmsCreatedMap().put(vmId, vm);
            getToCreateVmMap().remove(vmId);
            getSendCreateVmListToDatacenter().remove(vm.getId());

//            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
//                    " has been created in Datacenter #", datacenterId, ", Host #",
//                    getVmsCreatedMap().get(vmId).getHost().getId());
            setNumberOfCreatedVMs(getNumberOfCreatedVMs() + 1);
            if(WFCConstants.PRINT_VM_NUM){
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ":现在有 ", getVmsCreatedMap().size(), "个VM");
            }
        } else {
            Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": ", getName(), ": Creation of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }

        incrementVmsAcks();
//        if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
//        If we have tried creating all of the vms in the data center, we submit the containers.
        //因为在调度过程中动态创建VM也会增加getVmList的大小，会导致每次下述条件都满足，重复提交容器
        //所以将条件修改
//        if(getVmList().size() == vmsAcks){
//            submitContainers();
//        }
        if (WFCConstants.WFC_NUMBER_VMS == vmsAcks) {
            submitContainers();
        }
    }

    protected void processVmDestroy(SimEvent ev) {
        int[] data = (int[]) ev.getData();
        int datacenterId = data[0];
        int vmId = data[1];
        int result = data[2];
//        ContainerVm testVm = (ContainerVm)getVmList().get(1);
        if (CloudSimTags.TRUE == result) {
            //vmsCreatedList更新之后，调度算法才能知道某个VM已经销毁了
            //这里不能再读取这个vm的host，因为host上已经释放了这个vm，vm上也没有host的信息了
//            ContainerVm vm = ContainerVmList.getById(getVmList(), vmId);
            ContainerVm vm = getVmsCreatedMap().get(vmId);
            getVmsCreatedList().remove(vm);
            getVmsCreatedMap().remove(vmId);
            getToDestroyVmList().remove(vm);
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": VM #", vmId,
                    " has been destroyed in Datacenter #", datacenterId);
//            Log.printLine("+++++++++++++++++++++++++++++++++++++++++");
//            Log.printLine("+++++++++++++++++++++++++++++++++++++++++");
//            Log.printLine("+++++++++++++++++++++++++++++++++++++++++");
//            Log.printLine("+++++++++++++++++++++++++++++++++++++++++");
        } else {
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": Destruction of VM #", vmId,
                    " failed in Datacenter #", datacenterId);
        }
    }


    protected void submitContainers() {
        //第一批容器创建的时候，并没有指定对应的VM是哪个
//        for (Container container : getContainerList()) {
//            vmToCreatingContainer.put(container.getVm().getId(), container.getWorkflowId());
//        }
        sendNow(getDatacenterIdsList().get(0), containerCloudSimTags.CONTAINER_SUBMIT, getContainerList());
    }

    /**
     * 重载 submitContainers()，用来动态创建container
     *
     * @param list
     * @param <T>
     */
    protected <T extends Container> void submitContianers(List<T> list) {
        for (Container container : list) {
            vmToCreatingContainer.put(container.getVm().getId(), container.getWorkflowId());
        }
        send(getDatacenterIdsList().get(0), WFCConstants.CONTAINER_INITIAL_TIME, containerCloudSimTags.CONTAINER_SUBMIT, list);
    }


    /**
     * Process a cloudlet return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    /**
     * 这里可能会出现日志显示里的问题：如果WFCDatacenter在任务完成的同时有VM创建完成，会先返回VM创建的确认，再返回任务完成的确认
     * 日志中看起来像是任务还没确认完成，就分配了新任务，其实是正常的
     *
     * @param ev
     */
    protected void processCloudletReturn(SimEvent ev) {
        ContainerCloudlet cloudlet = (ContainerCloudlet) ev.getData();
        Job job = (Job) cloudlet;
        FailureGenerator.generate(job);

        getCloudletReceivedSet().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);//
        getCloudletsLeadToNewVm().remove(cloudlet.getCloudletId());
        getAlreadyNewContainerCloudletId().remove(cloudlet.getCloudletId());

        if (WFCConstants.PRINT_CLOUDLET_RETURN) {
            Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": ", getName().substring(27), ": Cloudlet ", cloudlet.getCloudletId(), " (", cloudlet.getWorkflowId(), ") ", "returned, ", cloudlet.getDeadline(), ", ", getCloudletReceivedSet().size(), " / ", WFCDeadline.numOfAllCloudlets);
        }
//        Log.printLine("这个任务属于工作流 " + cloudlet.getWorkflowId());
//        Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": The number of finished Cloudlets is:", getCloudletReceivedList().size());

        
        /*if (getCloudletList().size() == 0 && cloudletsSubmitted == 0) { // all cloudlets executed
            Log.printConcatLine(CloudSim.clock(), ": ", getName(), ": All Cloudlets executed. Finishing...");
            clearDatacenters();
            finishExecution();
        } else { // some cloudlets haven't finished yet
            if (getCloudletList().size() > 0 && cloudletsSubmitted == 0) {
                // all the cloudlets sent finished. It means that some bount
                // cloudlet is waiting its VM be created
                clearDatacenters();
                createVmsInDatacenter(0);
            }

        } */
        //*from wfScheduler


        //为什么有一个任务完成后，就要把对应的vm设置成空闲？
//        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if (Parameters.getOverheadParams().getPostDelay() != null) {
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
//        schedule(this.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);
        schedule(this.workflowEngineId, 0.0, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned
        //相当于有任务完成后，不等新的准备好的任务添加进来，就马上再去调度已有的准备好的任务
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);
//        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": 延迟队列中还有", getDetainedCloudlets().size(), "个任务");
    }

    /**
     * Overrides this method when making a new and different type of Broker. This method is called
     * by  for incoming unknown tags.
     *
     * @param ev a SimEvent object
     * @pre ev != null
     * @post $none
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printConcatLine(getName(), ".processOtherEvent(): ", "Error - an event is null.");
            return;
        }

        Log.printConcatLine(getName(), ".processOtherEvent(): Error - event unknown by this DatacenterBroker.");
    }

    /**
     * Create the virtual machines in a datacenter.
     * 创建第一批VM
     *
     * @param datacenterId Id of the chosen PowerDatacenter
     * @pre $none
     * @post $none
     */
    protected void createVmsInDatacenter(int datacenterId) {
        // send as much vms as possible for this datacenter before trying the next one
        int requestedVms = 0;
        String datacenterName = CloudSim.getEntityName(datacenterId);
        for (ContainerVm vm : getVmList()) {
            if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
//                Log.printLine(String.format("%s: %s: Trying to Create VM #%d in %s", CloudSim.clock(), getName(), vm.getId(), datacenterName));
                sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
                requestedVms++;
            }
        }

        getDatacenterRequestedIdsList().add(datacenterId);

        setVmsRequested(requestedVms);
        setVmsAcks(0);
    }

    /**
     * 添加一个重载函数，用来在调度过程中动态地创建新VM
     *
     * @param datacenterId
     * @param list
     */
    protected void createVmsInDatacenter(int datacenterId, List<? extends ContainerVm> list) {
//        Log.printLine("Scheduler向WFCDatacenter发出了创建VM的命令");
        //requestedVms是做什么用的？看了一下使用的函数，没发现什么作用
        int requestedVms = 0;
        for (ContainerVm vm : list) {
            if (!getVmsToDatacentersMap().containsKey(vm.getId())) {
                //这里应该延迟一会发，而不是立即发
//                sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);
                send(datacenterId, WFCConstants.VM_INITIAL_TIME, CloudSimTags.VM_CREATE_ACK, vm);
                requestedVms++;
            }
        }
        //下面这个一般用不到
        if (!getDatacenterRequestedIdsList().contains(datacenterId)) {
            getDatacenterRequestedIdsList().add(datacenterId);
        }

        setVmsRequested(getVmsRequested() + requestedVms);
    }

    protected void destroyVmsInDatacenter(List<? extends ContainerVm> list) {

//        Log.printLine("++++++++++++++++++++++++++++++++++++++++++");
//        Log.printLine("++++++++++++++++++++++++++++++++++++++++++");
//        Log.printLine("++++++++++++++++++++++++++++++++++++++++++");
//        Log.printLine("++++++++++++++++++++++++++++++++++++++++++");
        for (ContainerVm vm : list) {
//            Log.printLine(CloudSim.clock() + ": 真正删除VM" + vm.getId());
            if (getVmsToDatacentersMap().containsKey(vm.getId())) {
                //销毁是不需要延迟的
                sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY_ACK, vm);
            } else {
                Log.printLine("要求销毁的VM并不存在");
            }
        }
    }


    /**
     * getOverBookingfactor
     * Destroy the virtual machines running in datacenters.
     *
     * @pre $none
     * @post $none
     */
    protected void clearDatacenters() {
        for (ContainerVm vm : getVmsCreatedList()) {
//            Log.printConcatLine(CloudSim.clock(), ": " + getName(), ": Destroying VM #", vm.getId());
            sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
        }

        getVmsCreatedList().clear();
        getVmsCreatedMap().clear();
    }


    /**
     *
     */


    /**
     * Send an internal event communicating the end of the simulation.
     *
     * @pre $none
     * @post $none
     */
    protected void finishExecution() {
        sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        clearDatacenters();//added
        Log.printConcatLine(getName(), " is sssssssshutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        //schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
        int gisID = -1;
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }


    /**
     * 获取滞留的任务
     *
     * @param <T>
     * @return
     */
    public <T extends ContainerCloudlet> List<T> getDetainedCloudlets() {
        return (List<T>) detainedCloudlets;
    }

    /**
     * 设置滞留的任务列表
     *
     * @param detainedCloudlets
     * @param <T>
     */
    public <T extends ContainerCloudlet> void setDetainedCloudlets(List<T> detainedCloudlets) {
        this.detainedCloudlets = detainedCloudlets;
    }

    /**
     * 设置正在创建容器与VM的映射关系
     *
     * @param map
     */
    public void setVmToCreatingContainer(Map<Integer, Integer> map) {
        this.vmToCreatingContainer = map;
    }

    public Map<Integer, Integer> getVmToCreatingContainer() {
        return vmToCreatingContainer;
    }

    /**
     * 添加设定toDestroyVmList的函数
     *
     * @param toDestroyVmList
     * @param <T>
     */
    public <T extends ContainerVm> void setToDestroyVmList(List<T> toDestroyVmList) {
        this.toDestroyVmList = toDestroyVmList;
    }

    public <T extends ContainerVm> List<T> getToDestroyVmList() {
        return (List<T>) toDestroyVmList;
    }





    public void setSendCreateVmListToDatacenter(Map<Integer, ContainerVm> sendCreateVmListToDatacenter) {
        this.sendCreateVmListToDatacenter = sendCreateVmListToDatacenter;
    }

    public Map<Integer, ContainerVm> getSendCreateVmListToDatacenter() {
        return sendCreateVmListToDatacenter;
    }

    /**
     * Gets the vm list.
     *
     * @param <T> the generic type
     * @return the vm list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getVmList() {
        return (List<T>) vmList;
    }

    /**
     * Sets the vm list.
     *
     * @param <T>    the generic type
     * @param vmList the new vm list
     */
    protected <T extends ContainerVm> void setVmList(List<T> vmList) {
        this.vmList = vmList;
    }

    /**
     * Gets the cloudlet list.
     *
     * @param <T> the generic type
     * @return the cloudlet list
     */
    //@SuppressWarnings("unchecked")
    // getCloudletList()得到的是是未调度的任务的列表
    public <T extends ContainerCloudlet> List<T> getCloudletList() {
        return (List<T>) cloudletList;
    }

    /**
     * Sets the cloudlet list.
     *
     * @param <T>          the generic type
     * @param cloudletList the new cloudlet list
     */
    protected <T extends ContainerCloudlet> void setCloudletList(List<T> cloudletList) {
        this.cloudletList = cloudletList;
    }

    /**
     * Gets the cloudlet submitted list.
     *
     * @param <T> the generic type
     * @return the cloudlet submitted list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getCloudletSubmittedList() {
        return (List<T>) cloudletSubmittedList;
    }

    /**
     * Sets the cloudlet submitted list.
     *
     * @param <T>                   the generic type
     * @param cloudletSubmittedList the new cloudlet submitted list
     */
    protected <T extends ContainerCloudlet> void setCloudletSubmittedList(List<T> cloudletSubmittedList) {
        this.cloudletSubmittedList = cloudletSubmittedList;
    }

    /**
     * Gets the cloudlet received list.
     *
     * @param <T> the generic type
     * @return the cloudlet received list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> Set<T> getCloudletReceivedSet() {
        return (Set<T>) cloudletReceivedList;
    }

    /**
     * Sets the cloudlet received list.
     *
     * @param <T>                  the generic type
     * @param cloudletReceivedList the new cloudlet received list
     */
    protected <T extends ContainerCloudlet> void setCloudletReceivedList(Set<T> cloudletReceivedList) {
        this.cloudletReceivedList = cloudletReceivedList;
    }

    /**
     * Gets the vm list.
     *
     * @param <T> the generic type
     * @return the vm list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getVmsCreatedList() {
        return (List<T>) vmsCreatedList;
    }

    /**
     * Sets the vm list.
     *
     * @param <T>            the generic type
     * @param vmsCreatedList the vms created list
     */
    protected <T extends ContainerVm> void setVmsCreatedList(List<T> vmsCreatedList) {
        this.vmsCreatedList = vmsCreatedList;
    }


    /**
     * Gets the vms requested.
     *
     * @return the vms requested
     */
    protected int getVmsRequested() {
        return vmsRequested;
    }

    /**
     * Sets the vms requested.
     *
     * @param vmsRequested the new vms requested
     */
    protected void setVmsRequested(int vmsRequested) {
        this.vmsRequested = vmsRequested;
    }

    /**
     * Gets the vms acks.
     *
     * @return the vms acks
     */
    protected int getVmsAcks() {
        return vmsAcks;
    }

    /**
     * Sets the vms acks.
     *
     * @param vmsAcks the new vms acks
     */
    protected void setVmsAcks(int vmsAcks) {
        this.vmsAcks = vmsAcks;
    }

    /**
     * Increment vms acks.
     */
    protected void incrementVmsAcks() {
        vmsAcks++;
    }

    /**
     * Increment vms acks.
     */
    protected void incrementContainersAcks() {
        setContainersAcks(getContainersAcks() + 1);
    }

    /**
     * Gets the vms destroyed.
     *
     * @return the vms destroyed
     */
    protected int getVmsDestroyed() {
        return vmsDestroyed;
    }

    /**
     * Sets the vms destroyed.
     *
     * @param vmsDestroyed the new vms destroyed
     */
    protected void setVmsDestroyed(int vmsDestroyed) {
        this.vmsDestroyed = vmsDestroyed;
    }

    /**
     * Gets the datacenter ids list.
     *
     * @return the datacenter ids list
     */
    protected List<Integer> getDatacenterIdsList() {
        return datacenterIdsList;
    }

    /**
     * Sets the datacenter ids list.
     *
     * @param datacenterIdsList the new datacenter ids list
     */
    protected void setDatacenterIdsList(List<Integer> datacenterIdsList) {
        this.datacenterIdsList = datacenterIdsList;
    }

    /**
     * Gets the vms to datacenters map.
     *
     * @return the vms to datacenters map
     */
    protected Map<Integer, Integer> getVmsToDatacentersMap() {
        return vmsToDatacentersMap;
    }

    /**
     * Sets the vms to datacenters map.
     *
     * @param vmsToDatacentersMap the vms to datacenters map
     */
    protected void setVmsToDatacentersMap(Map<Integer, Integer> vmsToDatacentersMap) {
        this.vmsToDatacentersMap = vmsToDatacentersMap;
    }

    /**
     * Gets the datacenter characteristics list.
     *
     * @return the datacenter characteristics list
     */
    protected Map<Integer, ContainerDatacenterCharacteristics> getDatacenterCharacteristicsList() {
        return datacenterCharacteristicsList;
    }

    /**
     * Sets the datacenter characteristics list.
     *
     * @param datacenterCharacteristicsList the datacenter characteristics list
     */
    protected void setDatacenterCharacteristicsList(
            Map<Integer, ContainerDatacenterCharacteristics> datacenterCharacteristicsList) {
        this.datacenterCharacteristicsList = datacenterCharacteristicsList;
    }

    /**
     * Gets the datacenter requested ids list.
     *
     * @return the datacenter requested ids list
     */
    protected List<Integer> getDatacenterRequestedIdsList() {
        return datacenterRequestedIdsList;
    }

    /**
     * Sets the datacenter requested ids list.
     *
     * @param datacenterRequestedIdsList the new datacenter requested ids list
     */
    protected void setDatacenterRequestedIdsList(List<Integer> datacenterRequestedIdsList) {
        this.datacenterRequestedIdsList = datacenterRequestedIdsList;
    }

//------------------------------------------------

    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }

    public void setContainerList(List<? extends Container> containerList) {
        this.containerList = containerList;
    }

    /**
     * This method is used to send to the broker the list with virtual machines that must be
     * created.
     *
     * @param list the list
     * @pre list !=null
     * @post $none
     */
    public void submitContainerList(List<? extends Container> list) {
        getContainerList().addAll(list);
    }


    public Map<Integer, Integer> getContainersToVmsMap() {
        return containersToVmsMap;
    }

    public void setContainersToVmsMap(Map<Integer, Integer> containersToVmsMap) {
        this.containersToVmsMap = containersToVmsMap;
    }

    public <T extends Container> List<T> getContainersCreatedList() {
        return (List<T>) containersCreatedList;
    }

    public void setContainersCreatedList(List<? extends Container> containersCreatedList) {
        this.containersCreatedList = containersCreatedList;
    }

    public int getContainersAcks() {
        return containersAcks;
    }

    public void setContainersAcks(int containersAcks) {
        this.containersAcks = containersAcks;
    }

    public int getContainersCreated() {
        return containersCreated;
    }

    public void setContainersCreated(int containersCreated) {
        this.containersCreated = containersCreated;
    }

    public double getOverBookingfactor() {
        return overBookingfactor;
    }

    public void setOverBookingfactor(double overBookingfactor) {
        this.overBookingfactor = overBookingfactor;
    }

    public int getNumberOfCreatedVMs() {
        return numberOfCreatedVMs;
    }

    public void setNumberOfCreatedVMs(int numberOfCreatedVMs) {
        this.numberOfCreatedVMs = numberOfCreatedVMs;
    }


    /**
     * Binds this scheduler to a datacenter
     *
     * @param datacenterId data center id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        if (datacenterId <= 0) {
            Log.printLine("Error in data center id");
            return;
        }
        this.datacenterIdsList.add(datacenterId);
    }

    /**
     * Sets the workflow engine id
     *
     * @param workflowEngineId the workflow engine id
     */
    public void setWorkflowEngineId(int workflowEngineId) {
        this.workflowEngineId = workflowEngineId;
    }


    /**
     * Switch between multiple schedulers. Based on algorithm.method
     *
     * @param name the SchedulingAlgorithm name
     * @return the algorithm that extends BaseSchedulingAlgorithm
     */
    private BaseSchedulingAlgorithm getScheduler(Parameters.SchedulingAlgorithm name) {
        BaseSchedulingAlgorithm algorithm;

        // choose which algorithm to use. Make sure you have add related enum in
        //Parameters.java
        switch (name) {
            //by default it is Static
            case FCFS:
                algorithm = new FCFSSchedulingAlgorithm();
                break;
            case MINMIN:
                algorithm = new MinMinSchedulingAlgorithm();
                break;
            case MAXMIN:
                algorithm = new MaxMinSchedulingAlgorithm();
                break;
            case MCT:
                algorithm = new MCTSchedulingAlgorithm();
                break;
            case DATA:
                algorithm = new DataAwareSchedulingAlgorithm();
                break;
            case STATIC:
                algorithm = new StaticSchedulingAlgorithm();
                break;
            case ROUNDROBIN:
                algorithm = new RoundRobinSchedulingAlgorithm();
                break;

            //下面几种情况是自己添加的
            case JUSTTRY:
                algorithm = new JustToTryAlgorithm();
                break;
            case VMJUSTTRY:
                algorithm = new ByVmJustToTryAlogorithm();
                break;
            case GREEDY:
                algorithm = new ContainerGreedyAlgorithm();
                break;
            case TRYMAJOR:
                algorithm = new JustTryMajorAlogrithm();
                break;
            case INVALID:
                algorithm = new InvalidSchedulingAlgorithm();
                break;
            case SCDY:
                algorithm = new SchedulingDynamicWorkloadsAlgorithm();
                break;
            case FUTURESCDY:
                algorithm = new FutureVMSchedulingDynamicAlgorithm();
                break;
            case ORISCDY:
                algorithm = new OriginalSchedulingDynamicWorkloadsAlgorithm();
                break;
            case SLOW:
                algorithm = new SlowestAlgorithm();
                break;
            default:
                algorithm = new StaticSchedulingAlgorithm();
                break;

        }
        return algorithm;
    }

    class MyComparator implements Comparator<ContainerCloudlet> {
//        private Map<Integer, Double> lstMap;
//        MyComparator(Map<Integer, Double> lstMap){
//            this.lstMap = lstMap;
//        }

        @Override
        public int compare(ContainerCloudlet o1, ContainerCloudlet o2) {
            double lst1 = o1.getDeadline() - o1.getTransferTime() - o1.getCloudletLength() / ContainerVmType.EXTRA_LARGE.getMips();
            double lst2 = o2.getDeadline() - o2.getTransferTime() - o2.getCloudletLength() / ContainerVmType.EXTRA_LARGE.getMips();
            if(lst1 > lst2){
                return 1;
            }else if(lst1 < lst2){
                return -1;
            }else{
                return 0;
            }
        }
    }

    protected void sortByDeadline(List<ContainerCloudlet> scheduledList){
        Collections.sort(scheduledList, new Comparator<ContainerCloudlet>() {
            @Override
            public int compare(ContainerCloudlet o1, ContainerCloudlet o2) {
                if(o1.getDeadline() > o2.getDeadline()){
                    return 1;
                }else if(o1.getDeadline() < o2.getDeadline()){
                    return -1;
                }else{
                    return 0;
                }
            }
        });
    }

    protected void sortByLatestStartTime(List<ContainerCloudlet> scheduledList){
//        // 每个任务的最晚开始时间，数字是绝对时间
//        Map<Integer, Double> lstMap = new HashMap<>();
//        // 每个任务的处理时间
//        Map<Integer, Double> ptMap = new HashMap<>();
//        // 计算处理时间
//        for(ContainerCloudlet cloudlet : scheduledList){
//            //这里计算时间使用的是最快VM的mips
//            double pt = cloudlet.getTransferTime() + (double) cloudlet.getCloudletLength() / WFCConstants.EXTRA_LARGE_MIPS;
//            ptMap.put(cloudlet.getCloudletId(), pt);
//        }
//        for(ContainerCloudlet cloudlet : scheduledList){
//            lstMap.put(cloudlet.getCloudletId(), cloudlet.getDeadline() - ptMap.get(cloudlet.getCloudletId()));
//        }
        Collections.sort(scheduledList, new MyComparator());
    }

    /**
     * Update a cloudlet (job)
     * 作用：为提交的任务确定对应的VM的id，并通知WFCDatacenter
     *
     * @param ev a simEvent object
     */
    protected void processCloudletUpdate(SimEvent ev) {
        while(numEventsWaiting(new PredicateType(CloudSimTags.CLOUDLET_SUBMIT)) > 0){
            SimEvent submitEvent = getNextEvent(new PredicateType(CloudSimTags.CLOUDLET_SUBMIT));
            processEvent(submitEvent);
        }
        // 满足时间比例 0.915：不限制WFCScheduler的update，WFCEngine对最后一个事件不丢弃，推迟时间，WFCDatecenter对最后一个事件不丢弃，推迟时间。
        // 这段代码主要是为了解决某个事件出现得太频繁的问题，通过丢弃一部分事件，控制事件的发生间隔为 WFCConstants.MIN_TIME_BETWEEN_EVENTS
        // WFCConstants.MIN_TIME_BETWEEN_EVENTS 是一个常数，代表仿真的最小时间间隔
        // getLastUpdateTime() 是获取上次运行这个函数的时间，setLastUpdateTime() 是记录上次运行这个函数的时间
        if(CloudSim.clock() - getLastUpdateTime() < WFCConstants.MIN_TIME_BETWEEN_EVENTS / 5){
            // 每次调用 getNextEvent() 都会从时间队列中取出那个事件，并从队列中删除该事件
            // 把这里的 WorkflowSimTags.CLOUDLET_UPDATE 替换为本函数对应的那个事件标签
            SimEvent nextUpdateEvent = getNextEvent(new PredicateType(WorkflowSimTags.CLOUDLET_UPDATE));
            // 这个循环的作用是把最小间隔之前的相同事件，全部删掉
            // 这里也要替换掉 WorkflowSimTags.CLOUDLET_UPDATE
            while(null != nextUpdateEvent && nextUpdateEvent.eventTime() - getLastUpdateTime() < WFCConstants.MIN_TIME_BETWEEN_EVENTS / 5){
                nextUpdateEvent = getNextEvent(new PredicateType(WorkflowSimTags.CLOUDLET_UPDATE));
            }
            // 给队列还回去比较晚的事件，如果为null，要自己补充一个事件，这个事件只有delay不同
            // WFCScheduler的这里不能丢弃所有距离过近的事件，否则时间表现会明显下降
            if(null != nextUpdateEvent){
                schedule(nextUpdateEvent.getDestination(), nextUpdateEvent.eventTime() - CloudSim.clock(), nextUpdateEvent.getTag(), nextUpdateEvent.getData());
            }else{
                // 这里的 0.0001 只是为了让这个事件再延后一点点，以免这个事件被上述代码丢弃掉
                schedule(this.getId(), getLastUpdateTime() + WFCConstants.MIN_TIME_BETWEEN_EVENTS / 5 + 0.0001 - CloudSim.clock(), WorkflowSimTags.CLOUDLET_UPDATE);
            }
//            if(null != nextUpdateEvent){
//                schedule(nextUpdateEvent.getDestination(), nextUpdateEvent.eventTime() - CloudSim.clock(), nextUpdateEvent.getTag(), nextUpdateEvent.getData());
//            }
            return;
        }
        setLastUpdateTime(CloudSim.clock());
//        send(this.getId(), WFCConstants.MIN_TIME_BETWEEN_EVENTS + 0.0001, WorkflowSimTags.CLOUDLET_UPDATE);

        if (getLastUpdateOrContainerCreateTime() != CloudSim.clock()) {
            getLastSentCloudlets().clear();//考虑到调用check之前，在同一时间多次调用processCloudletUpdate()
            setLastUpdateOrContainerCreateTime(CloudSim.clock());
        }
        //getCloudletList()得到的是未调度的任务的列表，因为这个函数的最后会把其中调度过的任务去除掉
        List<ContainerCloudlet> scheduledList = getCloudletList();//scheduler.getScheduledList();
//        System.out.println(CloudSim.clock() + ":还有" + scheduledList.size() + "个任务等待调度");

        if(WFCConstants.PRINT_THREE_FUNCTION){
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":开始WFCScheduler的Update=========================");
        }

        if(WFCConstants.assignedAlgorithm == Parameters.SchedulingAlgorithm.SCDY || WFCConstants.assignedAlgorithm == Parameters.SchedulingAlgorithm.FUTURESCDY){
            // 按照deadline将任务排序
            sortByDeadline(scheduledList);
        }else {
            //按照最晚开始时间，将scheduledList中的任务进行排序
            sortByLatestStartTime(scheduledList);
        }

        //获取指定的调度算法
        BaseSchedulingAlgorithm scheduler = getScheduler(Parameters.getSchedulingAlgorithm());
        // 分别储存创建VM和销毁VM的列表，传给调度算法
        List<Container> toCreateContainerList = new ArrayList<>();
        // 本次调度要立即复制的任务
        List<ContainerCloudlet> toReplicateCloudlets = new ArrayList<>();
        // 本次调度算法想要创建的容器
        Map<Integer, Integer> vmPlanToCreateContainer = new HashMap<>();
        //如果有想要删除的VM，就把那些VM排除掉再传给调度算法
        List<ContainerVm> validVmList = new ArrayList<>(getVmsCreatedList());
        if (getToDestroyVmList().size() > 0) {
            validVmList.removeAll(getToDestroyVmList());
        }
        // 打乱可用VM数组中的元素
        Collections.shuffle(validVmList);

        scheduler.setCloudletList(scheduledList);
        scheduler.setVmList(validVmList);
        scheduler.setToCreateVmMap(getToCreateVmMap());
        scheduler.setToDestroyVmList(getToDestroyVmList());
        scheduler.setToCreateContainerList(toCreateContainerList);
        scheduler.setDetainedCloudletList(getDetainedCloudlets());
        scheduler.setLastSentCloudlets(getLastSentCloudlets());
        scheduler.setReplicateCloudlets(toReplicateCloudlets); //传入要立即复制的任务的列表
        scheduler.setHasSentReplicateCloudlets(getHasSentReplicateCloudlets()); //传入已经发送的要复制的任务
        scheduler.setCloudletsLeadToNewVm(getCloudletsLeadToNewVm());
        scheduler.setAverageDummyIdleTime(getAverageDummyIdleTime());
        scheduler.setVmsCreatingContainer(getVmToCreatingContainer());
        scheduler.setVmPlanToCreateContainer(vmPlanToCreateContainer);
        scheduler.setAlreadyNewContainerCloudletId(getAlreadyNewContainerCloudletId());
        scheduler.setAlreadyNewVmCloudletId(getAlreadyNewVmCloudletId());
        scheduler.setCompletedCloudlets(getCloudletReceivedSet());

        try {
            //确定每个任务对应的VM
            //传入这个WFCScheduler的Id，用来赋给新创建的VM
            scheduler.run(getId());
        } catch (Exception e) {

            Log.printLine("Error in configuring scheduler_method");
//            Log.printLine(e.getMessage());
            e.printStackTrace();
            //by arman I commented log  e.printStackTrace();
        }

        //让WFCEngine复制任务
        sendNow(this.workflowEngineId, WorkflowSimTags.CLOUDLET_REPLICATE, toReplicateCloudlets);
        getHasSentReplicateCloudlets().addAll(toReplicateCloudlets);
        //任务完成分配之后，需要查看其中哪些VM上需要创建新容器
        processDecideNewContainers(scheduledList, toCreateContainerList, vmPlanToCreateContainer);

        //在调度算法中确定任务所在的VM的id，然后发送给对应VM所在的Datacenter
        List<ContainerCloudlet> successfullySubmitted = new ArrayList<>();
        for (ContainerCloudlet cloudlet : scheduledList) {
            //如果没有找到容器对应的VM，则说明没有成功提交，下次重新提交
            ContainerVm targetVm = null;
            targetVm = getVmsCreatedMap().get(cloudlet.getVmId());
            //如果已有的VM中找不到该任务指定的，就去要创建的VM中寻找
            if (targetVm == null) {
                targetVm = getToCreateVmMap().get(cloudlet.getVmId());
            }
            //如果找到了要分配的VM（有可能该VM暂时还没创建完成）
            if (targetVm != null) {
//                Log.printLine("任务" + cloudlet.getCloudletId() + " 分配给了虚拟机 " + targetVm.getId());
                cloudletsSubmitted++;
                double delay = 0.0;
                if (Parameters.getOverheadParams().getQueueDelay() != null) {
                    delay = Parameters.getOverheadParams().getQueueDelay(cloudlet);
                }
                //getDetainedCloudlets()条件的地方一共有三处
                boolean detained = false;
                if (getDetainedCloudlets().contains(cloudlet)) {
                    detained = true;
                } else if (!getVmsCreatedMap().containsKey(targetVm.getId()) || targetVm.getContainerList().size() == 0
                        || (WFCConstants.ENABLE_SPECIFY_WORKFLOW_ID && targetVm.getContainerList().get(0).getWorkflowId() != cloudlet.getWorkflowId() && cloudlet.getWorkflowId() != 0)
                        || vmToCreatingContainer.containsKey(cloudlet.getVmId())
                ) {
                    detained = true;
                    getDetainedCloudlets().add(cloudlet);
                    if(WFCConstants.PRINT_DETAINED_LIST){
                        Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "因为VM上容器类型不符合，进入滞留队列");
                    }
                }
                //如果不是滞留的任务,才真正提交
                if (!detained) {
                    cloudlet.setContainerId(targetVm.getContainerList().get(0).getId());
                    schedule(getVmsToDatacentersMap().get(cloudlet.getVmId()), delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet);
                    WFCDeadline.wfcSchedulerSendJobNum++;
                    getLastSentCloudlets().add(cloudlet);
//                    Log.printLine(CloudSim.clock() + "WFCScheduler发送了处理任务" + cloudlet.getCloudletId() + "的请求，任务的虚拟机是" + cloudlet.getVmId());
                }
                successfullySubmitted.add(cloudlet);
            }
        }

        //如果把这个语句放到了processCloudletReturn()中，就要小心WFCDatacenter如果同时返回VM创建的确认和任务完成的确认，哪个在先哪个在后
        getCloudletList().removeAll(successfullySubmitted);
        // successfullySubmitted中的任务有可能是VM正在创建，还没有放到VM上的
        getCloudletSubmittedList().addAll(successfullySubmitted);

        //这里算是动态创建VM的起点，可以通过WFCConstants.ENABLE_DYNAMIC_VM_CREATE控制动态创建的开关
        if (WFCConstants.ENABLE_DYNAMIC_VM_CREATE && getToCreateVmMap().size() > 0) {
            //getVmList()得到的是所有创建过的VM的列表
            List<ContainerVm> immediatelyCreateVm = new ArrayList<>();
            for(ContainerVm vm : getToCreateVmMap().values()){
                // 如果这个VM还没有被发送过
                if(!getSendCreateVmListToDatacenter().containsKey(vm.getId())){
                    // 用来计算成本的开始时间，应该是开始创建流程的时间，而不是创建好的时间
                    vm.setCreatingTime(CloudSim.clock());
                    immediatelyCreateVm.add(vm);
                    getSendCreateVmListToDatacenter().put(vm.getId(), vm);
                }
            }
            //把要创建的VM立即发送给WFCDatacenter
            createVmsInDatacenter(getDatacenterIdsList().get(0), immediatelyCreateVm);
        }
//        setLastUpdateTime(CloudSim.clock());
        if(WFCConstants.PRINT_THREE_FUNCTION){
            Log.printLine(CloudSim.clock() + ":WFCScheduler的Update=========================结束");
        }
//        if(getCloudletList().size() > 0){
//            schedule(this.getId(), getLastUpdateTime() + WFCConstants.MIN_TIME_BETWEEN_EVENTS
//                    + 0.001 - CloudSim.clock(), WorkflowSimTags.CLOUDLET_UPDATE);
//        }
    }

    /**
     * 任务完成分配之后，需要查看其中哪些VM上需要创建新容器
     */
    public void processDecideNewContainers(List<ContainerCloudlet> scheduledList, List<Container> toCreateContainerList) {
        // 如果只有stage-in任务，则不需要处理
        if(scheduledList.size() == 1 && scheduledList.get(0).getWorkflowId() == 0){
            return;
        }
        //vmToNewContainerType记录那些需要新创建容器的VM上，需要创建什么类型的容器
        Map<Integer, Integer> vmToNewContainerType = new HashMap<>();

        // 下面考虑到了，一个VM上已有的任务的优先级是最高的
        // 1.看看同一时间已经发送出去的任务的需求
        if (getLastUpdateOrContainerCreateTime() == CloudSim.clock()) {
            for (ContainerCloudlet cloudlet : getLastSentCloudlets()) {
                if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                    vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
                }
            }
        }

        //2.看看detainedCloudlet中任务的需求，其优先级是第二高的
        for (ContainerCloudlet cloudlet : getDetainedCloudlets()) {
//            Log.printConcatLine(CloudSim.clock(), "看延迟列表中，任务", cloudlet.getCloudletId(), "的需求");
            //每个VM只接收第一个任务的指定
            if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
//                Log.printConcatLine(CloudSim.clock(), "接受了延迟列表中，任务", cloudlet.getCloudletId(), "的需求");
            }
        }

        //3.看看scheduledList中，新分配的任务的需求
        for (ContainerCloudlet cloudlet : scheduledList) {
            if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
            }
        }

        //整理完需求之后，VM需要创建新容器的情况：
        //1. 必要条件：vmToNewContainerType.containsKey(targetVm.getId())
        //2. 必要条件：VM当前没有正在创建的容器
        //2. 可能的情况：VM当前没有容器
        //3. 可能的情况：VM当前有容器，且VM的容器当前没有正在运行的任务，且当前容器类型与需求不匹配
        //怎么避免因为VM上新容器创建有延迟，而重复发送创建新容器的命令？可以创建一个Map，记录每个正在创建新容器的VM，其未来的新容器的workflowId
        for (ContainerVm targetVm : getVmsCreatedList()) {
            //必要条件：对VM的容器类型有要求，且VM没有正在创建的容器
            if (vmToNewContainerType.containsKey(targetVm.getId()) && !vmToCreatingContainer.containsKey(targetVm.getId())) {
                //VM当前没有容器
                if (targetVm.getContainerList().size() == 0) {
                    //vmToNewContainerType中要求创建什么类型的容器，就创建什么类型的
                    Container newContainer = new JustToTryPowerContainer(vmToNewContainerType.get(targetVm.getId()), IDs.pollId(Container.class), getId(), (double) WFCConstants.WFC_CONTAINER_MIPS,
                            WFCConstants.WFC_CONTAINER_PES_NUMBER, WFCConstants.WFC_CONTAINER_RAM,
                            WFCConstants.WFC_CONTAINER_BW, WFCConstants.WFC_CONTAINER_SIZE, WFCConstants.WFC_CONTAINER_VMM,
                            new ContainerCloudletSchedulerJustToTry(WFCConstants.WFC_NUMBER_WORKFLOW, WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                            WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                    // 判断是否有任务已经分配给了即将切换容器的VM，如果有，需要WFCScheduler代替调度算法保存映射关系，
                    // 等到确认容器创建完成后，再赋给对应的VM和新容器
                    for (ContainerCloudlet cloudlet : scheduledList) {
                        if (cloudlet.getVmId() == targetVm.getId()) {
                            getDetainedCloudlets().add(cloudlet);
                            if(WFCConstants.PRINT_DETAINED_LIST){
                                Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "因为VM切换容器，进入滞留队列");
                            }
                        }
                    }
                    newContainer.setVm(targetVm);
                    toCreateContainerList.add(newContainer);

                    //开始容器创建的消息流程
                    if(WFCConstants.PRINT_CONTAINER_CREATE){
                        Log.printLine(CloudSim.clock() + ":虚拟机" + targetVm.getId() + "开始动态创建新容器，类型为：" + newContainer.getWorkflowId());
                    }

                } else {
                    //如果VM当前已有容器
                    Container replacedContainer = targetVm.getContainerList().get(0);
                    ContainerCloudletScheduler containerCloudletScheduler = replacedContainer.getContainerCloudletScheduler();
                    //如果容器上没有正在运行或等待的任务，且容器类型与需求不一致，就创建新容器
                    if (containerCloudletScheduler.getCloudletExecList().size() + containerCloudletScheduler.getCloudletWaitingList().size() == 0
                            && (WFCConstants.ENABLE_SPECIFY_WORKFLOW_ID && replacedContainer.getWorkflowId() != vmToNewContainerType.get(targetVm.getId()))
                    ) {
                        Container newContainer = new JustToTryPowerContainer(vmToNewContainerType.get(targetVm.getId()), IDs.pollId(Container.class), getId(), (double) WFCConstants.WFC_CONTAINER_MIPS,
                                WFCConstants.WFC_CONTAINER_PES_NUMBER, WFCConstants.WFC_CONTAINER_RAM,
                                WFCConstants.WFC_CONTAINER_BW, WFCConstants.WFC_CONTAINER_SIZE, WFCConstants.WFC_CONTAINER_VMM,
                                new ContainerCloudletSchedulerJustToTry(WFCConstants.WFC_NUMBER_WORKFLOW, WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                                WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                        // 判断是否有任务已经分配给了即将切换容器的VM，如果有，需要WFCScheduler代替调度算法保存映射关系，
                        // 等到确认容器创建完成后，再赋给对应的VM和新容器
                        for (ContainerCloudlet cloudlet : scheduledList) {
                            if (cloudlet.getVmId() == targetVm.getId()) {
                                getDetainedCloudlets().add(cloudlet);
                                if(WFCConstants.PRINT_DETAINED_LIST){
                                    Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "因为VM切换容器，进入滞留队列");
                                }
                            }
                        }

                        newContainer.setVm(targetVm);
                        toCreateContainerList.add(newContainer);
                        //开始容器创建的消息流程
                        if(WFCConstants.PRINT_CONTAINER_CREATE){
                            Log.printLine(CloudSim.clock() + ":虚拟机" + targetVm.getId() + "开始动态创建新容器，类型为：" + newContainer.getWorkflowId());
                        }
                    }
                }
            }
        }
        //之后收到WFCDatacenter的容器创建确认消息后，要用到getContainerList()
        getContainerList().addAll(toCreateContainerList);
        submitContianers(toCreateContainerList);
    }

    /**
     * 重载processDecideNewContainers()，入参多考虑一个调度算法指定创建的容器
     * @param scheduledList
     * @param toCreateContainerList
     * @param vmPlanToCreateContainer
     */
    public void processDecideNewContainers(List<ContainerCloudlet> scheduledList, List<Container> toCreateContainerList, Map<Integer, Integer> vmPlanToCreateContainer) {
        // 如果只有stage-in任务，则不需要处理
        if(scheduledList.size() == 1 && scheduledList.get(0).getWorkflowId() == 0){
            return;
        }
        //vmToNewContainerType记录那些需要新创建容器的VM上，需要创建什么类型的容器
        Map<Integer, Integer> vmToNewContainerType = new HashMap<>();

        // 下面考虑到了，一个VM上已有的任务的优先级是最高的
        // 1.查看同一时间已经发送出去的任务的需求
        if (getLastUpdateOrContainerCreateTime() == CloudSim.clock()) {
            for (ContainerCloudlet cloudlet : getLastSentCloudlets()) {
                if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                    vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
                }
            }
        }

        //2.查看detainedCloudlet中任务的需求，其优先级是第二高的
        for (ContainerCloudlet cloudlet : getDetainedCloudlets()) {
//            Log.printConcatLine(CloudSim.clock(), "看延迟列表中，任务", cloudlet.getCloudletId(), "的需求");
            //每个VM只接收第一个任务的指定
            if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
//                Log.printConcatLine(CloudSim.clock(), "接受了延迟列表中，任务", cloudlet.getCloudletId(), "的需求");
            }
        }

        //3.查看scheduledList中，新分配的任务的需求
        for (ContainerCloudlet cloudlet : scheduledList) {
            if (!vmToNewContainerType.containsKey(cloudlet.getVmId())) {
                vmToNewContainerType.put(cloudlet.getVmId(), cloudlet.getWorkflowId());
            }
        }

        //4.查看调度算法中指定创建的workflow id的容器
        for(Map.Entry<Integer, Integer> entry : vmPlanToCreateContainer.entrySet()){
            if(!vmToNewContainerType.containsKey(entry.getKey())){
                vmToNewContainerType.put(entry.getKey(), entry.getValue());
            }
        }

        //整理完需求之后，VM需要创建新容器的情况：
        //1. 必要条件：vmToNewContainerType.containsKey(targetVm.getId())
        //2. 必要条件：VM当前没有正在创建的容器
        //2. 可能的情况：VM当前没有容器
        //3. 可能的情况：VM当前有容器，且VM的容器当前没有正在运行的任务，且当前容器类型与需求不匹配
        //怎么避免因为VM上新容器创建有延迟，而重复发送创建新容器的命令？可以创建一个Map，记录每个正在创建新容器的VM，其未来的新容器的workflowId
        for (ContainerVm targetVm : getVmsCreatedList()) {
            //必要条件：对VM的容器类型有要求，且VM没有正在创建的容器
            if (vmToNewContainerType.containsKey(targetVm.getId()) && !vmToCreatingContainer.containsKey(targetVm.getId())) {
                //VM当前没有容器
                if (targetVm.getContainerList().size() == 0) {
                    //vmToNewContainerType中要求创建什么类型的容器，就创建什么类型的
                    Container newContainer = new JustToTryPowerContainer(vmToNewContainerType.get(targetVm.getId()), IDs.pollId(Container.class), getId(), (double) WFCConstants.WFC_CONTAINER_MIPS,
                            WFCConstants.WFC_CONTAINER_PES_NUMBER, WFCConstants.WFC_CONTAINER_RAM,
                            WFCConstants.WFC_CONTAINER_BW, WFCConstants.WFC_CONTAINER_SIZE, WFCConstants.WFC_CONTAINER_VMM,
                            new ContainerCloudletSchedulerJustToTry(WFCConstants.WFC_NUMBER_WORKFLOW, WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                            WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                    // 判断是否有任务已经分配给了即将切换容器的VM，如果有，需要WFCScheduler代替调度算法保存映射关系，
                    // 等到确认容器创建完成后，再赋给对应的VM和新容器
                    for (ContainerCloudlet cloudlet : scheduledList) {
                        if (cloudlet.getVmId() == targetVm.getId()) {
                            getDetainedCloudlets().add(cloudlet);
                            if(WFCConstants.PRINT_DETAINED_LIST){
                                Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "因为VM切换容器，进入滞留队列");
                            }
                        }
                    }
                    newContainer.setVm(targetVm);
                    toCreateContainerList.add(newContainer);

                    //开始容器创建的消息流程
                    if(WFCConstants.PRINT_CONTAINER_CREATE){
                        Log.printLine(CloudSim.clock() + ":虚拟机" + targetVm.getId() + "开始动态创建新容器，类型为：" + newContainer.getWorkflowId());
                    }

                } else {
                    //如果VM当前已有容器
                    Container replacedContainer = targetVm.getContainerList().get(0);
                    ContainerCloudletScheduler containerCloudletScheduler = replacedContainer.getContainerCloudletScheduler();
                    //如果容器上没有正在运行或等待的任务，且容器类型与需求不一致，就创建新容器
                    if (containerCloudletScheduler.getCloudletExecList().size() + containerCloudletScheduler.getCloudletWaitingList().size() == 0
                            && (WFCConstants.ENABLE_SPECIFY_WORKFLOW_ID && replacedContainer.getWorkflowId() != vmToNewContainerType.get(targetVm.getId()))
                    ) {
                        Container newContainer = new JustToTryPowerContainer(vmToNewContainerType.get(targetVm.getId()), IDs.pollId(Container.class), getId(), (double) WFCConstants.WFC_CONTAINER_MIPS,
                                WFCConstants.WFC_CONTAINER_PES_NUMBER, WFCConstants.WFC_CONTAINER_RAM,
                                WFCConstants.WFC_CONTAINER_BW, WFCConstants.WFC_CONTAINER_SIZE, WFCConstants.WFC_CONTAINER_VMM,
                                new ContainerCloudletSchedulerJustToTry(WFCConstants.WFC_NUMBER_WORKFLOW, WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                                WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                        // 判断是否有任务已经分配给了即将切换容器的VM，如果有，需要WFCScheduler替调度算法保存任务到虚拟机的映射关系，
                        // 等到确认容器创建完成后，再赋给对应的VM和新容器
                        for (ContainerCloudlet cloudlet : scheduledList) {
                            if (cloudlet.getVmId() == targetVm.getId()) {
                                getDetainedCloudlets().add(cloudlet);
                                if(WFCConstants.PRINT_DETAINED_LIST){
                                    Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "因为VM切换容器，进入滞留队列");
                                }
                            }
                        }
                        newContainer.setVm(targetVm);
                        toCreateContainerList.add(newContainer);
                        //开始容器创建的消息流程
                        if(WFCConstants.PRINT_CONTAINER_CREATE){
                            Log.printLine(CloudSim.clock() + ":虚拟机" + targetVm.getId() + "开始动态创建新容器，类型为：" + newContainer.getWorkflowId());
                        }
                    }
                }
            }
        }
        //之后收到WFCDatacenter的容器创建确认消息后，要用到getContainerList()
        getContainerList().addAll(toCreateContainerList);
        submitContianers(toCreateContainerList);
    }


    /**
     * A trick here. Assure that we just submit it once
     */
    private boolean processCloudletSubmitHasShown = false;

    /**
     * Submits cloudlet (job) list
     *
     * @param ev a simEvent object
     */
    protected void processCloudletSubmit(SimEvent ev) {
//        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": 调用了WFCScheduler的processCloudletSubmit()");
        List<Job> list = (List) ev.getData();
//        if(list.size() > 0){
//            Log.printLine(CloudSim.clock() + "WFCScheduler接收到了发来的新任务，第一个任务是：" + list.get(0).getCloudletId() + "，总数为" + list.size());
//        }
        getCloudletList().addAll(list);

        sendNow(this.getId(), WorkflowSimTags.CLOUDLET_UPDATE);
        if (!processCloudletSubmitHasShown) {
            processCloudletSubmitHasShown = true;
        }
    }

    /**
     * Submit cloudlets to the created VMs.
     *
     * @pre $none
     * @post $none
     */
    protected void submitCloudlets() {
//        System.out.println("调用了WFCScheduler的submitCloudlets()，时间是" + CloudSim.clock());
        //sendNow(getDatacenterIdsList().get(0), WorkflowSimTags.CLOUDLET_UPDATE, null);
        sendNow(this.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, null);


    }


}


