package org.wfc.core;

import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.text.DecimalFormat;
import java.util.*;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.containerCloudSimTags;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerDatacenterCharacteristics;
import org.cloudbus.cloudsim.container.core.ContainerHost;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.core.predicates.PredicateType;
import org.wfc.core.WFCConstants;
import org.workflowsim.FileItem;
import org.workflowsim.Job;
import org.workflowsim.Task;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.WFCReplicaCatalog;

/**
 * Created by sareh on 13/08/15.
 */
public class WFCDatacenter extends SimEntity {

    /**
     * The characteristics.
     */
    private ContainerDatacenterCharacteristics characteristics;

    /**
     * The regional cis name.
     */
    private String regionalCisName;

    /**
     * The vm provisioner.
     */
    private ContainerVmAllocationPolicy vmAllocationPolicy;
    /**
     * The container provisioner.
     */
    private ContainerAllocationPolicy containerAllocationPolicy;

    /**
     * The last process time.
     */
    private double lastProcessTime;

    /**
     * The storage list.
     */
    private List<Storage> storageList;

    /**
     * The vm list.
     */
    private List<? extends ContainerVm> containerVmList;
    /**
     * The container list.
     */
    private List<? extends Container> containerList;

    /**
     * The scheduling interval.
     */
    private double schedulingInterval;
    /**
     * The scheduling interval.
     */
    private String experimentName;
    /**
     * The log address.
     */
    private String logAddress;

    /**
     * 添加一个属性，用来计算总的cost
     */
    public static Double totalCost = 0.0;

    /**
     * 用来让每轮都调用updateCloduletProcessing()两次
     */
    private boolean secondTime;


    /**
     * Allocates a new PowerDatacenter object.
     *
     * @param name
     * @param characteristics
     * @param vmAllocationPolicy
     * @param containerAllocationPolicy
     * @param storageList
     * @param schedulingInterval
     * @param experimentName
     * @param logAddress
     * @throws Exception
     */
    public WFCDatacenter(
            String name,
            ContainerDatacenterCharacteristics characteristics,
            ContainerVmAllocationPolicy vmAllocationPolicy,
            ContainerAllocationPolicy containerAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval, String experimentName, String logAddress) throws Exception {
        super(name);

        setCharacteristics(characteristics);
        setVmAllocationPolicy(vmAllocationPolicy);
        setContainerAllocationPolicy(containerAllocationPolicy);
        setLastProcessTime(0.0);
        setStorageList(storageList);
        setContainerVmList(new ArrayList<ContainerVm>());
        setContainerList(new ArrayList<Container>());
        setSchedulingInterval(schedulingInterval);
        setExperimentName(experimentName);
        setLogAddress(logAddress);
        secondTime = false;

        for (ContainerHost host : getCharacteristics().getHostList()) {
            host.setDatacenter(this);
        }

        // If this resource doesn't have any PEs then no useful at all
        if (getCharacteristics().getNumberOfPes() == 0) {
            throw new Exception(super.getName()
                    + " : Error - this entity has no PEs. Therefore, can't process any Cloudlets.");
        }

        // stores id of this class
        getCharacteristics().setId(super.getId());
    }

    /**
     * Overrides this method when making a new and different type of resource. <br>
     * <b>NOTE:</b> You do not need to override {} method, if you use this method.
     *
     * @pre $none
     * @post $none
     */
    protected void registerOtherEntity() {
        // empty. This should be override by a child class
    }

    /**
     * Processes events or services that are available for this PowerDatacenter.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    @Override
    public void processEvent(SimEvent ev) {

        int srcId = -1;
        if (WFCConstants.CAN_PRINT_SEQ_LOG)
            Log.printLine("ContainerDataCener=>ProccessEvent()=>ev.getTag():" + ev.getTag());

        switch (ev.getTag()) {
            // Resource characteristics inquiry
            case CloudSimTags.RESOURCE_CHARACTERISTICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), getCharacteristics());
                break;

            // Resource dynamic info inquiry
            case CloudSimTags.RESOURCE_DYNAMICS:
                srcId = ((Integer) ev.getData()).intValue();
                sendNow(srcId, ev.getTag(), 0);
                break;

            case CloudSimTags.RESOURCE_NUM_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int numPE = getCharacteristics().getNumberOfPes();
                sendNow(srcId, ev.getTag(), numPE);
                break;

            case CloudSimTags.RESOURCE_NUM_FREE_PE:
                srcId = ((Integer) ev.getData()).intValue();
                int freePesNumber = getCharacteristics().getNumberOfFreePes();
                sendNow(srcId, ev.getTag(), freePesNumber);
                break;

            // New Cloudlet arrives
            case CloudSimTags.CLOUDLET_SUBMIT:
                processCloudletSubmit(ev, false);
                break;

            // New Cloudlet arrives, but the sender asks for an ack
            case CloudSimTags.CLOUDLET_SUBMIT_ACK:
                processCloudletSubmit(ev, true);
                break;

            // Cancels a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudlet(ev, CloudSimTags.CLOUDLET_CANCEL);
                break;

            // Pauses a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE);
                break;

            // Pauses a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_PAUSE_ACK);
                break;

            // Resumes a previously submitted Cloudlet
            case CloudSimTags.CLOUDLET_RESUME:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME);
                break;

            // Resumes a previously submitted Cloudlet, but the sender
            // asks for an acknowledgement
            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudlet(ev, CloudSimTags.CLOUDLET_RESUME_ACK);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE);
                break;

            // Moves a previously submitted Cloudlet to a different resource
            case CloudSimTags.CLOUDLET_MOVE_ACK:
                processCloudletMove((int[]) ev.getData(), CloudSimTags.CLOUDLET_MOVE_ACK);
                break;

            // Checks the status of a Cloudlet
            case CloudSimTags.CLOUDLET_STATUS:
                processCloudletStatus(ev);
                break;

            // Ping packet
            case CloudSimTags.INFOPKT_SUBMIT:
                processPingRequest(ev);
                break;

            case CloudSimTags.VM_CREATE:
                if (!WFCDeadline.endSimulation) {
                    processVmCreate(ev, false);
                }
                break;

            case CloudSimTags.VM_CREATE_ACK:
                if (!WFCDeadline.endSimulation) {
                    processVmCreate(ev, true);
                }
                break;

            case CloudSimTags.VM_DESTROY:
                processVmDestroy(ev, false);
                break;

            case CloudSimTags.VM_DESTROY_ACK:
                processVmDestroy(ev, true);
                break;

            case CloudSimTags.VM_MIGRATE:
                processVmMigrate(ev, false);
                break;

            case CloudSimTags.VM_MIGRATE_ACK:
                processVmMigrate(ev, true);
                break;

            case CloudSimTags.VM_DATA_ADD:
                processDataAdd(ev, false);
                break;

            case CloudSimTags.VM_DATA_ADD_ACK:
                processDataAdd(ev, true);
                break;

            case CloudSimTags.VM_DATA_DEL:
                processDataDelete(ev, false);
                break;

            case CloudSimTags.VM_DATA_DEL_ACK:
                processDataDelete(ev, true);
                break;

            case CloudSimTags.VM_DATACENTER_EVENT:
                //为了应对future队列中堆积过多的VM_DATACENTER_EVENT事项
                if (!WFCDeadline.endSimulation) {
                    updateCloudletProcessing();
                    checkCloudletCompletion();
                }
                break;
            case containerCloudSimTags.CONTAINER_SUBMIT:
                processContainerSubmit(ev, true);
                break;

            case containerCloudSimTags.CONTAINER_MIGRATE:
                processContainerMigrate(ev, false);
                // other unknown tags are processed by this method
                break;
            //新增一个事件，VM宕机
            case CloudSimTags.VM_SHUTDOWN:
                processVmShutdown(ev, true);
                break;
            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * 处理WFCScheduler发来的VM宕机的命令
     *
     * @param ev
     */
    private void processVmShutdown(SimEvent ev, boolean ack) {
        int vmId = (int) ev.getData();
        ContainerVm targetVm = null;
        for (ContainerVm vm : getContainerVmList()) {
            if (vm.getId() == vmId) {
                targetVm = vm;
                break;
            }
        }
        if (targetVm == null) {
//            Log.printLine(CloudSim.clock() + ":即将宕机的VM并不存在");
            return;
        }

        targetVm.setDestroyTime(CloudSim.clock());
        totalCost += targetVm.getTotalCost();
        if (WFCConstants.PRINT_COST_CALCULATE) {
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":虚拟机" + targetVm.getId() + "宕机，成本变为：" + totalCost);
        }
        getVmAllocationPolicy().deallocateHostForVm(targetVm);
        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = targetVm.getId();
            data[2] = CloudSimTags.TRUE;
            sendNow(targetVm.getUserId(), CloudSimTags.VM_SHUTDOWN_ACK, data);
        }
        getContainerVmList().remove(targetVm);
    }

    protected void processContainerSubmit(SimEvent ev, boolean ack) {
        // 新加的，在创建新Container之前，先把容器上完成了的任务都发送到WFCEngine
        checkCloudletCompletion();

        List<Container> containerList = (List<Container>) ev.getData();

        for (Container container : containerList) {
//            Log.printLine("正在处理创建容器" + container.getId() + "的命令");
            //分情况处理，如果container已经有指定的VM，就不再调用上面的分配方法，而是直接分配给特定的VM
            //如果container没有指定VM，则为其寻找和分配VM
            boolean result = false;
            if (null != container.getVm()) {
                //下面这行代码，不设置应该没问题，因为ContainerAllocationPolicy的ContainerVmList也只在为Container寻找VM才用到
                //但是这个分支里Container已经有了指定的VM
//                getContainerAllocationPolicy().setContainerVmList(getContainerVmList());

                //如果取出来的VM不是null，就在VM上创建Container，同时在映射表上记录
//                Log.printLine("这个容器指定了VM");
                result = getContainerAllocationPolicy().allocateVmForContainer(container, container.getVm());
            } else {
//                Log.printLine("这个容器并没有指定VM");
                result = getContainerAllocationPolicy().allocateVmForContainer(container, getContainerVmList());
            }
            if (ack) {
                int[] data = new int[3];
                data[1] = container.getId();
                if (result) {
                    data[2] = CloudSimTags.TRUE;
                } else {
                    data[2] = CloudSimTags.FALSE;
                }
                if (result) {
                    ContainerVm containerVm = getContainerAllocationPolicy().getContainerVm(container);
                    data[0] = containerVm.getId();
                    if (containerVm.getId() == -1) {

                        Log.printConcatLine("The ContainerVM ID is not known (-1) !");
                    }
//                    Log.printConcatLine("Assigning the container#" + container.getUid() + "to VM #" + containerVm.getUid());
                    getContainerList().add(container);
                    if (container.isBeingInstantiated()) {
                        container.setBeingInstantiated(false);
                    }
                    container.updateContainerProcessing(CloudSim.clock(), getContainerAllocationPolicy().getContainerVm(container).getContainerScheduler().getAllocatedMipsForContainer(container));
                } else {
                    data[0] = -1;
                    //notAssigned.add(container);
                    Log.printLine(String.format("Couldn't find a vm to host the container #%s", container.getUid()));
                }

//                send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), containerCloudSimTags.CONTAINER_CREATE_ACK, data);
                //这里把延迟改为了0
//                Log.printLine("返回了创建容器" + container.getId() + "的确认");
                send(ev.getSource(), 0, containerCloudSimTags.CONTAINER_CREATE_ACK, data);

            }
        }

    }

    /**
     * Process data del.
     *
     * @param ev  the ev
     * @param ack the ack
     */
    protected void processDataDelete(SimEvent ev, boolean ack) {
        if (ev == null) {
            return;
        }

        Object[] data = (Object[]) ev.getData();
        if (data == null) {
            return;
        }

        String filename = (String) data[0];
        int req_source = ((Integer) data[1]).intValue();
        int tag = -1;

        // check if this file can be deleted (do not delete is right now)
        int msg = deleteFileFromStorage(filename);
        if (msg == DataCloudTags.FILE_DELETE_SUCCESSFUL) {
            tag = DataCloudTags.CTLG_DELETE_MASTER;
        } else { // if an error occured, notify user
            tag = DataCloudTags.FILE_DELETE_MASTER_RESULT;
        }

        if (ack) {
            // send back to sender
            Object pack[] = new Object[2];
            pack[0] = filename;
            pack[1] = Integer.valueOf(msg);

            sendNow(req_source, tag, pack);
        }
    }

    /**
     * Process data add.
     *
     * @param ev  the ev
     * @param ack the ack
     */
    protected void processDataAdd(SimEvent ev, boolean ack) {
        if (ev == null) {
            return;
        }

        Object[] pack = (Object[]) ev.getData();
        if (pack == null) {
            return;
        }

        File file = (File) pack[0]; // get the file
        file.setMasterCopy(true); // set the file into a master copy
        int sentFrom = ((Integer) pack[1]).intValue(); // get sender ID

        /******
         * // DEBUG Log.printLine(super.get_name() + ".addMasterFile(): " + file.getName() +
         * " from " + CloudSim.getEntityName(sentFrom));
         *******/

        Object[] data = new Object[3];
        data[0] = file.getName();

        int msg = addFile(file); // add the file

        if (ack) {
            data[1] = Integer.valueOf(-1); // no sender id
            data[2] = Integer.valueOf(msg); // the result of adding a master file
            sendNow(sentFrom, DataCloudTags.FILE_ADD_MASTER_RESULT, data);
        }
    }

    /**
     * Processes a ping request.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processPingRequest(SimEvent ev) {
        InfoPacket pkt = (InfoPacket) ev.getData();
        pkt.setTag(CloudSimTags.INFOPKT_RETURN);
        pkt.setDestId(pkt.getSrcId());

        // sends back to the sender
        sendNow(pkt.getSrcId(), CloudSimTags.INFOPKT_RETURN, pkt);
    }

    /**
     * Process the event for an User/Broker who wants to know the status of a Cloudlet. This
     * PowerDatacenter will then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processCloudletStatus(SimEvent ev) {
        int cloudletId = 0;
        int userId = 0;
        int vmId = 0;
        int containerId = 0;
        int status = -1;

        try {
            // if a sender using cloudletXXX() methods
            int data[] = (int[]) ev.getData();
            cloudletId = data[0];
            userId = data[1];
            vmId = data[2];
            containerId = data[3];
            //Log.printLine("Data Center is processing the cloudletStatus Event ");
            status = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).
                    getContainer(containerId, userId).getContainerCloudletScheduler().getCloudletStatus(cloudletId);
        }

        // if a sender using normal send() methods
        catch (ClassCastException c) {
            try {
                ContainerCloudlet cl = (ContainerCloudlet) ev.getData();
                cloudletId = cl.getCloudletId();
                userId = cl.getUserId();
                containerId = cl.getContainerId();

                status = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
                        .getContainerCloudletScheduler().getCloudletStatus(cloudletId);
            } catch (Exception e) {
                Log.printConcatLine(getName(), ": Error in processing CloudSimTags.CLOUDLET_STATUS");
                Log.printLine(e.getMessage());
                return;
            }
        } catch (Exception e) {
            Log.printConcatLine(getName(), ": Error in processing CloudSimTags.CLOUDLET_STATUS");
            Log.printLine(e.getMessage());
            return;
        }

        int[] array = new int[3];
        array[0] = getId();
        array[1] = cloudletId;
        array[2] = status;

        int tag = CloudSimTags.CLOUDLET_STATUS;
        sendNow(userId, tag, array);
    }

    /**
     * Here all the method related to VM requests will be received and forwarded to the related
     * method.
     *
     * @param ev the received event
     * @pre $none
     * @post $none
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printConcatLine(getName(), ".processOtherEvent(): Error - an event is null.");
        }
    }

    /**
     * Process the event for a User/Broker who wants to create a VM in this PowerDatacenter. This
     * PowerDatacenter will then send the status back to the User/Broker.
     *
     * @param ev  a Sim_event object
     * @param ack the ack
     * @pre ev != null
     * @post $none
     */
    protected void processVmCreate(SimEvent ev, boolean ack) {
//        Log.printLine("called VmCreate in WFCDatacenter");

        ContainerVm containerVm = (ContainerVm) ev.getData();
//        containerVm.setCreatingTime(CloudSim.clock());
//        if(containerVm.getId() > WFCConstants.WFC_NUMBER_VMS){
//            Log.printLine("WFCDatacenter收到了动态创建新VM的命令");
//        }
        //allocateHostForVmI()用的是PowerContainerVmAllocationAbstract中的
        //顺利的话这句代码就在host中创建了vm
        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm);
        ContainerHost host = containerVm.getHost();
        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            //创建好之后应该立即通知WFCScheduler，而不是等一会才通知
            //需要修改这行代码
//            if(containerVm.getId() > WFCConstants.WFC_NUMBER_VMS){
//                Log.printLine("WFCDatacenter向WFCScheduler返回了创建VM的反馈");
//            }

            //这里改成了延迟为0
//            send(containerVm.getUserId(), CloudSim.getMinTimeBetweenEvents(), CloudSimTags.VM_CREATE_ACK, data);
            send(containerVm.getUserId(), 0, CloudSimTags.VM_CREATE_ACK, data);
        }

        if (result) {
            getContainerVmList().add(containerVm);

            if (containerVm.isBeingInstantiated()) {
                containerVm.setBeingInstantiated(false);
            }

            containerVm.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(containerVm).getContainerVmScheduler()
                    .getAllocatedMipsForContainerVm(containerVm));
        }

    }

    /**
     * Process the event for a User/Broker who wants to destroy a VM previously created in this
     * PowerDatacenter. This PowerDatacenter may send, upon request, the status back to the
     * User/Broker.
     *
     * @param ev  a Sim_event object
     * @param ack the ack
     * @pre ev != null
     * @post $none
     */
    protected void processVmDestroy(SimEvent ev, boolean ack) {
        ContainerVm containerVm = (ContainerVm) ev.getData();
        containerVm.setDestroyTime(CloudSim.clock());
        totalCost += containerVm.getTotalCost();
        if (WFCConstants.PRINT_COST_CALCULATE) {
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":虚拟机" + containerVm.getId() + "销毁，成本变为：" + totalCost);
        }
        getVmAllocationPolicy().deallocateHostForVm(containerVm);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();
            data[2] = CloudSimTags.TRUE;

            sendNow(containerVm.getUserId(), CloudSimTags.VM_DESTROY_ACK, data);
        }

        getContainerVmList().remove(containerVm);
//        Log.printConcatLine(CloudSim.clock(), "VM", containerVm.getId(), "已经正常销毁");
    }

    /**
     * Process the event for a User/Broker who wants to migrate a VM. This PowerDatacenter will
     * then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processVmMigrate(SimEvent ev, boolean ack) {
        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        ContainerVm containerVm = (ContainerVm) migrate.get("vm");
        ContainerHost host = (ContainerHost) migrate.get("host");

        getVmAllocationPolicy().deallocateHostForVm(containerVm);
        host.removeMigratingInContainerVm(containerVm);
        boolean result = getVmAllocationPolicy().allocateHostForVm(containerVm, host);
        if (!result) {
            Log.printLine("[Datacenter.processVmMigrate] VM allocation to the destination host failed");
            System.exit(0);
        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = containerVm.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(ev.getSource(), CloudSimTags.VM_CREATE_ACK, data);
        }

        Log.formatLine(
                "%.2f: Migration of VM #%d to Host #%d is completed",
                CloudSim.clock(),
                containerVm.getId(),
                host.getId());
        containerVm.setInMigration(false);
    }

    /**
     * Process the event for a User/Broker who wants to migrate a VM. This PowerDatacenter will
     * then send the status back to the User/Broker.
     *
     * @param ev a Sim_event object
     * @pre ev != null
     * @post $none
     */
    protected void processContainerMigrate(SimEvent ev, boolean ack) {

        Object tmp = ev.getData();
        if (!(tmp instanceof Map<?, ?>)) {
            throw new ClassCastException("The data object must be Map<String, Object>");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> migrate = (HashMap<String, Object>) tmp;

        Container container = (Container) migrate.get("container");
        ContainerVm containerVm = (ContainerVm) migrate.get("vm");

        getContainerAllocationPolicy().deallocateVmForContainer(container);
        if (containerVm.getContainersMigratingIn().contains(container)) {
            containerVm.removeMigratingInContainer(container);
        }
        boolean result = getContainerAllocationPolicy().allocateVmForContainer(container, containerVm);
        if (!result) {
            Log.printLine("[Datacenter.processContainerMigrate]Container allocation to the destination vm failed");
            System.exit(0);
        }
        if (containerVm.isInWaiting()) {
            containerVm.setInWaiting(false);

        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = container.getId();

            if (result) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(ev.getSource(), containerCloudSimTags.CONTAINER_CREATE_ACK, data);
        }

        Log.formatLine(
                "%.2f: Migration of container #%d to Vm #%d is completed",
                CloudSim.clock(),
                container.getId(),
                container.getVm().getId());
        container.setInMigration(false);
    }

    /**
     * Processes a Cloudlet based on the event type.
     *
     * @param ev   a Sim_event object
     * @param type event type
     * @pre ev != null
     * @pre type > 0
     * @post $none
     */
    protected void processCloudlet(SimEvent ev, int type) {
        int cloudletId = 0;
        int userId = 0;
        int vmId = 0;
        int containerId = 0;

        try { // if the sender using cloudletXXX() methods
            int data[] = (int[]) ev.getData();
            cloudletId = data[0];
            userId = data[1];
            vmId = data[2];
            containerId = data[3];
        }

        // if the sender using normal send() methods
        catch (ClassCastException c) {
            try {
                ContainerCloudlet cl = (ContainerCloudlet) ev.getData();
                cloudletId = cl.getCloudletId();
                userId = cl.getUserId();
                vmId = cl.getVmId();
                containerId = cl.getContainerId();
            } catch (Exception e) {
                Log.printConcatLine(super.getName(), ": Error in processing Cloudlet");
                Log.printLine(e.getMessage());
                return;
            }
        } catch (Exception e) {
            Log.printConcatLine(super.getName(), ": Error in processing a Cloudlet.");
            Log.printLine(e.getMessage());
            return;
        }

        // begins executing ....
        switch (type) {
            case CloudSimTags.CLOUDLET_CANCEL:
                processCloudletCancel(cloudletId, userId, vmId, containerId);
                break;

            case CloudSimTags.CLOUDLET_PAUSE:
                processCloudletPause(cloudletId, userId, vmId, containerId, false);
                break;

            case CloudSimTags.CLOUDLET_PAUSE_ACK:
                processCloudletPause(cloudletId, userId, vmId, containerId, true);
                break;

            case CloudSimTags.CLOUDLET_RESUME:
                processCloudletResume(cloudletId, userId, vmId, containerId, false);
                break;

            case CloudSimTags.CLOUDLET_RESUME_ACK:
                processCloudletResume(cloudletId, userId, vmId, containerId, true);
                break;
            default:
                break;
        }

    }

    /**
     * Process the event for a User/Broker who wants to move a Cloudlet.
     *
     * @param receivedData information about the migration
     * @param type         event tag
     * @pre receivedData != null
     * @pre type > 0
     * @post $none
     */
    protected void processCloudletMove(int[] receivedData, int type) {
        updateCloudletProcessing();

        int[] array = receivedData;
        int cloudletId = array[0];
        int userId = array[1];
        int vmId = array[2];
        int containerId = array[3];
        int vmDestId = array[4];
        int containerDestId = array[5];
        int destId = array[6];

        // get the cloudlet
        Cloudlet cl = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletCancel(cloudletId);

        boolean failed = false;
        if (cl == null) {// cloudlet doesn't exist
            failed = true;
        } else {
            // has the cloudlet already finished?
            if (cl.getCloudletStatusString().equals("Success")) {// if yes, send it back to user
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cloudletId;
                data[2] = 0;
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
            }

            // prepare cloudlet for migration
            cl.setVmId(vmDestId);

            // the cloudlet will migrate from one vm to another does the destination VM exist?
            if (destId == getId()) {
                ContainerVm containerVm = getVmAllocationPolicy().getHost(vmDestId, userId).getContainerVm(vmDestId, userId);
                if (containerVm == null) {
                    failed = true;
                } else {
                    // time to transfer the files
                    double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());
                    containerVm.getContainer(containerDestId, userId).getContainerCloudletScheduler().cloudletSubmit(cl, fileTransferTime);
                }
            } else {// the cloudlet will migrate from one resource to another
                int tag = ((type == CloudSimTags.CLOUDLET_MOVE_ACK) ? CloudSimTags.CLOUDLET_SUBMIT_ACK
                        : CloudSimTags.CLOUDLET_SUBMIT);
                sendNow(destId, tag, cl);
            }
        }

        if (type == CloudSimTags.CLOUDLET_MOVE_ACK) {// send ACK if requested
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (failed) {
                data[2] = 0;
            } else {
                data[2] = 1;
            }
            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet submission.
     *
     * @param ev  a SimEvent object
     * @param ack an acknowledgement
     * @pre ev != null
     * @post $none
     */
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();
        try {
            Job cl = (Job) ev.getData();
//            Log.printLine(CloudSim.clock() + ":WFCDatacenter: 现在处理任务" + cl.getCloudletId() + "的提交++++++++++");
//            Log.printLine("分配给了容器" + cl.getContainerId() + "++++++++++");

            // checks whether this Cloudlet has finished or not
            if (cl.isFinished()) {
                String name = CloudSim.getEntityName(cl.getUserId());
                Log.printConcatLine(getName(), ": Warning - Cloudlet #", cl.getCloudletId(), " owned by ", name,
                        " is already completed/finished.");
                /*Log.printLine("Therefore, it is not being executed again");
                Log.printLine();*/

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = cl.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(cl.getUserId(), tag, data);
                }

                sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

                return;
            }
            // process this Cloudlet to this CloudResource
            //cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics().getCostPerBw());
            //获取分配给该任务的资源
            int userId = cl.getUserId();
            int vmId = cl.getVmId();
            int containerId = cl.getContainerId();

            ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
            //获得该任务所在的VM
            ContainerVm vm = host.getContainerVm(vmId, userId);
            //有可能此时VM已经宕机了(从已有的同步函数的措施，应该不会此时VM已经正常销毁，但有可能已经宕机)
//            if (null == vm) {
//                return;
//            }

            Container container = vm.getContainer(containerId, userId);
            //计费方式是按照数据中心，还是按照虚拟机
            switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    cl.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }
            //有一个任务是新增的任务，类型为stage in
            //这个任务是什么时候放进来的？WFCEngineClustering.processDatastaging()中添加的
            if (cl.getClassType() == ClassType.STAGE_IN.value) {
                //把需要使用的输入文件加入到WFCReplicaCatalog的storage中
                stageInFile2FileSystem(cl);
            }

            //double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());

            //Scheduler schedulerVm = vm.getContainerScheduler();
            //getContainerCloudletScheduler()获取的是什么？WFCExample中获取的是ContainerCloudletSchedulerDynamicWorkload
            ContainerCloudletScheduler schedulerContainer = container.getContainerCloudletScheduler();
            //将任务添加到execList或waitingList中，并计算估计的完成时间
            //ContainerCloudletSchedulerDynamicWorkload.cloudletSubmit并没有用到fileTransferTime

            double estimatedFinishTime = schedulerContainer.cloudletSubmit(cl, cl.getTransferTime());
            WFCDeadline.wfcDatacenterAllocateJobNum++;

            //设定job中每个task的开始时间和结束时间
            //放在这里不合理，因为有可能job还没有开始运行，就无法设定真正的开始时间
            updateTaskExecTime(cl, vm);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                //这里修改过，把第二个参数从estimatedFinishTime改成了estimatedFinishTime - CloudSim.clock()
                //这样修改之后，才能让结果输出的运行时间对应上dax文件里的任务长度
                send(getId(), estimatedFinishTime - CloudSim.clock(), CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = cl.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                // unique tag = operation tag
                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(cl.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(String.format("%s.processCloudletSubmit(): ClassCastException error.", getName()));
            //by arman I commented log   c.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();           //by arman I commented log
            Log.printLine(String.format("%s.processCloudletSubmit(): Exception error.", getName()));
            Log.print(e.getMessage());
        }

        checkCloudletCompletion();
//        Log.printLine(CloudSim.clock() + ":WFCDatacenter的submit+++++++++++++++++++++++++++++结束");
    }

    /**
     * Predict file transfer time.
     *
     * @param requiredFiles the required files
     * @return the double
     */
    protected double predictFileTransferTime(List<String> requiredFiles) {
        double time = 0.0;

        for (String fileName : requiredFiles) {
            for (int i = 0; i < getStorageList().size(); i++) {
                Storage tempStorage = getStorageList().get(i);
                File tempFile = tempStorage.getFile(fileName);
                if (tempFile != null) {
                    time += tempFile.getSize() / tempStorage.getMaxTransferRate();
                    break;
                }
            }
        }
        return time;
    }

    /**
     * Processes a Cloudlet resume request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param ack        $true if an ack is requested after operation
     * @param vmId       the vm id
     * @pre $none
     * @post $none
     */
    protected void processCloudletResume(int cloudletId, int userId, int vmId, int containerId, boolean ack) {
        double eventTime = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletResume(cloudletId);

        boolean status = false;
        if (eventTime > 0.0) { // if this cloudlet is in the exec queue
            status = true;
            if (eventTime > CloudSim.clock()) {
                schedule(getId(), eventTime, CloudSimTags.VM_DATACENTER_EVENT);
            }
        }

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (status) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(userId, CloudSimTags.CLOUDLET_RESUME_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet pause request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param ack        $true if an ack is requested after operation
     * @param vmId       the vm id
     * @pre $none
     * @post $none
     */
    protected void processCloudletPause(int cloudletId, int userId, int vmId, int containerId, boolean ack) {
        boolean status = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletPause(cloudletId);

        if (ack) {
            int[] data = new int[3];
            data[0] = getId();
            data[1] = cloudletId;
            if (status) {
                data[2] = CloudSimTags.TRUE;
            } else {
                data[2] = CloudSimTags.FALSE;
            }
            sendNow(userId, CloudSimTags.CLOUDLET_PAUSE_ACK, data);
        }
    }

    /**
     * Processes a Cloudlet cancel request.
     *
     * @param cloudletId resuming cloudlet ID
     * @param userId     ID of the cloudlet's owner
     * @param vmId       the vm id
     * @pre $none
     * @post $none
     */
    protected void processCloudletCancel(int cloudletId, int userId, int vmId, int containerId) {
        Cloudlet cl = getVmAllocationPolicy().getHost(vmId, userId).getContainerVm(vmId, userId).getContainer(containerId, userId)
                .getContainerCloudletScheduler().cloudletCancel(cloudletId);
        sendNow(userId, CloudSimTags.CLOUDLET_CANCEL, cl);
    }

    /**
     * Updates processing of each cloudlet running in this PowerDatacenter. It is necessary because
     * Hosts and VirtualMachines are simple objects, not entities. So, they don't receive events and
     * updating cloudlets inside them must be called from the outside.
     * <p>
     * 导致这个函数运行次数多，有两个来源，一是processCloudletSubmit()调用，二是smallerTime决定的事项时间
     *
     * @pre $none
     * @post $none
     */
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        //if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + CloudSim.getMinTimeBetweenEvents()) {

        if(CloudSim.clock() - getLastProcessTime() < WFCConstants.MIN_TIME_BETWEEN_EVENTS){
            SimEvent nextUpdateEvent = getNextEvent(new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
            // 把最小间隔之前的相同事件，全部删掉
            while(null != nextUpdateEvent && nextUpdateEvent.eventTime() - getLastProcessTime() < WFCConstants.MIN_TIME_BETWEEN_EVENTS){
                nextUpdateEvent = getNextEvent(new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
            }
            // 还回去比较晚的，不能删除所有距离近的事件，否则时间表现明显下降
            if(null != nextUpdateEvent){
                schedule(nextUpdateEvent.getDestination(), nextUpdateEvent.eventTime() - CloudSim.clock(), nextUpdateEvent.getTag(), nextUpdateEvent.getData());
            }else{
                schedule(getId(), getLastProcessTime() + WFCConstants.MIN_TIME_BETWEEN_EVENTS + 0.0001 - CloudSim.clock(), CloudSimTags.VM_DATACENTER_EVENT);
            }
//            if(null != nextUpdateEvent){
//                schedule(nextUpdateEvent.getDestination(), nextUpdateEvent.eventTime() - CloudSim.clock(), nextUpdateEvent.getTag(), nextUpdateEvent.getData());
//            }
            return;
        }
        setLastProcessTime(CloudSim.clock());
//        schedule(getId(), WFCConstants.MIN_TIME_BETWEEN_EVENTS + 0.0001, CloudSimTags.VM_DATACENTER_EVENT);

//        // 尝试获取下一次调用processCloudletSubmit()的时间（目前已知的）
//        SimEvent nextEvent = getNextEvent(new PredicateType(CloudSimTags.CLOUDLET_SUBMIT));
//        // getNextEvent()会把event移除，
//        // 而且是processCloudletSubmit()，而不是该函数的，所以取出来还要换回去
//        if (null != nextEvent) {
//            schedule(nextEvent.getDestination(), nextEvent.eventTime() - CloudSim.clock(), nextEvent.getTag(), nextEvent.getData());
//        }
//        // 如果这一时刻已经运行过一次，而且同一时刻，后面还有相同的事件，就跳过
//        // 相当于只运行同一时刻的第一次调用和最后一次调用
//        if (getLastProcessTime() == CloudSim.clock() && null != nextEvent && nextEvent.eventTime() == CloudSim.clock()) {
//            return;
//        }
        if(WFCConstants.PRINT_THREE_FUNCTION){
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":WFCDatacenter的updateCloudletProcessing+++++++++++++++++++++++++++++");
        }
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        double smallerTime = Double.MAX_VALUE;
        // for each host...
        for (int i = 0; i < list.size(); i++) {
            ContainerHost host = list.get(i);
            // inform VMs to update processing
            //获得这个host中下一个事项的发生时间
            double time = host.updateContainerVmsProcessing(CloudSim.clock());
            // what time do we expect that the next cloudlet will finish?
            if (time < smallerTime) {
                smallerTime = time;
            }
        }
        // gurantees a minimal interval before scheduling the event
           /* if (smallerTime < CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01) {
                smallerTime = CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01;
            }*/
        // 如果不加0.0001，有可能这个事件被拒绝掉
        if (smallerTime < getLastProcessTime() + WFCConstants.MIN_TIME_BETWEEN_EVENTS + 0.0001) {
            smallerTime = getLastProcessTime() + WFCConstants.MIN_TIME_BETWEEN_EVENTS + 0.0001;
        }
//            Log.printConcatLine(CloudSim.clock(), ":计算出的下一个事项时间是：", smallerTime);
        if (smallerTime != Double.MAX_VALUE) {
            schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
        }
    }

    /**
     * Verifies if some cloudlet inside this PowerDatacenter already finished. If yes, send it to
     * the User/Broker
     *
     * @pre $none
     * @post $none
     */
    protected void checkCloudletCompletion() {
        List<? extends ContainerHost> list = getVmAllocationPolicy().getContainerHostList();
        for (int i = 0; i < list.size(); i++) {
            ContainerHost host = list.get(i);
            for (ContainerVm vm : host.getVmList()) {
                for (Container container : vm.getContainerList()) {
                    //如果这个container中至少存在一个完成的任务还没有被处理（getNextFinishedCloudlet()会从列表中移除处理过的任务）
                    while (container.getContainerCloudletScheduler().isFinishedCloudlets()) {
                        Cloudlet cl = container.getContainerCloudletScheduler().getNextFinishedCloudlet();

                        //新添加，用来真正更新task的开始时间
                        Job job = (Job) cl;
                        updateTaskExecTime(job, vm);

                        if (cl != null) {
                            sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                            register(cl);//notice me it is important
                        }
                    }
                }
            }
        }
    }

    /**
     * Adds a file into the resource's storage before the experiment starts. If the file is a master
     * file, then it will be registered to the RC when the experiment begins.
     *
     * @param file a DataCloud file
     * @return a tag number denoting whether this operation is a success or not
     */
    public int addFile(File file) {
        if (file == null) {
            return DataCloudTags.FILE_ADD_ERROR_EMPTY;
        }

        if (contains(file.getName())) {
            return DataCloudTags.FILE_ADD_ERROR_EXIST_READ_ONLY;
        }

        // check storage space first
        if (getStorageList().size() <= 0) {
            return DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL;
        }

        Storage tempStorage = null;
        int msg = DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL;

        for (int i = 0; i < getStorageList().size(); i++) {
            tempStorage = getStorageList().get(i);
            if (tempStorage.getAvailableSpace() >= file.getSize()) {
                tempStorage.addFile(file);
                msg = DataCloudTags.FILE_ADD_SUCCESSFUL;
                break;
            }
        }

        return msg;
    }

    /**
     * Checks whether the resource has the given file.
     *
     * @param file a file to be searched
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    protected boolean contains(File file) {
        if (file == null) {
            return false;
        }
        return contains(file.getName());
    }

    /**
     * Checks whether the resource has the given file.
     *
     * @param fileName a file name to be searched
     * @return <tt>true</tt> if successful, <tt>false</tt> otherwise
     */
    protected boolean contains(String fileName) {
        if (fileName == null || fileName.length() == 0) {
            return false;
        }

        Iterator<Storage> it = getStorageList().iterator();
        Storage storage = null;
        boolean result = false;

        while (it.hasNext()) {
            storage = it.next();
            if (storage.contains(fileName)) {
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * Deletes the file from the storage. Also, check whether it is possible to delete the file from
     * the storage.
     *
     * @param fileName the name of the file to be deleted
     * @return the error message
     */
    private int deleteFileFromStorage(String fileName) {
        Storage tempStorage = null;
        File tempFile = null;
        int msg = DataCloudTags.FILE_DELETE_ERROR;

        for (int i = 0; i < getStorageList().size(); i++) {
            tempStorage = getStorageList().get(i);
            tempFile = tempStorage.getFile(fileName);
            tempStorage.deleteFile(fileName, tempFile);
            msg = DataCloudTags.FILE_DELETE_SUCCESSFUL;
        } // end for

        return msg;
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        Log.printConcatLine(getName(), " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     */
    @Override
    public void startEntity() {
        Log.printConcatLine(getName(), " is starting...");
        // this resource should register to regional GIS.
        // However, if not specified, then register to system GIS (the
        // default CloudInformationService) entity.
        int gisID = CloudSim.getEntityId(regionalCisName);
        if (gisID == -1) {
            gisID = CloudSim.getCloudInfoServiceEntityId();
        }

        // send the registration to GIS
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
        // Below method is for a child class to override
        registerOtherEntity();
    }

    /**
     * Gets the host list.
     *
     * @return the host list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerHost> List<T> getHostList() {
        return (List<T>) getCharacteristics().getHostList();
    }

    /**
     * Gets the characteristics.
     *
     * @return the characteristics
     */
    protected ContainerDatacenterCharacteristics getCharacteristics() {
        return characteristics;
    }

    /**
     * Sets the characteristics.
     *
     * @param characteristics the new characteristics
     */
    protected void setCharacteristics(ContainerDatacenterCharacteristics characteristics) {
        this.characteristics = characteristics;
    }

    /**
     * Gets the regional cis name.
     *
     * @return the regional cis name
     */
    protected String getRegionalCisName() {
        return regionalCisName;
    }

    /**
     * Sets the regional cis name.
     *
     * @param regionalCisName the new regional cis name
     */
    protected void setRegionalCisName(String regionalCisName) {
        this.regionalCisName = regionalCisName;
    }

    /**
     * Gets the vm allocation policy.
     *
     * @return the vm allocation policy
     */
    public ContainerVmAllocationPolicy getVmAllocationPolicy() {
        return vmAllocationPolicy;
    }

    /**
     * Sets the vm allocation policy.
     *
     * @param vmAllocationPolicy the new vm allocation policy
     */
    protected void setVmAllocationPolicy(ContainerVmAllocationPolicy vmAllocationPolicy) {
        this.vmAllocationPolicy = vmAllocationPolicy;
    }

    /**
     * Gets the last process time.
     *
     * @return the last process time
     */
    protected double getLastProcessTime() {
        return lastProcessTime;
    }

    /**
     * Sets the last process time.
     *
     * @param lastProcessTime the new last process time
     */
    protected void setLastProcessTime(double lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }

    /**
     * Gets the storage list.
     *
     * @return the storage list
     */
    protected List<Storage> getStorageList() {
        return storageList;
    }

    /**
     * Sets the storage list.
     *
     * @param storageList the new storage list
     */
    protected void setStorageList(List<Storage> storageList) {
        this.storageList = storageList;
    }

    /**
     * Gets the vm list.
     *
     * @return the vm list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerVm> List<T> getContainerVmList() {
        return (List<T>) containerVmList;
    }

    /**
     * Sets the vm list.
     *
     * @param containerVmList the new vm list
     */
    protected <T extends ContainerVm> void setContainerVmList(List<T> containerVmList) {
        this.containerVmList = containerVmList;
    }

    /**
     * Gets the scheduling interval.
     *
     * @return the scheduling interval
     */
    protected double getSchedulingInterval() {
        return schedulingInterval;
    }

    /**
     * Sets the scheduling interval.
     *
     * @param schedulingInterval the new scheduling interval
     */
    protected void setSchedulingInterval(double schedulingInterval) {
        this.schedulingInterval = schedulingInterval;
    }


    public ContainerAllocationPolicy getContainerAllocationPolicy() {
        return containerAllocationPolicy;
    }

    public void setContainerAllocationPolicy(ContainerAllocationPolicy containerAllocationPolicy) {
        this.containerAllocationPolicy = containerAllocationPolicy;
    }


    public <T extends Container> List<T> getContainerList() {
        return (List<T>) containerList;
    }

    public void setContainerList(List<? extends Container> containerList) {
        this.containerList = containerList;
    }


    public String getExperimentName() {
        return experimentName;
    }

    public void setExperimentName(String experimentName) {
        this.experimentName = experimentName;
    }

    public String getLogAddress() {
        return logAddress;
    }

    public void setLogAddress(String logAddress) {
        this.logAddress = logAddress;
    }

    /**
     * Update the submission time/exec time of a job
     *
     * @param job
     * @param vm
     */
    private void updateTaskExecTime(Job job, ContainerVm vm) {
        double start_time = job.getExecStartTime();
        for (Task task : job.getTaskList()) {
            task.setExecStartTime(start_time);
            double task_runtime = task.getCloudletLength() / vm.getMips();
            start_time += task_runtime;
            //Because CloudSim would not let us update end time here
            task.setTaskFinishTime(start_time);
        }
    }

    /**
     * Stage in files for a stage-in job. For a local file system (such as
     * condor-io) add files to the local storage; For a shared file system (such
     * as NFS) add files to the shared storage
     *
     * @param job
     * @pre $none
     * @post $none
     */
    private void stageInFile2FileSystem(Job job) {
        List<FileItem> fList = job.getFileList();
        /*Log.printLine("**WFCDatacenter=>stageInFile2FileSystem**");
        Log.print(job.getDepth());*/
        for (FileItem file : fList) {
                  /*Log.printLine("          file : ");
                  Log.printLine(file);
                  Log.printLine("          getDepth : ");
                  Log.printLine(job.getDepth());*/
            switch (WFCReplicaCatalog.getFileSystem()) {
                /**
                 * For local file system, add it to local storage (data center
                 * name)
                 */
                case LOCAL:

                    WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                    /**
                     * Is it not really needed currently but it is left for
                     * future usage
                     */
                    //ClusterStorage storage = (ClusterStorage) getStorageList().get(0);
                    //storage.addFile(file);
                    break;
                /**
                 * For shared file system, add it to the shared storage
                 */
                case SHARED:
                    WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());

                    break;
                default:
                    break;
            }
        }
    }

    /*
     * Stage in for a single job (both stage-in job and compute job)
     * @param requiredFiles, all files to be stage-in
     * @param job, the job to be processed
     * @pre  $none
     * @post $none
     */
    protected double processDataStageInForComputeJob(List<FileItem> requiredFiles, Job job) throws Exception {
        //Log.printLine("**WFCDatacenter=>processDataStageInForComputeJob**");

        double time = 0.0;
        //requiredFiles包含的是所有输入和输出的文件
        for (FileItem file : requiredFiles) {
                     /*Log.printLine("          file : ");
                     Log.printLine(file);
                     Log.printLine("          getDepth : ");
                     Log.printLine(job.getDepth());*/
            //The input file is not an output File
            //检查这个文件是不是input标签的，并且没有和另一个output标签的文件重名
            if (file.isRealInputFile(requiredFiles)) {
                double maxBwth = 0.0;
                //siteList是什么？
                List siteList = WFCReplicaCatalog.getStorageList(file.getName());
//                Log.printConcatLine(CloudSim.clock(), ":processDataStageInForComputeJob(): 查找文件", file.getName());
                  /*Log.printLine("          siteList : ");
                  Log.printLine(siteList);*/
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
                switch (WFCReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        //stage-in job
                        /**
                         * Picks up the site that is closest
                         */
                        double maxRate = Double.MIN_VALUE;
                        for (Storage storage : getStorageList()) {
                            double rate = storage.getMaxTransferRate();
                            if (rate > maxRate) {
                                maxRate = rate;
                            }
                        }
                        //Storage storage = getStorageList().get(0);
                        time += file.getSize() / (double) Consts.MILLION / maxRate;
                        break;
                    case LOCAL:
                        int vmId = job.getVmId();
                        int userId = job.getUserId();
                        ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
                        ContainerVm vm = host.getContainerVm(vmId, userId);

                        boolean requiredFileStagein = true;
                        for (Iterator it = siteList.iterator(); it.hasNext(); ) {
                            //site is where one replica of this data is located at
                            String site = (String) it.next();
                            if (site.equals(this.getName())) {
                                continue;
                            }
                            /**
                             * This file is already in the local vm and thus it
                             * is no need to transfer
                             */
                            if (site.equals(Integer.toString(vmId))) {
                                requiredFileStagein = false;
                                break;
                            }
                            double bwth;
                            if (site.equals(Parameters.SOURCE)) {
                                //transfers from the source to the VM is limited to the VM bw only
                                bwth = vm.getBw();
                                //bwth = dcStorage.getBaseBandwidth();
                            } else {
                                //transfers between two VMs is limited to both VMs
                                bwth = Math.min(vm.getBw(), getVmAllocationPolicy().getHost(Integer.parseInt(site), userId).getContainerVm(Integer.parseInt(site), userId).getBw());
                                //bwth = dcStorage.getBandwidth(Integer.parseInt(site), vmId);
                            }
                            if (bwth > maxBwth) {
                                maxBwth = bwth;
                            }
                        }
                        if (requiredFileStagein && maxBwth > 0.0) {
                            //file.getSize()的值为大约4222080，单位应该是B
                            //按照下面的算法，如果size单位是字节，maxBwth的单位应该是MB
                            time += file.getSize() / (double) Consts.MILLION / maxBwth;
                        }

                        /**
                         * For the case when storage is too small it is not
                         * handled here
                         */
                        //We should add but since CondorVm has a small capability it often fails
                        //We currently don't use this storage to do anything meaningful. It is left for future. 
                        //condorVm.addLocalFile(file);
                        WFCReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
        return time;
    }

    //新写一个函数，计算输出任务需要的传输时间
    protected double processDataStageOutForComputeJob(List<FileItem> requiredFiles, Job job) throws Exception {
        //Log.printLine("**WFCDatacenter=>processDataStageInForComputeJob**");

        double time = 0.0;
        //requiredFiles包含的是所有输入和输出的文件
        for (FileItem file : requiredFiles) {
                     /*Log.printLine("          file : ");
                     Log.printLine(file);
                     Log.printLine("          getDepth : ");
                     Log.printLine(job.getDepth());*/
            if (file.getType() == Parameters.FileType.OUTPUT) {
                double maxBwth = 0.0;

                switch (WFCReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        /**
                         * Picks up the site that is closest
                         */
                        double maxRate = Double.MIN_VALUE;
                        for (Storage storage : getStorageList()) {
                            double rate = storage.getMaxTransferRate();
                            if (rate > maxRate) {
                                maxRate = rate;
                            }
                        }
                        //Storage storage = getStorageList().get(0);
                        time += file.getSize() / (double) Consts.MILLION / maxRate;
                        break;
                }
            }
        }
        return time;
    }


    private void register(Cloudlet cl) {

        //Log.printLine("**WFCDatacenter=>register**");

        Task tl = (Task) cl;
        List<FileItem> fList = tl.getFileList();
        for (FileItem file : fList) {
            /*Log.printLine("          file : ");
            Log.printLine(file);
            Log.printLine("          task-getDepth : ");
            Log.printLine(tl.getDepth());*/
            if (file.getType() == Parameters.FileType.OUTPUT)//output file
            {
                switch (WFCReplicaCatalog.getFileSystem()) {
                    case SHARED:
//                        Log.printConcatLine(CloudSim.clock(), ":因为新任务", cl.getCloudletId(), "完成，存储了新文件", file.getName());
                        WFCReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                        break;
                    case LOCAL:
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        ContainerHost host = getVmAllocationPolicy().getHost(vmId, userId);
                        /**
                         * Left here for future work
                         */
                        ContainerVm vm = (ContainerVm) host.getContainerVm(vmId, userId);
                        WFCReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
    }
}


