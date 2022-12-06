/**
 * Copyright 2019-2020 University Of Southern California
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfc.core;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.predicates.PredicateType;
import org.wfc.scheduler.ContainerCloudletSchedulerJustToTry;
import org.wfc.scheduler.WFCScheduler;

import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.*;
import org.workflowsim.reclustering.ReclusteringEngine;
import org.workflowsim.utils.Parameters;

/**
 * WFCEngine represents a engine acting on behalf of a user. It hides VM
 * management, as vm creation, submission of cloudlets to this VMs and
 * destruction of VMs.
 *
 * @author Arman Riazi
 * @date March 29, 2020
 * @since WorkflowSim Toolkit 1.0
 */
public final class WFCEngine extends SimEntity {

    /**
     * 新加一个属性，表示当前已经到达了几个工作流
     */
    private int hasArrivedWorkflowNum;

    /**
     * 新加
     */
    private static int failCloudletNum = 0;

    /**
     * The job list.
     */
    protected List<? extends ContainerCloudlet> jobsList;
    /**
     * The job submitted list.
     */
    protected List<? extends ContainerCloudlet> jobsSubmittedList;
    /**
     * The job received list.
     */
    protected List<? extends ContainerCloudlet> jobsReceivedList;
    /**
     * The job submitted.
     */
    protected int jobsSubmitted;

    protected List<? extends ContainerVm> vmList;

    protected List<? extends Container> containerList;

    /**
     * The associated scheduler id*
     */
    private List<Integer> schedulerId;

    private List<WFCScheduler> scheduler;

    /**
     * 自己加一个成员，将不同工作流的任务分开保存
     */
    private List<List<ContainerCloudlet>> jobListByWorkflow;

    /**
     * 自己加一个成员，保存已经提交的任务的id
     */
    protected Set<Integer> jobIDsReceivedSet;

    protected double lastSubmitTime;

    private boolean secondTime;


    /**
     * Created a new WorkflowEngine object.
     *
     * @param name name to be associated with this entity (as required by
     *             Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public WFCEngine(String name) throws Exception {
        this(name, 1);
    }

    public WFCEngine(String name, int schedulers) throws Exception {
        super(name);

        setJobsList(new ArrayList<>());
        setJobsSubmittedList(new ArrayList<>());
        setJobsReceivedList(new ArrayList<>());
        /**
         * 自己添加的成员初始化
         */
        setLastSubmitTime(-1000);
        setJobListByWorkflow(new ArrayList<>());
        setJobIDsReceivedSet(new HashSet<>());
        hasArrivedWorkflowNum = 0;

        jobsSubmitted = 0;
        setSchedulers(new ArrayList<>());
        setSchedulerIds(new ArrayList<>());
        secondTime = false;

        for (int i = 0; i < schedulers; i++) {
            WFCScheduler wfs = new WFCScheduler(name + "_Scheduler_" + i, WFCConstants.OVERBOOKING_FACTOR);

            containerList = createContainerList(wfs.getId(), WFCConstants.WFC_NUMBER_CONTAINER);
            wfs.submitContainerList(containerList);

            vmList = createVmList(wfs.getId(), WFCConstants.WFC_NUMBER_VMS);
            wfs.submitVmList(vmList);

            getSchedulers().add(wfs);
            getSchedulerIds().add(wfs.getId());
            wfs.setWorkflowEngineId(this.getId());
        }
    }

    public void setLastSubmitTime(double lastSubmitTime) {
        this.lastSubmitTime = lastSubmitTime;
    }

    public double getLastSubmitTime() {
        return lastSubmitTime;
    }

    public static List<Container> createContainerList(int brokerId, int containersNumber) {
        LinkedList<Container> list = new LinkedList<>();
        //peList.add(new ContainerPe(0, new CotainerPeProvisionerSimple((double)mips * ratio)));
        //create VMs
        try {
            Container[] containers = new Container[containersNumber];
            for (int i = 0; i < containersNumber; i++) {

                //换成了自己写的Container
                containers[i] = new JustToTryPowerContainer(1, IDs.pollId(Container.class), brokerId, (double) WFCConstants.WFC_CONTAINER_MIPS,
                        WFCConstants.WFC_CONTAINER_PES_NUMBER, WFCConstants.WFC_CONTAINER_RAM,
                        WFCConstants.WFC_CONTAINER_BW, WFCConstants.WFC_CONTAINER_SIZE, WFCConstants.WFC_CONTAINER_VMM,
                        //new ContainerCloudletSchedulerTimeShared(),WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                        //这里的scheduler替换成了自己写的
//                        new ContainerCloudletSchedulerDynamicWorkload(WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                        new ContainerCloudletSchedulerJustToTry(i % WFCConstants.WFC_NUMBER_WORKFLOW + 1, WFCConstants.WFC_CONTAINER_MIPS, WFCConstants.WFC_CONTAINER_PES_NUMBER),
                        WFCConstants.WFC_DC_SCHEDULING_INTERVAL);
                list.add(containers[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return list;
    }

    public static List<ContainerVm> createVmList(int brokerId, int containerVmsNumber) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<ContainerVm> list = new LinkedList<>();
        ArrayList peList = new ArrayList();

        try {
            for (int p = 0; p < WFCConstants.WFC_NUMBER_VM_PES; p++) {
                peList.add(new ContainerPe(p, new CotainerPeProvisionerSimple((double) WFCConstants.WFC_VM_MIPS * WFCConstants.WFC_VM_RATIO)));
            }
            //create VMs
            ContainerVm[] vm = new ContainerVm[containerVmsNumber];

            for (int i = 0; i < containerVmsNumber; i++) {
//                vm[i] = new PowerContainerVm(IDs.pollId(ContainerVm.class), brokerId, WFCConstants.WFC_VM_MIPS, (float)WFCConstants.WFC_VM_RAM,
//                        WFCConstants.WFC_VM_BW, WFCConstants.WFC_VM_SIZE,  WFCConstants.WFC_VM_VMM,
//                        new ContainerSchedulerTimeSharedOverSubscription(peList),
//                        //new ContainerSchedulerTimeSharedOverSubscription(peList),
//                        new ContainerRamProvisionerSimple(WFCConstants.WFC_VM_RAM),
//                        new ContainerBwProvisionerSimple(WFCConstants.WFC_VM_BW), peList,
//                        WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                //把上面的虚拟机替换成了自己写的JustToTryPowerContainerVm
                vm[i] = new JustToTryPowerContainerVm(WFCConstants.DEFAULT_VM_TYPE, IDs.pollId(ContainerVm.class), brokerId, WFCConstants.WFC_VM_MIPS, (float) WFCConstants.WFC_VM_RAM,
                        WFCConstants.WFC_VM_BW, WFCConstants.WFC_VM_SIZE, WFCConstants.WFC_VM_VMM,
                        new ContainerSchedulerTimeSharedOverSubscription(peList),
                        //new ContainerSchedulerTimeSharedOverSubscription(peList),
                        new ContainerRamProvisionerSimple(WFCConstants.WFC_VM_RAM),
                        new ContainerBwProvisionerSimple(WFCConstants.WFC_VM_BW), peList,
                        WFCConstants.WFC_DC_SCHEDULING_INTERVAL);

                       /*new ContainerVm(IDs.pollId(ContainerVm.class), brokerId, (double) mips, (float) ram,
                       bw, size, "Xen", new ContainerSchedulerTimeShared(peList),
                       new ContainerRamProvisionerSimple(ram),
                       new ContainerBwProvisionerSimple(bw), peList);*/

                //new ContainerVm(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
                list.add(vm[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return list;
    }

    /**
     * This method is used to send to the broker the list with virtual machines
     * that must be created.
     *
     * @param list        the list
     * @param schedulerId the scheduler id
     */
    public void submitVmList(List<? extends ContainerVm> list, int schedulerId) {
        getScheduler(schedulerId).submitVmList(list);
    }

    public void submitVmList(List<? extends ContainerVm> list) {
        //bug here, not sure whether we should have different workflow schedulers
        getScheduler(0).submitVmList(list);
        setVmList(list);
    }

    public List<? extends ContainerVm> getAllVmList() {
        if (this.vmList != null && !this.vmList.isEmpty()) {
            return this.vmList;
        } else {
            List list = new ArrayList();
            for (int i = 0; i < getSchedulers().size(); i++) {
                list.addAll(getScheduler(i).getVmList());
            }
            return list;
        }
    }

    /**
     * This method is used to send to the broker the list of cloudlets.
     *
     * @param list the list
     */
    public void submitCloudletList(List<? extends ContainerCloudlet> list) {
        getJobsList().addAll(list);
    }

    /**
     * Processes events available for this Broker.
     *
     * @param ev a SimEvent object
     */
    @Override
    public void processEvent(SimEvent ev) {

        if (WFCConstants.CAN_PRINT_SEQ_LOG)
            Log.printLine("WFEngine=>ProccessEvent()=>ev.getTag():" + ev.getTag());

        switch (ev.getTag()) {
            // Resource characteristics request
            case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
                processResourceCharacteristicsRequest(ev);
                break;
            //this call is from workflow scheduler when all vms are created
            case CloudSimTags.CLOUDLET_SUBMIT:
                if (!WFCDeadline.endSimulation) {
                    submitJobs();
                }
                break;
            case CloudSimTags.CLOUDLET_RETURN:
                processJobReturn(ev);
                break;
            case CloudSimTags.END_OF_SIMULATION:
                shutdownEntity();
                break;
            case WorkflowSimTags.JOB_SUBMIT:
                processJobSubmit(ev);
                break;
            case WorkflowSimTags.NEW_WORKFLOW_ARRIVE:
                processNewWorkflowArrive();
                break;
            case WorkflowSimTags.CLOUDLET_REPLICATE:
                processJobReplicate(ev);
                break;

            default:
                processOtherEvent(ev);
                break;
        }
    }

    /**
     * Process a request for the characteristics of a PowerDatacenter.
     *
     * @param ev a SimEvent object
     */
    protected void processResourceCharacteristicsRequest(SimEvent ev) {
        for (int i = 0; i < getSchedulerIds().size(); i++) {
            schedule(getSchedulerId(i), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
        }
    }

    /**
     * Binds a scheduler with a datacenter.
     *
     * @param datacenterId the data center id
     * @param schedulerId  the scheduler id
     */
    public void bindSchedulerDatacenter(int datacenterId, int schedulerId) {
        getScheduler(schedulerId).bindSchedulerDatacenter(datacenterId);
    }

    /**
     * Binds a datacenter to the default scheduler (id=0)
     *
     * @param datacenterId dataceter Id
     */
    public void bindSchedulerDatacenter(int datacenterId) {
        bindSchedulerDatacenter(datacenterId, 0);
    }

//    /**
//     * Process a submit event
//     *
//     * @param ev a SimEvent object
//     */
//    protected void processJobSubmit(SimEvent ev) {
//        List<? extends ContainerCloudlet> list = (List) ev.getData();
//        setJobsList(list);
////        Log.printLine("所有任务的数量为：" + list.size());
////        Log.printLine("000000000000000000000000000000000000000");
////        Log.printLine("000000000000000000000000000000000000000");
////        Log.printLine("000000000000000000000000000000000000000");
//    }

    /**
     * 添加额外的任务长度
     */
    private void addTransferTime(Job job) {
        double transferTime = 0.0;
        List<FileItem> requiredFiles = job.getFileList();
        for (FileItem file : requiredFiles) {
            if (file.isRealInputFile(requiredFiles)) {
                transferTime += file.getSize() / (double) Consts.MILLION / WFCConstants.WFC_DC_MAX_TRANSFER_RATE;
            } else if (file.getType() == Parameters.FileType.OUTPUT) {
                transferTime += file.getSize() / (double) Consts.MILLION / WFCConstants.WFC_DC_MAX_TRANSFER_RATE;
            }
        }
        job.setTransferTime(transferTime);
    }

    /**
     * Process a submit event
     * 为了能让不同工作流的任务分时间到达，把上面的processJobSubmit()复制下来，重写
     *
     * @param ev a SimEvent object
     */
    protected void processJobSubmit(SimEvent ev) {
        List<Job> list = (List) ev.getData();
        WFCDeadline.numOfAllCloudlets = list.size();
        //除了最后一个stage in任务，其它任务都要计算传输时间，放到任务里
        if (WFCConstants.CONSIDER_TRANSFER_TIME) {
            for (int i = 0; i < list.size() - 1; i++) {
                addTransferTime(list.get(i));
            }
        }

        // 把list中包含的所有工作流的所有任务，拆分为每个工作流
        int pointer = 0;
        for (int i = 0; i < WFCConstants.WFC_NUMBER_WORKFLOW; i++) {
            List<ContainerCloudlet> tmpList = new ArrayList<>();
            for (int j = 0; j < WFCDeadline.cloudletNumOfWorkflow.get(i + 1); j++) {
                tmpList.add(list.get(pointer++));
            }
            getJobListByWorkflow().add(tmpList);
        }
        // 最后有一个stage in的任务，要添加到第一批任务里
        getJobListByWorkflow().get(0).add(list.get(pointer));

        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "安排所有工作流的到达");
        //安排好之后会到达的所有工作流
        for (int i = 0; i < WFCConstants.WFC_NUMBER_WORKFLOW; i++) {
            schedule(this.getId(), WFCConstants.ARRIVAL_TIME[i], WorkflowSimTags.NEW_WORKFLOW_ARRIVE);
            //设定工作流的截止时间（绝对时间点）
            WFCDeadline.getDeadlines().put(i + 1, WFCConstants.ARRIVAL_TIME[i] + WFCConstants.DEADLINES[i] * WFCConstants.DEADLINE_RATE);
        }

        //之后会改变getJobListByWorkflow()里的第一个List的内容，向其中添加其它List中的任务
        //但是没什么影响
//        if (getJobListByWorkflow().size() > 0) {
//            getJobsList().addAll(getJobListByWorkflow().get(0));
//            hasArrivedWorkflowNum++;
//        }

        //原来的代码是下面这行，改成了上面的
//        setJobsList(list);
//        Log.printLine("所有任务的数量为：" + list.size());
//        Log.printLine("000000000000000000000000000000000000000");
//        Log.printLine("000000000000000000000000000000000000000");
//        Log.printLine("000000000000000000000000000000000000000");
    }

    /**
     * 处理复制任务
     */
    public void processJobReplicate(SimEvent ev) {
        List<Job> jobList = (List) ev.getData();
        for (Job job : jobList) {
            int newId = IDs.pollId(Job.class);
            List<Job> newJobList = ReclusteringEngine.process(job, newId);

            for (Job newJob : newJobList) {
                //添加从到主的映射
                WFCReplication.getSlaveToMasterMap().put(newJob, job);

                if (WFCConstants.PRINT_REPLICATE_RETRY) {
                    Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "任务", newJob.getCloudletId(), "是由任务", job.getCloudletId(), "复制而来的");
                }
                //添加主到从的映射
                if (!WFCReplication.getMasterToSlaveMap().containsKey(job)) {
                    Set<Cloudlet> slaves = new HashSet<>();
                    slaves.add(newJob);
                    WFCReplication.getMasterToSlaveMap().put(job, slaves);
                } else {
                    WFCReplication.getMasterToSlaveMap().get(job).add(newJob);
                }

                //添加新任务到起源任务的映射
                Cloudlet origin = job;
                while (WFCReplication.getToOrigin().containsKey(origin)) {
                    origin = WFCReplication.getToOrigin().get(origin);
                }
                WFCReplication.getToOrigin().put(newJob, origin);
                WFCDeadline.numOfAllCloudlets++;
            }

            //原任务要记录下这些新增的副本
//            job.getReplications().addAll(newJobList);
            //准备发给WFCScheduler
            getJobsList().addAll(newJobList);
        }
        sendNow(this.getId(), CloudSimTags.CLOUDLET_SUBMIT, null);
    }

    /**
     * 为了达到动态提交新工作流的目的，写一个新函数，用来在某个设定好的时间，将任务加入jobsList
     */
    protected void processNewWorkflowArrive() {


        if (hasArrivedWorkflowNum < getJobListByWorkflow().size()) {
            getJobsList().addAll(getJobListByWorkflow().get(hasArrivedWorkflowNum));
//            if(WFCConstants.assignedAlgorithm == Parameters.SchedulingAlgorithm.SCDY){
//                updateDeadlineSchedulingDynamic(getJobListByWorkflow().get(hasArrivedWorkflowNum));
//            }
            hasArrivedWorkflowNum++;
            Log.printLine(CloudSim.clock() + ":第" + hasArrivedWorkflowNum + "个新工作流到达了!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            sendNow(this.getId(), CloudSimTags.CLOUDLET_SUBMIT, null);
        }
    }

    /**
     * Process a job return event.
     *
     * @param ev a SimEvent object
     * @pre ev != $null
     * @post $none
     */
    protected void processJobReturn(SimEvent ev) {
//        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ": 调用了WFCEngine的processJobReturn()");
        Job job = (Job) ev.getData();
        if (job.getWorkflowId() == 0 && job.getStatus() == Cloudlet.SUCCESS) {
            WFCDeadline.stageInFinish = true;
        }
        if (job.getStatus() == Cloudlet.FAILED) {
            failCloudletNum++;

            //一个任务即使失败也不一定要重新尝试，还要判断是不是它的主或者它的所有从都失败了
            //如果只看上面的条件，有可能两个副本同时失败，然后决定两个副本各自都重新提交
            //所以还有一个容易忽略的条件：和它一个集团的，没有任何一个任务重新提交
            boolean needRetry = true;
            if (WFCReplication.getMasterToSlaveMap().containsKey(job)) {
                for (Cloudlet cloudlet : WFCReplication.getMasterToSlaveMap().get(job)) {
                    if (cloudlet.getStatus() != Cloudlet.FAILED || WFCReplication.getOldResubmitToNew().containsKey(cloudlet)) {
                        needRetry = false;
                        break;
                    }
                }
            } else if (WFCReplication.getSlaveToMasterMap().containsKey(job)) {
                Cloudlet master = WFCReplication.getSlaveToMasterMap().get(job);
                if (master.getStatus() != Cloudlet.FAILED || WFCReplication.getOldResubmitToNew().containsKey(master)) {
                    needRetry = false;
                }
                for (Cloudlet cloudlet : WFCReplication.getMasterToSlaveMap().get(master)) {
                    if (cloudlet.getStatus() != Cloudlet.FAILED || WFCReplication.getOldResubmitToNew().containsKey(cloudlet)) {
                        needRetry = false;
                        break;
                    }
                }
            }

            if (needRetry) {
                //如果决定重新提交，说明旧任务和旧任务所属的“集团”都已经失败，要把这些任务和它们的后继任务之间的“箭头”删除掉
                if (WFCReplication.getMasterToSlaveMap().containsKey(job)) {
                    for (Task child : job.getChildList()) {
                        child.getParentList().remove(job);
                    }
                    for (Cloudlet cloudlet : WFCReplication.getMasterToSlaveMap().get(job)) {
                        for (Task child : ((Task) cloudlet).getChildList()) {
                            child.getParentList().remove(cloudlet);
                        }
                    }
                } else if (WFCReplication.getSlaveToMasterMap().containsKey(job)) {
                    Task master = (Task) WFCReplication.getSlaveToMasterMap().get(job);
                    for (Task child : master.getChildList()) {
                        child.getParentList().remove(master);
                    }
                    for (Cloudlet cloudlet : WFCReplication.getMasterToSlaveMap().get(master)) {
                        for (Task child : ((Task) cloudlet).getChildList()) {
                            child.getParentList().remove(cloudlet);
                        }
                    }
                }

                //开始创建新任务
                int newId = IDs.pollId(Job.class);
                if (WFCConstants.PRINT_REPLICATE_RETRY) {
                    Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ":任务", job.getCloudletId(), "自己和所有副本都失败了，进行重新提交");
                }
                WFCReplication.retry++;
                List<Job> newJobList = ReclusteringEngine.process(job, newId);
                getJobsList().addAll(newJobList);
                for (Job newJob : newJobList) {
                    if (WFCConstants.PRINT_REPLICATE_RETRY) {
                        Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "任务", newJob.getCloudletId(), "是由任务", job.getCloudletId(), "重新提交而来的");
                    }
                    //记录重新提交的关系
                    WFCReplication.getOldResubmitToNew().put(job, newJob);
                    //记录起源关系
                    Cloudlet origin = job;
                    while (WFCReplication.getToOrigin().containsKey(origin)) {
                        origin = WFCReplication.getToOrigin().get(origin);
                    }
                    WFCReplication.getToOrigin().put(newJob, origin);
                    WFCDeadline.numOfAllCloudlets++;
                }

            }
        }
        //if (!hasJobListContainsID(this.getJobsReceivedList(), job.getCloudletId())) {

        //不管任务是否成功，都会把任务加入
        getJobsReceivedList().add(job);
        getJobIDsReceivedSet().add(job.getCloudletId());
        jobsSubmitted--;

        //为了达到动态提交工作流的目的，这里添加第三个条件
        if (getJobsList().isEmpty() && jobsSubmitted == 0 && hasArrivedWorkflowNum >= WFCConstants.WFC_NUMBER_WORKFLOW) {
            Log.printConcatLine(CloudSim.clock(), ":仿真流程应该到此结束");
            //send msg to all the schedulers
            for (int i = 0; i < getSchedulerIds().size(); i++) {
                sendNow(getSchedulerId(i), CloudSimTags.END_OF_SIMULATION, null);
            }
            WFCDeadline.endSimulation = true;
        } else {
            sendNow(this.getId(), CloudSimTags.CLOUDLET_SUBMIT, null);
        }

    }

    /**
     * Overrides this method when making a new and different type of Broker.
     *
     * @param ev a SimEvent object
     */
    protected void processOtherEvent(SimEvent ev) {
        if (ev == null) {
            Log.printLine(getName() + ".processOtherEvent(): " + "Error - an event is null.");
            return;
        }
        Log.printLine(getName() + ".processOtherEvent(): "
                + "Error - event unknown by this DatacenterBroker.");
    }


    /**
     * Checks whether a job list contains a id
     *
     * @param jobList the job list
     * @param id      the job id
     * @return
     */
    private boolean hasJobListContainsID(List jobList, int id) {
        for (Iterator it = jobList.iterator(); it.hasNext(); ) {
            Job job = (Job) it.next();
            if (job.getCloudletId() == id) {
                return true;
            }
        }
        return false;
    }

    /**
     * 这是优化后的版本
     * Submit jobs to the created VMs.
     *
     * @pre $none
     * @post $none
     */
    protected void submitJobs() {
        //专门处理stage-in任务
        if (!WFCDeadline.stageInFinish) {
            if (getJobsList().size() > 0) {
                List<Job> list = getJobsList();
                int num = list.size();
                Job stageInJob = null;
                for (int i = 0; i < num; i++) {
                    Job job = list.get(i);
                    if (job.getClassType() == Parameters.ClassType.STAGE_IN.value) {
                        stageInJob = job;
                    }
                }
                if (null == stageInJob) {
                    return;
                }
                List<Job> submittedList = new ArrayList<>();
                submittedList.add(stageInJob);
                jobsSubmitted++;
                WFCDeadline.wfcEngineSendJobNum++;
                getJobsSubmittedList().add(stageInJob);
                getJobsList().remove(stageInJob);
                sendNow(this.getSchedulerId(0), CloudSimTags.CLOUDLET_SUBMIT, submittedList);
            }
            return;
        }
        // 在处理本函数前，先把同一时间的返回的任务都处理掉
        while(numEventsWaiting(new PredicateType(CloudSimTags.CLOUDLET_RETURN)) > 0){
            SimEvent returnEvent = getNextEvent(new PredicateType(CloudSimTags.CLOUDLET_RETURN));
            processEvent(returnEvent);
        }
        if (CloudSim.clock() - getLastSubmitTime() < WFCConstants.MIN_TIME_BETWEEN_EVENTS / 2) {
            // 去掉所有多余的CloudSimTags.CLOUDLET_SUBMIT事项
            SimEvent nextEvent = getNextEvent(new PredicateType(CloudSimTags.CLOUDLET_SUBMIT));
            while (null != nextEvent && nextEvent.eventTime() - CloudSim.clock() < WFCConstants.MIN_TIME_BETWEEN_EVENTS / 2) {
                nextEvent = getNextEvent(new PredicateType(CloudSimTags.CLOUDLET_SUBMIT));
            }
            // 把这个时间不一样的nextEvent还回去
            // 最好把所有距离很近的时间都丢弃，否则会无法及时响应processJobReturn()发来的消息
            if (null != nextEvent) {
                schedule(nextEvent.getDestination(), nextEvent.eventTime() - CloudSim.clock(), nextEvent.getTag(), nextEvent.getData());
            }
            return;
        }
        setLastSubmitTime(CloudSim.clock());

        if (WFCConstants.PRINT_THREE_FUNCTION) {
            Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ":调用了WFCEngine的submitJobs()oooooooooooooooooooooooooooo");
        }

        if (WFCConstants.assignedAlgorithm == Parameters.SchedulingAlgorithm.SCDY
                || WFCConstants.assignedAlgorithm == Parameters.SchedulingAlgorithm.FUTURESCDY) {
            updateDeadlineSchedulingDynamic();
        }

        //getJobsList()获取的是已到达，但还没有提交的任务
        List<Job> list = getJobsList();
        //记录每个scheduler对应的要提交的任务
        List<Job> submittedList = new ArrayList<>();
        int num = list.size();
//        WFCReplication.unFinishedWorkflowIds.clear();
        //对于每个需要提交的Job
        for (int i = 0; i < num; i++) {
            //at the beginning
            Job job = list.get(i);
            //Dont use job.isFinished() it is not right
            //如果没有完成该任务
            if (!getJobIDsReceivedSet().contains(job.getCloudletId())) {
                List<Job> parentList = job.getParentList();
                // job的所有前驱任务是否都完成了
                boolean flag = true;
                //查看该任务的所有前驱任务有没有完成，如果不是所有前驱任务完成，该任务不能提交到虚拟机
                for (Job parent : parentList) {
                    //前驱任务如果是复制来的，那么就看看它的所有副本中是否有任务已经完成了
                    //如果有其它副本已经完成，等价于这个任务已经完成了
                    boolean equalFinish = false;
                    if (WFCReplication.getMasterToSlaveMap().containsKey(parent)) {
                        for (Cloudlet slave : WFCReplication.getMasterToSlaveMap().get(parent)) {
                            if (getJobIDsReceivedSet().contains(slave) && slave.getStatus() == Cloudlet.SUCCESS) {
                                equalFinish = true;
                                break;
                            }
                        }
                    } else if (WFCReplication.getSlaveToMasterMap().containsKey(parent)) {
                        Cloudlet master = WFCReplication.getSlaveToMasterMap().get(parent);
                        if (getJobIDsReceivedSet().contains(master.getCloudletId()) && master.getStatus() == Cloudlet.SUCCESS) {
                            equalFinish = true;
                        } else {
                            for (Cloudlet slave : WFCReplication.getMasterToSlaveMap().get(master)) {
                                if (getJobIDsReceivedSet().contains(slave) && slave.getStatus() == Cloudlet.SUCCESS) {
                                    equalFinish = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (!equalFinish && !getJobIDsReceivedSet().contains(parent.getCloudletId())) {
                        flag = false;
                        break;
                    }
                }
                /**
                 * This job's parents have all completed successfully. Should
                 * submit.
                 */
                if (flag) {
                    submittedList.add(job);
                    jobsSubmitted++;
                    WFCDeadline.wfcEngineSendJobNum++;
                    getJobsSubmittedList().add(job);
                    list.remove(job);
                    i--;
                    num--;
                } else {
//                    WFCReplication.unFinishedWorkflowIds.add(job.getWorkflowId());
                }
            }

        }

        if (!submittedList.isEmpty()) {
            sendNow(this.getSchedulerId(0), CloudSimTags.CLOUDLET_SUBMIT, submittedList);
        }
    }

    protected boolean findCpMajor(Job curJob, Job endJob, List<Job> cp, Map<Integer, Double> eftMap,
                                  Set<Job> beforeSubmitJobs, Map<Integer, Double> newDeadlineMap) {
        if (cp.size() > 2 && newDeadlineMap.containsKey(curJob.getCloudletId())) {
            return true;
        }
        List<Job> parentList = curJob.getParentList();
        List<Job> candidateNextJob = new ArrayList<>();
        //找到所有候选的下一个结点
        for (Job parentJob : parentList) {
            if (beforeSubmitJobs.contains(parentJob)) {
                // 不能选择的：1.当前cp中只有一个任务，就不能将newDealineMap包含的作为候选
//                if (cp.size() >= 2 || !endJob.getParentList().contains(parentJob) || !newDeadlineMap.containsKey(parentJob.getCloudletId())) {
//                    //只要没有找到过，就是候选的
//                    candidateNextJob.add(parentJob);
//                }
                if (cp.size() >= 2 || !newDeadlineMap.containsKey(parentJob.getCloudletId())) {
                    //只要没有找到过，就是候选的
                    candidateNextJob.add(parentJob);
                }
            }
        }
//        if (candidateNextJob.contains(startJob)) {
//            cp.add(startJob);
//            return true;
//        }
        if (candidateNextJob.size() > 0) {
            Collections.sort(candidateNextJob, new MyComparator(eftMap));
        }
        for (Job nextJob : candidateNextJob) {
            cp.add(nextJob);
            if (findCpMajor(nextJob, endJob, cp, eftMap, beforeSubmitJobs, newDeadlineMap)) {
                return true;
            }
            cp.remove(nextJob);
        }
        return false;
    }

    /**
     * 自己想的 deadline 分配方法
     * 和 updateDeadlineUsingImbalance 类似
     */
    protected void updateDeadlineMajor() {
//        Log.printConcatLine(CloudSim.clock(), "updateDeadlineMajor开始oooooooooooooooooooooooo");
        // getJobsList()获取的是已到达，但还没有提交的任务
        List<Job> list = getJobsList();
        Set<Job> beforeSubmitJobs = new HashSet<>(list);
        // 每个任务的最早开始时间，数字是距现在多久之后
        Map<Integer, Double> estMap = new HashMap<>();
        // 每个任务的处理时间
        Map<Integer, Double> ptMap = new HashMap<>();
        // 每个任务的最早完成时间，数字是距现在多久之后
        Map<Integer, Double> eftMap = new HashMap<>();
        // 给所有前驱已经完成的任务加哑结点
        Map<Integer, Job> dummyStartJobList = new HashMap<>();
        // 给所有没有后继的任务加哑结点
        Map<Integer, Job> dummyEndJobMap = new HashMap<>();
        // 本轮新计算的deadline结果，数字是距现在多久之后
        Map<Integer, Double> newDeadlineMap = new HashMap<>();
        int dummyId = -1;
        //给工作流中添加哑结点
        for (Job job : list) {
            boolean fakeStart = true;
            List<Job> parentList = job.getParentList();
            for (Job parent : parentList) {
                if (beforeSubmitJobs.contains(parent)) {
                    fakeStart = false;
                    break;
                }
            }
            //如果在待调度任务里，没有它的前驱任务
            if (fakeStart) {
                //如果是fakeStart的任务，而且该工作流没有加过哑结点，就要新增首部的哑结点，并连接
                if (!dummyStartJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                    dummyStartJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getParentList().contains(dummyStartJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyStartJobList.get(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                }
            }
            //如果没有后继，并且没有加过尾部的哑结点
            if (job.getChildList().size() == 0) {
                //如果是没有后继的结点，且该工作流没有加过尾部哑结点，就要新增哑结点，并连接
                if (!dummyEndJobMap.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                    dummyEndJobMap.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getChildList().contains(dummyEndJobMap.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyEndJobMap.get(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                }
            }
        }
        //将哑结点加入分配的集合
        beforeSubmitJobs.addAll(dummyStartJobList.values());
        beforeSubmitJobs.addAll(dummyEndJobMap.values());
        //起始结点和终止的结点的截止时间是设定好的
        for (Job job : dummyStartJobList.values()) {
            newDeadlineMap.put(job.getCloudletId(), 0.0);
//            estMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobMap.values()) {
            //WFCDeadline.getDeadlines().get()获取的是绝对时间点的工作流截止时间，所以要减去当前的时间
            newDeadlineMap.put(job.getCloudletId(), WFCDeadline.getDeadlines().get(job.getWorkflowId()) - CloudSim.clock());
        }

        //计算每个任务的处理时间，除了哑结点
        for (Job job : list) {
            double pt = calculateProcessingTime(job, ContainerVmType.EXTRA_LARGE);
            ptMap.put(job.getCloudletId(), pt);
        }
        //计算哑结点的处理时间
        for (Job job : dummyStartJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobMap.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }

        //计算每个任务最早开始时间，存储的是当前时间多久以后，包括哑结点
        for (Job job : dummyStartJobList.values()) {
            calculateEerliestStartTime(job, estMap, ptMap, beforeSubmitJobs);
        }

        //计算每个任务的最早完成时间，存储的是相对当前时间多久以后
        for (Job job : dummyStartJobList.values()) {
            eftMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobMap.values()) {
            eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
        }
        for (Job job : list) {
            eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
        }

        //寻找关键路径，划分截止时间
        for (int workflowId : dummyStartJobList.keySet()) {
//            divideDeadLineMajor(dummyEndJobMap.get(workflowId), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
            divideDeadLineFastMajor(dummyEndJobMap.get(workflowId), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
        }
        //把dummy结点的连接删除掉
        for (Job job : dummyStartJobList.values()) {
            for (Task child : job.getChildList()) {
                child.getParentList().remove(job);
            }
        }
        for (Job job : dummyEndJobMap.values()) {
            for (Job parent : (List<Job>) job.getParentList()) {
                parent.getChildList().remove(job);
            }
        }

        //把相对的时间转换为绝对的截止时间，储存在任务中
        double miniDeadline = Double.MAX_VALUE;
//        int minCloudlet = -1;
//        int minWorkflow = -1;
        for (Job job : list) {
//            if(newDeadlineMap.get(job.getCloudletId()) >= 0){
//                job.setDeadline(CloudSim.clock() + newDeadlineMap.get(job.getCloudletId()));
//            }else{
//                job.setDeadline(CloudSim.clock());
//            }
            job.setDeadline(CloudSim.clock() + newDeadlineMap.get(job.getCloudletId()));
            if (newDeadlineMap.get(job.getCloudletId()) < miniDeadline) {
                miniDeadline = newDeadlineMap.get(job.getCloudletId());
//                minCloudlet = job.getCloudletId();
//                minWorkflow = job.getWorkflowId();
            }
        }
        WFCDeadline.smallestNewDead = Math.min(WFCDeadline.smallestNewDead, miniDeadline);
//        Log.printConcatLine(CloudSim.clock(), "最小deadline(相对时间)是", miniDeadline);
//        Log.printConcatLine(CloudSim.clock(), "updateDeadlineMajor结束oooooooooooooooooooooooo");
    }

    /**
     * 根据Using Imbalance论文的划分截止时间方法，但没能完全复现
     */
    protected void updateDeadlineUsingImbalance() {
//        Log.printConcatLine(CloudSim.clock(),"开始updateDeadline()");
        // getJobsList()获取的是已到达，但还没有提交的任务
        List<Job> list = getJobsList();
        Set<Job> beforeSubmitJobs = new HashSet<>(list);
        // 每个任务的最早开始时间，数字是距现在多久之后
        Map<Integer, Double> estMap = new HashMap<>();
        // 每个任务的处理时间
        Map<Integer, Double> ptMap = new HashMap<>();
        // 每个任务的最早完成时间，数字是距现在多久之后
        Map<Integer, Double> eftMap = new HashMap<>();
        // 给所有前驱已经完成的任务加哑结点
        Map<Integer, Job> dummyStartJobList = new HashMap<>();
        // 给所有没有后继的任务加哑结点
        Map<Integer, Job> dummyEndJobList = new HashMap<>();
        // 本轮新计算的deadline结果，数字是距现在多久之后
        Map<Integer, Double> newDeadlineMap = new HashMap<>();
        int dummyId = -1;
        //给工作流中添加哑结点
        for (Job job : list) {
            boolean fakeStart = true;
            List<Job> parentList = job.getParentList();
            for (Job parent : parentList) {
                if (beforeSubmitJobs.contains(parent)) {
                    fakeStart = false;
                    break;
                }
            }
            //如果在待调度任务里，没有它的前驱任务
            if (fakeStart) {
                //如果是fakeStart的任务，而且该工作流没有加过哑结点，就要新增首部的哑结点，并连接
                if (!dummyStartJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                    dummyStartJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getParentList().contains(dummyStartJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyStartJobList.get(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                }
            }
            //如果没有后继，并且没有加过尾部的哑结点
            if (job.getChildList().size() == 0) {
                //如果是没有后继的结点，且该工作流没有加过尾部哑结点，就要新增哑结点，并连接
                if (!dummyEndJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                    dummyEndJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getChildList().contains(dummyEndJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyEndJobList.get(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                }
            }
        }
        //将哑结点加入分配的集合
        beforeSubmitJobs.addAll(dummyStartJobList.values());
        beforeSubmitJobs.addAll(dummyEndJobList.values());
        //起始结点和终止的结点的截止时间是设定好的
        for (Job job : dummyStartJobList.values()) {
            newDeadlineMap.put(job.getCloudletId(), 0.0);
//            estMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            //WFCDeadline.getDeadlines().get()获取的是绝对时间点的工作流截止时间，所以要减去当前的时间
            newDeadlineMap.put(job.getCloudletId(), WFCDeadline.getDeadlines().get(job.getWorkflowId()) - CloudSim.clock());
        }

        //计算每个任务的处理时间，除了哑结点
        for (Job job : list) {
            double pt = calculateProcessingTime(job, ContainerVmType.EXTRA_LARGE);
            ptMap.put(job.getCloudletId(), pt);
        }
        //计算哑结点的处理时间
        for (Job job : dummyStartJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }

        //计算每个任务最早开始时间，存储的是当前时间多久以后，包括哑结点
        for (Job job : dummyStartJobList.values()) {
            calculateEerliestStartTime(job, estMap, ptMap, beforeSubmitJobs);
        }

        //计算每个任务的最早完成时间，存储的是相对当前时间多久以后
        for (Job job : dummyStartJobList.values()) {
            eftMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
        }
        for (Job job : list) {
            eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
        }
        //寻找关键路径，划分截止时间
        for (int workflowId : dummyStartJobList.keySet()) {
            divideDeadLineBetween(dummyStartJobList.get(workflowId), dummyEndJobList.get(workflowId), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
        }
        //把dummy结点的连接删除掉
        for (Job job : dummyStartJobList.values()) {
            for (Task child : job.getChildList()) {
                child.getParentList().remove(job);
            }
        }
        for (Job job : dummyEndJobList.values()) {
            for (Job parent : (List<Job>) job.getParentList()) {
                parent.getChildList().remove(job);
            }
        }

        //把相对的时间转换为绝对的截止时间，储存在任务中
        double miniDeadline = Double.MAX_VALUE;
        int minCloudlet = -1;
        int minWorkflow = -1;
        for (Job job : list) {
            job.setDeadline(CloudSim.clock() + newDeadlineMap.get(job.getCloudletId()));
            if (newDeadlineMap.get(job.getCloudletId()) < miniDeadline) {
                miniDeadline = newDeadlineMap.get(job.getCloudletId());
                minCloudlet = job.getCloudletId();
                minWorkflow = job.getWorkflowId();
            }
        }
//        Log.printConcatLine(CloudSim.clock(), ":最小的deadline是 ", miniDeadline, " 任务是", minCloudlet, "属于工作流", minWorkflow);
//        List<Double> resu = new ArrayList<>();
//        for (Job job : list) {
//            if (null != job) {
//                resu.add(job.getDeadline());
//            }
//        }
//        int test = 333;
//        Log.printConcatLine(CloudSim.clock(),"结束updateDeadline()");
    }


    protected void updateDeadlineSchedulingDynamic() {
        // getJobsList()获取的是已到达，但还没有提交的任务
        List<Job> list = getJobsList();
        // 还没有提交给WFCScheduler的任务
        Set<Job> beforeSubmitJobs = new HashSet<>(list);
        // 每个任务的最早开始时间，数字是距现在多久之后
        Map<Integer, Double> estMap = new HashMap<>();
        // 每个任务的处理时间
        Map<Integer, Double> ptMap = new HashMap<>();
        // 加上spare time之后，任务可以有的处理时间
        Map<Integer, Double> ptPlusSpareMap = new HashMap<>();
        // 每个任务的最早完成时间，数字是距现在多久之后
        Map<Integer, Double> eftMap = new HashMap<>();
        Map<Integer, Integer> upwardRankMap = new HashMap<>();
        // 每个工作流中，每个rank的最大处理时间
        Map<Integer, Map<Integer, Double>> workflowRankPt = new HashMap<>();
        // 对于每个工作流，各个rank的最大处理时间之和
        Map<Integer, Double> workflowSumRankPt = new HashMap<>();
        // 给所有前驱已经完成的任务加哑结点
        Map<Integer, Job> dummyStartJobList = new HashMap<>();
        // 给所有没有后继的任务加哑结点
        Map<Integer, Job> dummyEndJobList = new HashMap<>();
        // 本轮新计算的deadline结果，数字是距现在多久之后
        Map<Integer, Double> newDeadlineMap = new HashMap<>();
        // 每个工作流对应的最适合的VM类型的下标
        Map<Integer, Integer> workflowToTypeindex = new HashMap<>();
        // 每个工作流对应的spare time
        Map<Integer, Double> workflowToSpareTime = new HashMap<>();
        // 每个工作流对应的任务总处理时间
        Map<Integer, Double> workflowToSumPt = new HashMap<>();
        int dummyId = -1;
        //给工作流中添加哑结点
        for (Job job : list) {
            // 先记录这个list中包含几个不同的工作流
            if (!workflowToTypeindex.containsKey(job.getWorkflowId())) {
                workflowToTypeindex.put(job.getWorkflowId(), 0);
            }
            boolean fakeStart = true;
            List<Job> parentList = job.getParentList();
            for (Job parent : parentList) {
                if (beforeSubmitJobs.contains(parent)) {
                    fakeStart = false;
                    break;
                }
            }
            //如果在待调度任务里，没有它的前驱任务
            if (fakeStart) {
                //如果是fakeStart的任务，而且该工作流没有加过哑结点，就要新增首部的哑结点，并连接
                if (!dummyStartJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                    dummyStartJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getParentList().contains(dummyStartJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyStartJobList.get(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                }
            }
            //如果没有后继，并且没有加过尾部的哑结点
            if (job.getChildList().size() == 0) {
                //如果是没有后继的结点，且该工作流没有加过尾部哑结点，就要新增哑结点，并连接
                if (!dummyEndJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                    dummyEndJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getChildList().contains(dummyEndJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyEndJobList.get(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                }
            }
        }
        //将哑结点加入分配的集合
        beforeSubmitJobs.addAll(dummyStartJobList.values());
        beforeSubmitJobs.addAll(dummyEndJobList.values());
        //起始结点和终止的结点的截止时间是设定好的
        for (Job job : dummyStartJobList.values()) {
            newDeadlineMap.put(job.getCloudletId(), 0.0);
//            estMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            //WFCDeadline.getDeadlines().get()获取的用绝对时间点表示的工作流截止时间，所以要减去当前的时间
            newDeadlineMap.put(job.getCloudletId(), WFCDeadline.getDeadlines().get(job.getWorkflowId()) - CloudSim.clock());
        }
        //计算哑结点的处理时间
        for (Job job : dummyStartJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }

        // 对每个工作流，先尝试最慢的VM类型，不满足截止时间再换更快一级的
        ContainerVmType[] types = new ContainerVmType[4];
        types[0] = ContainerVmType.SMALL;
        types[1] = ContainerVmType.MEDIUM;
        types[2] = ContainerVmType.LARGE;
        types[3] = ContainerVmType.EXTRA_LARGE;
        for (Map.Entry<Integer, Integer> entry : workflowToTypeindex.entrySet()) {
            Job onlyDummyEndJob = dummyEndJobList.get(entry.getKey());
            int tryTypeIndex = 0;
            do {
                estMap.clear();
                //计算每个任务的处理时间，除了哑结点
                for (Job job : list) {
                    if (job.getWorkflowId() == entry.getKey()) {
                        double pt = calculateProcessingTime(job, types[tryTypeIndex]);
                        ptMap.put(job.getCloudletId(), pt);
                    }
                }
                //计算每个任务最早开始时间，存储的是当前时间多久以后，包括哑结点
                for (Job job : dummyStartJobList.values()) {
                    if (job.getWorkflowId() == entry.getKey()) {
                        calculateEerliestStartTime(job, estMap, ptMap, beforeSubmitJobs);
                    }
                }
                //计算每个任务的最早完成时间，存储的是相对当前时间多久以后
                for (Job job : dummyStartJobList.values()) {
                    if (job.getWorkflowId() == entry.getKey()) {
                        eftMap.put(job.getCloudletId(), 0.0);
                    }
                }
                for (Job job : dummyEndJobList.values()) {
                    if (job.getWorkflowId() == entry.getKey()) {
                        eftMap.put(job.getCloudletId(),
                                estMap.get(job.getCloudletId()) + 0.0);
                    }
                }
                for (Job job : list) {
                    if (job.getWorkflowId() == entry.getKey()) {
                        eftMap.put(job.getCloudletId(),
                                estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
                    }
                }
                tryTypeIndex++;
            } while (eftMap.get(onlyDummyEndJob.getCloudletId()) > newDeadlineMap.get(onlyDummyEndJob.getCloudletId()) && tryTypeIndex < 4);
            tryTypeIndex--;
            entry.setValue(tryTypeIndex);
            double spareTime = newDeadlineMap.get(onlyDummyEndJob.getCloudletId()) - eftMap.get(onlyDummyEndJob.getCloudletId());
            if (spareTime < 0) {
//                Log.printConcatLine(CloudSim.clock(), ":出错了，多余时间为负数");
                spareTime = 0;
            }
            workflowToSpareTime.put(entry.getKey(), spareTime);
            workflowToSumPt.put(entry.getKey(), 0.0);
        }
        for (Job job : list) {
            workflowToSumPt.put(job.getWorkflowId(), workflowToSumPt.get(job.getWorkflowId()) + ptMap.get(job.getCloudletId()));
        }
        // 计算所有任务的upwardRank
        for (Job job : dummyEndJobList.values()) {
            calculateUpwardRank(job, upwardRankMap, beforeSubmitJobs);
        }
        for (Job job : list) {
            if (!workflowRankPt.containsKey(job.getWorkflowId())) {
                Map<Integer, Double> rankToLargestPt = new HashMap<>();
                workflowRankPt.put(job.getWorkflowId(), rankToLargestPt);
            }
            Map<Integer, Double> rankToLargestPt = workflowRankPt.get(job.getWorkflowId());
            // 如果没有记录这个rank的信息
            if (!rankToLargestPt.containsKey(upwardRankMap.get(job.getCloudletId()))) {
                rankToLargestPt.put(upwardRankMap.get(job.getCloudletId()), ptMap.get(job.getCloudletId()));
            } else {
                // 如果记录了这个rank的信息，看看有没有更大的pt
                double curLargestPt = rankToLargestPt.get(upwardRankMap.get(job.getCloudletId()));
                rankToLargestPt.put(upwardRankMap.get(job.getCloudletId()), Math.max(curLargestPt, ptMap.get(job.getCloudletId())));
            }
        }
        // 统计每个工作流，其各rank最大处理时间之和
        for (Map.Entry<Integer, Map<Integer, Double>> entry : workflowRankPt.entrySet()) {
            if (!workflowSumRankPt.containsKey(entry.getKey())) {
                workflowSumRankPt.put(entry.getKey(), 0.0);
            }
            for (Map.Entry<Integer, Double> rankToLargestPt : entry.getValue().entrySet()) {
                workflowSumRankPt.put(entry.getKey(), workflowSumRankPt.get(entry.getKey()) + rankToLargestPt.getValue());
            }
        }
        //ptPlusSpareMap计算哑结点的处理时间
        for (Job job : dummyStartJobList.values()) {
            ptPlusSpareMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            ptPlusSpareMap.put(job.getCloudletId(), 0.0);
        }
        // 计算每个任务的处理时间，除了哑结点
        // 因为每个任务的处理时间不再是用VM处理完的时间，还要加上分得的spareTime
        for (Job job : list) {
            int workflowId = job.getWorkflowId();
            int cloudletId = job.getCloudletId();
            ptPlusSpareMap.put(cloudletId, ptMap.get(cloudletId)
                    + workflowRankPt.get(workflowId).get(upwardRankMap.get(cloudletId)) / workflowSumRankPt.get(workflowId) * workflowToSpareTime.get(workflowId));
        }
        // 根据新的处理时间，重新计算最早开始时间
        estMap.clear();
        for (Job job : dummyStartJobList.values()) {
            calculateEerliestStartTime(job, estMap, ptPlusSpareMap, beforeSubmitJobs);
        }
        // 计算除了哑结点的最早完成时间
        for (Job job : list) {
            eftMap.put(job.getCloudletId(),
                    estMap.get(job.getCloudletId()) + ptPlusSpareMap.get(job.getCloudletId()));
        }
//        for (Job job : list) {
//            newDeadlineMap.put(job.getCloudletId(),
//                    eftMap.get(job.getCloudletId())
//                            + ptMap.get(job.getCloudletId()) / workflowToSumPt.get(job.getWorkflowId()) * workflowToSpareTime.get(job.getWorkflowId()));
//        }
        for (Job job : list) {
            newDeadlineMap.put(job.getCloudletId(), eftMap.get(job.getCloudletId()));
        }
        // 把相对的时间转换为绝对的截止时间，储存在任务中
        for (Job job : list) {
            job.setDeadline(CloudSim.clock() + newDeadlineMap.get(job.getCloudletId()));
        }
        //把dummy结点的连接删除掉
        for (Job job : dummyStartJobList.values()) {
            for (Task child : job.getChildList()) {
                child.getParentList().remove(job);
            }
        }
        for (Job job : dummyEndJobList.values()) {
            for (Job parent : (List<Job>) job.getParentList()) {
                parent.getChildList().remove(job);
            }
        }
    }

    private void calculateUpwardRank(Task job, Map<Integer, Integer> upwardRankMap, Set<Job> beforeSubmitJobs) {
        if (upwardRankMap.containsKey(job.getCloudletId())) {
            return;
        }
        int upwardRank = 1;
        boolean terminate = false;
        List<Task> childList = job.getChildList();
        for (Task child : childList) {
            if (!beforeSubmitJobs.contains(child)) {
                continue;
            }
            if (!upwardRankMap.containsKey(child.getCloudletId())) {
                terminate = true;
                break;
            }
            upwardRank = Math.max(upwardRank, upwardRankMap.get(child.getCloudletId()) + 1);
        }
        if (terminate) {
            return;
        }
        upwardRankMap.put(job.getCloudletId(), upwardRank);
        List<Task> parentList = job.getParentList();
        for (Task parent : parentList) {
            calculateUpwardRank(parent, upwardRankMap, beforeSubmitJobs);
        }
    }

    /**
     * 根据Scheduling Dynamic论文中写的划分截止时间的方法(指为一个工作流划分截止时间，这个不再使用)
     *
     * @deprecated 因为SchedulingDynamic论文里要求每次有任务完成都要更新deadline，所以这个不再使用
     */
    protected void originalUpdateDeadlineSchedulingDynamic(List<ContainerCloudlet> cloudletList) {
        if (null == cloudletList || 0 == cloudletList.size()) {
            return;
        }
        List<Job> list = new ArrayList<>();
        for (ContainerCloudlet cloudlet : cloudletList) {
            list.add((Job) cloudlet);
        }
        Set<Job> beforeSubmitJobs = new HashSet<>(list);
        // 每个任务的最早开始时间，数字是距现在多久之后
        Map<Integer, Double> estMap = new HashMap<>();
        // 每个任务的处理时间
        Map<Integer, Double> ptMap = new HashMap<>();
        // 每个任务的最早完成时间，数字是距现在多久之后
        Map<Integer, Double> eftMap = new HashMap<>();
        // 给所有前驱已经完成的任务加哑结点
        Map<Integer, Job> dummyStartJobList = new HashMap<>();
        // 给所有没有后继的任务加哑结点
        Map<Integer, Job> dummyEndJobList = new HashMap<>();
        // 本轮新计算的deadline结果，数字是距现在多久之后
        Map<Integer, Double> newDeadlineMap = new HashMap<>();
        // 这个函数处理的任务都是同一个工作流的，因此应该有唯一的dummyEndJob
        Job onlyDummyEndJob = null;
        int dummyId = -1;
        //给工作流中添加哑结点
        for (Job job : list) {
            boolean fakeStart = true;
            List<Job> parentList = job.getParentList();
            for (Job parent : parentList) {
                if (beforeSubmitJobs.contains(parent)) {
                    fakeStart = false;
                    break;
                }
            }
            //如果在待调度任务里，没有它的前驱任务
            if (fakeStart) {
                //如果是fakeStart的任务，而且该工作流没有加过哑结点，就要新增首部的哑结点，并连接
                if (!dummyStartJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                    dummyStartJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getParentList().contains(dummyStartJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyStartJobList.get(job.getWorkflowId());
                    dummyJob.getChildList().add(job);
                    job.getParentList().add(dummyJob);
                }
            }
            //如果没有后继，并且没有加过尾部的哑结点
            if (job.getChildList().size() == 0) {
                //如果是没有后继的结点，且该工作流没有加过尾部哑结点，就要新增哑结点，并连接
                if (!dummyEndJobList.containsKey(job.getWorkflowId())) {
                    Job dummyJob = new Job(dummyId--, 0);
                    dummyJob.setWorkflowId(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                    dummyEndJobList.put(job.getWorkflowId(), dummyJob);
                } else if (!job.getChildList().contains(dummyEndJobList.get(job.getWorkflowId()))) {
                    //有可能该工作流加过哑结点，但是该任务没有和哑结点连接
                    Job dummyJob = dummyEndJobList.get(job.getWorkflowId());
                    dummyJob.getParentList().add(job);
                    job.getChildList().add(dummyJob);
                }
            }
        }
        // 这个函数处理的任务都是同一个工作流的，因此应该有唯一的dummyEndJob
        for (Job job : dummyEndJobList.values()) {
            onlyDummyEndJob = job;
        }
        //将哑结点加入分配的集合
        beforeSubmitJobs.addAll(dummyStartJobList.values());
        beforeSubmitJobs.addAll(dummyEndJobList.values());
        //起始结点和终止的结点的截止时间是设定好的
        for (Job job : dummyStartJobList.values()) {
            newDeadlineMap.put(job.getCloudletId(), 0.0);
//            estMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            //WFCDeadline.getDeadlines().get()获取的用绝对时间点表示的工作流截止时间，所以要减去当前的时间
            newDeadlineMap.put(job.getCloudletId(), WFCDeadline.getDeadlines().get(job.getWorkflowId()) - CloudSim.clock());
        }
        //计算哑结点的处理时间
        for (Job job : dummyStartJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }
        for (Job job : dummyEndJobList.values()) {
            ptMap.put(job.getCloudletId(), 0.0);
        }

        // 先尝试最慢的VM类型，不满足截止时间再换更快一级的
        ContainerVmType[] types = new ContainerVmType[4];
        types[0] = ContainerVmType.SMALL;
        types[1] = ContainerVmType.MEDIUM;
        types[2] = ContainerVmType.LARGE;
        types[3] = ContainerVmType.EXTRA_LARGE;
        int tryTypeIndex = 0;
        do {
            estMap.clear();
            //计算每个任务的处理时间，除了哑结点
            for (Job job : list) {
                double pt = calculateProcessingTime(job, types[tryTypeIndex]);
                ptMap.put(job.getCloudletId(), pt);
            }
            //计算每个任务最早开始时间，存储的是当前时间多久以后，包括哑结点
            for (Job job : dummyStartJobList.values()) {
                calculateEerliestStartTime(job, estMap, ptMap, beforeSubmitJobs);
            }
            //计算每个任务的最早完成时间，存储的是相对当前时间多久以后
            for (Job job : dummyStartJobList.values()) {
                eftMap.put(job.getCloudletId(), 0.0);
            }
            for (Job job : dummyEndJobList.values()) {
                eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
            }
            for (Job job : list) {
                eftMap.put(job.getCloudletId(), estMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()));
            }
            tryTypeIndex++;
        } while (eftMap.get(onlyDummyEndJob.getCloudletId()) > newDeadlineMap.get(onlyDummyEndJob.getCloudletId()) && tryTypeIndex < 4);
        tryTypeIndex--;

        double spareTime = newDeadlineMap.get(onlyDummyEndJob.getCloudletId()) - eftMap.get(onlyDummyEndJob.getCloudletId());
        if (spareTime < 0) {
//            Log.printConcatLine(CloudSim.clock(), ":出错了，多余时间为负数");
            spareTime = -spareTime;
        }
        double sumPt = 0;
        for (Job job : list) {
            sumPt += ptMap.get(job.getCloudletId());
        }
        for (Job job : list) {
            newDeadlineMap.put(job.getCloudletId(), eftMap.get(job.getCloudletId()) + ptMap.get(job.getCloudletId()) / sumPt * spareTime);
        }
        // 把相对的时间转换为绝对的截止时间，储存在任务中
        for (Job job : list) {
            job.setDeadline(CloudSim.clock() + newDeadlineMap.get(job.getCloudletId()));
        }
        //把dummy结点的连接删除掉
        for (Job job : dummyStartJobList.values()) {
            for (Task child : job.getChildList()) {
                child.getParentList().remove(job);
            }
        }
        for (Job job : dummyEndJobList.values()) {
            for (Job parent : (List<Job>) job.getParentList()) {
                parent.getChildList().remove(job);
            }
        }
    }

    /**
     * 自己想的deadline分配方法
     * 最重要的不同点在于：在搜索关键路径的时候不指定目标路径的起点，只要路径延伸到了某个被 newDeadlineMap包含的结点，就停止搜索
     * 这种深度优先的划分方法不太合理
     *
     * @param endJob
     * @param eftMap
     * @param ptMap
     * @param newDeadlineMap
     * @param beforeSubmitJobs
     */
    public void divideDeadLineMajor(Job endJob, Map<Integer, Double> eftMap, Map<Integer, Double> ptMap,
                                    Map<Integer, Double> newDeadlineMap, Set<Job> beforeSubmitJobs) {
        if (newDeadlineMap.size() >= beforeSubmitJobs.size()) {
            return;
        }
        //储存关键路径
        List<Job> cp = new ArrayList<>();
        cp.add(endJob);
        Job curJob = endJob;
        // 如果找不到一条长度至少为3，且除了两端为newDeadlineMap包含的任务，其他任务都不包含在里面的，就停止
        if (!findCpMajor(curJob, endJob, cp, eftMap, beforeSubmitJobs, newDeadlineMap)) {
            return;
        }
//        List<Integer> res = new ArrayList<>();
//        for (Job job : cp) {
//            if (null != job) {
//                res.add(job.getCloudletId());
//            }
//        }
        //cp一开始是倒序，反转为正序
        Collections.reverse(cp);
        //找到的关键路径至少有3个任务，并且只有第一个任务和最后一个任务包含在newDeadlineMap中，才需要计算
        if (cp.size() > 2) {
            Job startJob = cp.get(0);
            //划分截止时间
            double sumPt = 0;
            //第一个任务不算在内
            for (int i = 1; i < cp.size(); i++) {
                sumPt += ptMap.get(cp.get(i).getCloudletId());
            }
            double cpDeadline = newDeadlineMap.get(endJob.getCloudletId()) - newDeadlineMap.get(startJob.getCloudletId());
            for (int i = 1; i < cp.size() - 1; i++) {
                newDeadlineMap.put(cp.get(i).getCloudletId(),
                        ptMap.get(cp.get(i).getCloudletId()) / sumPt * cpDeadline + newDeadlineMap.get(cp.get(i - 1).getCloudletId()));
            }
            for (int i = cp.size() - 1; i >= 1; i--) {
                divideDeadLineMajor(cp.get(i), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
            }
        }
    }

    /**
     * 自己想的deadline分配方法，和Using Imbalance的论文中的划分方法
     * 最重要的不同点在于：在搜索关键路径的时候不指定目标路径的起点，只要路径延伸到了某个被 newDeadlineMap包含的结点，就停止搜索
     * 与 divideDeadLineMajor()的不同在于，使用宽度优先的搜索关键路径的方式，这样可能划分的合理性更强
     */
    public void divideDeadLineFastMajor(Job endJob, Map<Integer, Double> eftMap, Map<Integer, Double> ptMap,
                                        Map<Integer, Double> newDeadlineMap, Set<Job> beforeSubmitJobs) {
        Queue<Job> toSearchFrom = new LinkedList<>();
        boolean firstRound = true;
        toSearchFrom.offer(endJob);
        while (!toSearchFrom.isEmpty() && newDeadlineMap.size() < beforeSubmitJobs.size()) {
            Job job = toSearchFrom.poll();
            //储存关键路径
            List<Job> cp = new ArrayList<>();
            cp.add(job);
            // 如果找不到一条长度至少为3，且除了两端为newDeadlineMap包含的任务，其他任务都不包含在里面的，就停止
            if (!findCpMajor(job, job, cp, eftMap, beforeSubmitJobs, newDeadlineMap)) {
                continue;
            }
            //cp一开始是倒序，反转为正序
            Collections.reverse(cp);
            //找到的关键路径至少有3个任务，并且只有第一个任务和最后一个任务包含在newDeadlineMap中，才需要计算
            if (cp.size() > 2) {
                if(firstRound){
                    for(Job criticalJob : cp){
                        criticalJob.setCritical(true);
                    }
                    firstRound = false;
                }
                Job startJob = cp.get(0);
                //划分截止时间
                double sumPt = 0;
                //第一个任务不算在内
                for (int i = 1; i < cp.size(); i++) {
                    sumPt += ptMap.get(cp.get(i).getCloudletId());
                }
                double cpDeadline = newDeadlineMap.get(job.getCloudletId()) - newDeadlineMap.get(startJob.getCloudletId());
                for (int i = 1; i < cp.size() - 1; i++) {
                    newDeadlineMap.put(cp.get(i).getCloudletId(),
                            ptMap.get(cp.get(i).getCloudletId()) / sumPt * cpDeadline + newDeadlineMap.get(cp.get(i - 1).getCloudletId()));
                }
                for (int i = cp.size() - 1; i >= 1; i--) {
                    toSearchFrom.offer(cp.get(i));
//                    divideDeadLineFastMajor(cp.get(i), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
                }
            }
        }
    }

    /**
     * 已经不再使用
     * 找到从startJob到endJob的所有路径上的任务的deadline
     * 不一定能找到一条startJob到endJob的路径
     *
     * @param startJob
     * @param endJob
     * @param eftMap
     * @param ptMap
     * @param newDeadlineMap
     * @param beforeSubmitJobs
     */
    public void divideDeadLineBetween(Job startJob, Job endJob, Map<Integer, Double> eftMap, Map<Integer, Double> ptMap,
                                      Map<Integer, Double> newDeadlineMap, Set<Job> beforeSubmitJobs) {
        if (newDeadlineMap.size() >= beforeSubmitJobs.size()) {
            return;
        }
        //储存关键路径
        List<Job> cp = new ArrayList<>();
        cp.add(endJob);
        Job curJob = endJob;
        //如果不能找到一条从endJob到startJob的比较紧凑的路径，就停止
        if (!findCp(curJob, startJob, endJob, cp, eftMap, beforeSubmitJobs, newDeadlineMap)) {
            return;
        }
//        List<Integer> res = new ArrayList<>();
//        for (Job job : cp) {
//            if (null != job) {
//                res.add(job.getCloudletId());
//            }
//        }
        //cp一开始是倒序，反转为正序
        Collections.reverse(cp);
        //找到的关键路径至少有3个任务，并且cp中最后一个任务是startJob，才需要计算
        if (cp.size() > 2 && cp.get(0) == startJob) {
            //划分截止时间
            double sumPt = 0;
            //第一个任务不算在内
            for (int i = 1; i < cp.size(); i++) {
                sumPt += ptMap.get(cp.get(i).getCloudletId());
            }
            double cpDeadline = newDeadlineMap.get(endJob.getCloudletId()) - newDeadlineMap.get(startJob.getCloudletId());
            for (int i = 1; i < cp.size() - 1; i++) {
                newDeadlineMap.put(cp.get(i).getCloudletId(),
                        ptMap.get(cp.get(i).getCloudletId()) / sumPt * cpDeadline + newDeadlineMap.get(cp.get(i - 1).getCloudletId()));
            }
            for (int i = 0; i < cp.size() - 1; i++) {
                for (int j = i + 2; j < cp.size(); j++) {
                    divideDeadLineBetween(cp.get(i), cp.get(j), eftMap, ptMap, newDeadlineMap, beforeSubmitJobs);
                }
            }
        }
    }

    public boolean findCp(Job curJob, Job startJob, Job endJob, List<Job> cp, Map<Integer, Double> eftMap,
                          Set<Job> beforeSubmitJobs, Map<Integer, Double> newDeadlineMap) {
        if (curJob == startJob) {
            return true;
        }
        List<Job> parentList = curJob.getParentList();
        List<Job> candidateNextJob = new ArrayList<>();
        //找到所有候选的下一个结点
        for (Job parentJob : parentList) {
            if (beforeSubmitJobs.contains(parentJob)) {
                if (cp.size() >= 2 || !endJob.getParentList().contains(parentJob) || !newDeadlineMap.containsKey(parentJob.getCloudletId())) {
                    //只要没有找到过，就是候选的
                    candidateNextJob.add(parentJob);
                }
            }
        }
        if (candidateNextJob.contains(startJob)) {
            cp.add(startJob);
            return true;
        }
        if (candidateNextJob.size() > 0) {
            Collections.sort(candidateNextJob, new MyComparator(eftMap));
        }
        for (Job nextJob : candidateNextJob) {
            cp.add(nextJob);
            if (findCp(nextJob, startJob, endJob, cp, eftMap, beforeSubmitJobs, newDeadlineMap)) {
                return true;
            }
            cp.remove(nextJob);
        }
        return false;
    }

    class MyComparator implements Comparator<Job> {
        private Map<Integer, Double> eftMap;

        MyComparator(Map<Integer, Double> eftMap) {
            this.eftMap = eftMap;
        }

        @Override
        public int compare(Job o1, Job o2) {
            if (eftMap.get(o1.getCloudletId()) - eftMap.get(o2.getCloudletId()) < 0) {
                return 1;
            } else if (eftMap.get(o1.getCloudletId()) - eftMap.get(o2.getCloudletId()) == 0) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    public void calculateEerliestStartTime(Job job, Map<Integer, Double> estMap, Map<Integer, Double> ptMap, Set<Job> beforeSubmitJobs) {
        //如果计算过，就不需要再计算了
        if (estMap.containsKey(job.getCloudletId())) {
            return;
        }
//        if(dummyStartJobList.containsKey(job.getCloudletId())){
//            estMap.put(job.getCloudletId(), 0);
//        }
        List<Job> parentList = job.getParentList();
        double earliestFinishTime = 0.0;
        for (Job parent : parentList) {
            if (!beforeSubmitJobs.contains(parent)) {
                continue;
            }
            //如果该前驱没有计算过
            if (!estMap.containsKey(parent.getCloudletId())) {
                calculateEerliestStartTime(parent, estMap, ptMap, beforeSubmitJobs);
            }
            earliestFinishTime = Math.max(earliestFinishTime, estMap.get(parent.getCloudletId()) + ptMap.get(parent.getCloudletId()));
        }
        estMap.put(job.getCloudletId(), earliestFinishTime);
        List<Task> childList = job.getChildList();
        for (Task child : childList) {
            calculateEerliestStartTime((Job) child, estMap, ptMap, beforeSubmitJobs);
        }
    }

    //在寻找关键路径时，估计的处理时间
    public double calculateProcessingTime(Job job, ContainerVmType type) {
//        List<FileItem> fileList = job.getFileList();
//        //传输时间
//        double transferTime = 0.0;
//        for (FileItem file : fileList) {
//            if (file.isRealInputFile(fileList) || file.getType() == Parameters.FileType.OUTPUT) {
//                transferTime += file.getSize() / Consts.MILLION / WFCConstants.WFC_DC_MAX_TRANSFER_RATE;
//            }
//        }
//        //这里计算时间使用的是最快VM的mips
//        return transferTime + (double) job.getCloudletLength() / type.getMips();
        return job.getTransferTime() + (double) job.getCloudletLength() / type.getMips();
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#shutdownEntity()
     */
    @Override
    public void shutdownEntity() {
        Log.printLine(getName() + " is shutting down...");
    }

    /*
     * (non-Javadoc)
     * @see cloudsim.core.SimEntity#startEntity()
     * Here we creata a message when it is started
     */
    @Override
    public void startEntity() {
        Log.printLine(getName() + " is starting...");
        schedule(getId(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST);
    }

    /**
     * Gets the job list.
     *
     * @param <T> the generic type
     * @return the job list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsList() {
        return (List<T>) jobsList;
    }

    /**
     * Sets the job list.
     */
    private <T extends ContainerCloudlet> void setJobsList(List<T> jobsList) {
        this.jobsList = jobsList;
    }

    /**
     * Gets the job submitted list.
     *
     * @param <T> the generic type
     * @return the job submitted list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsSubmittedList() {
        return (List<T>) jobsSubmittedList;
    }

    /**
     * Sets the job submitted list.
     *
     * @param <T>               the generic type
     * @param jobsSubmittedList the new job submitted list
     */
    private <T extends ContainerCloudlet> void setJobsSubmittedList(List<T> jobsSubmittedList) {
        this.jobsSubmittedList = jobsSubmittedList;
    }

    /**
     * Gets the job received list.
     *
     * @param <T> the generic type
     * @return the job received list
     */
    @SuppressWarnings("unchecked")
    public <T extends ContainerCloudlet> List<T> getJobsReceivedList() {
        return (List<T>) jobsReceivedList;
    }

    /**
     * Sets the job received list.
     */
    private <T extends ContainerCloudlet> void setJobsReceivedList(List<T> jobsReceivedList) {
        this.jobsReceivedList = jobsReceivedList;
    }

    /**
     * 用来设定jobsReceivedSet
     */
    private void setJobIDsReceivedSet(Set<Integer> jobIDsReceivedSet) {
        this.jobIDsReceivedSet = jobIDsReceivedSet;
    }

    /**
     * 用来设定jobsReceivedSet
     */
    private Set<Integer> getJobIDsReceivedSet() {
        return this.jobIDsReceivedSet;
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
    private <T extends ContainerVm> void setVmList(List<T> vmList) {
        this.vmList = vmList;
    }

    /**
     * Gets the schedulers.
     *
     * @return the schedulers
     */
    public List<WFCScheduler> getSchedulers() {
        return this.scheduler;
    }

    /**
     * Sets the scheduler list.
     */
    private void setSchedulers(List list) {
        this.scheduler = list;
    }

    /**
     * Gets the scheduler id.
     *
     * @return the scheduler id
     */
    public List<Integer> getSchedulerIds() {
        return this.schedulerId;
    }

    /**
     * Sets the scheduler id list.
     */
    private void setSchedulerIds(List list) {
        this.schedulerId = list;
    }

    /**
     * Gets the scheduler id list.
     *
     * @param index
     * @return the scheduler id list
     */
    public int getSchedulerId(int index) {
        if (this.schedulerId != null) {
            return this.schedulerId.get(index);
        }
        return 0;
    }

    /**
     * Gets the scheduler .
     *
     * @param schedulerId
     * @return the scheduler
     */
    public WFCScheduler getScheduler(int schedulerId) {
        if (this.scheduler != null) {
            return this.scheduler.get(schedulerId);
        }
        return null;
    }

    /**
     * 自己添加set和get方法
     */
    public List<List<ContainerCloudlet>> getJobListByWorkflow() {
        return jobListByWorkflow;
    }

    public void setJobListByWorkflow(List<List<ContainerCloudlet>> list) {
        jobListByWorkflow = list;
    }
}
