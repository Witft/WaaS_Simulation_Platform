/**
 * Copyright 2019-2020 ArmanRiazi
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
package org.wfc.examples;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerPe;
import org.cloudbus.cloudsim.container.containerProvisioners.ContainerRamProvisionerSimple;
import org.cloudbus.cloudsim.container.containerProvisioners.CotainerPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmBwProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPe;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmPeProvisionerSimple;
import org.cloudbus.cloudsim.container.containerVmProvisioners.ContainerVmRamProvisionerSimple;
import org.cloudbus.cloudsim.container.core.*;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicy;
import org.cloudbus.cloudsim.container.hostSelectionPolicies.HostSelectionPolicyFirstFit;
import org.cloudbus.cloudsim.container.resourceAllocatorMigrationEnabled.PowerContainerVmAllocationPolicyMigrationAbstractHostSelection;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.ContainerVmAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicy;
import org.cloudbus.cloudsim.container.resourceAllocators.PowerContainerAllocationPolicySimple;
import org.cloudbus.cloudsim.container.schedulers.ContainerSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.schedulers.ContainerVmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.container.utils.IDs;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicy;
import org.cloudbus.cloudsim.container.vmSelectionPolicies.PowerContainerVmSelectionPolicyMaximumUsage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.util.Conversion;
import org.wfc.core.*;
import org.workflowsim.Job;
import org.workflowsim.JustToTryPowerContainerHost;
import org.wfc.core.WFCEngine;
import org.workflowsim.WFCPlanner;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.failure.FailureMonitor;
import org.workflowsim.failure.FailureParameters;
import org.workflowsim.utils.*;
import org.workflowsim.utils.Parameters.ClassType;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.text.DecimalFormat;
import java.util.*;

/*
 * @author Arman Riazi
 * @since WFC Toolkit 1.0
 * @date March 29, 2020
 */
/*
ConstantsExamples.WFC_DC_SCHEDULING_INTERVAL+ 0.1D

On Vm (
    Allocation= PowerContainerVmAllocationPolicyMigrationAbstractHostSelection
    Scheduler = ContainerVmSchedulerTimeSharedOverSubscription    
    SelectionPolicy = PowerContainerVmSelectionPolicyMaximumUsage
    Pe = CotainerPeProvisionerSimple
    Overhead = 0
    ClusteringMethod.NONE
    SchedulingAlgorithm.MINMIN
    PlanningAlgorithm.INVALID
    FileSystem.LOCAL
)

On Host (
    Scheduler = ContainerVmSchedulerTimeSharedOverSubscription    
    SelectionPolicy = HostSelectionPolicyFirstFit
    Pe = PeProvisionerSimple 
)

On Container (
    Allocation = PowerContainerAllocationPolicySimple
    Scheduler = ContainerCloudletSchedulerDynamicWorkload 
    UtilizationModelFull
)
*/

/**
 * 这个文件由MyWFCExample2复制得到，用来做一些修改，尝试读取多个工作流
 */
public class MyWFCExample4 {

    private static String experimentName = "MyWFCExampleStatic4";
    private static int num_user = 1;
    private static boolean trace_flag = false;  // mean trace events
    private static boolean failure_flag = false;   //可能是指不考虑容错
    private static List<Container> containerList;
    private static List<ContainerHost> hostList;
    public static List<? extends ContainerVm> vmList; //这个vmList没有用到？

    public static void main(String[] args) {
        try {
            long startTime = System.currentTimeMillis();
            WFCConstants.CAN_PRINT_SEQ_LOG = false;
            WFCConstants.CAN_PRINT_SEQ_LOG_Just_Step = false;
            WFCConstants.ENABLE_OUTPUT = false;
            WFCConstants.FAILURE_FLAG = false;
            WFCConstants.RUN_AS_STATIC_RESOURCE = true;

            FailureParameters.FTCMonitor ftc_monitor = null;
            FailureParameters.FTCFailure ftc_failure = null;
            FailureParameters.FTCluteringAlgorithm ftc_method = null;
            DistributionGenerator[][] failureGenerators = null;

            Log.printLine("当前时间为：" + new Date().toString());
            Log.printLine("Starting " + experimentName + " ... ");

            List<String> daxPathList = new ArrayList<>();
            for(int i = 0; i < WFCConstants.WFC_NUMBER_WORKFLOW; i++){
                daxPathList.add("./config/dax/" + WFCWorkload.workload2000[i]);
            }
//            daxPathList.add("./config/dax/" + WFCWorkload.workload1000[15]);
            //如果考虑容错？
            if (failure_flag) {
                /*
                 *  Fault Tolerant Parameters
                 */
                /**
                 * MONITOR_JOB classifies failures based on the level of jobs;
                 * MONITOR_VM classifies failures based on the vm id; MOINTOR_ALL
                 * does not do any classification; MONITOR_NONE does not record any
                 * failiure.
                 */
                ftc_monitor = FailureParameters.FTCMonitor.MONITOR_ALL;
                /**
                 * Similar to FTCMonitor, FTCFailure controls the way how we
                 * generate failures.
                 */
                ftc_failure = FailureParameters.FTCFailure.FAILURE_ALL;
                /**
                 * In this example, we have no clustering and thus it is no need to
                 * do Fault Tolerant Clustering. By default, WorkflowSim will just
                 * rety all the failed task.
                 */
                ftc_method = FailureParameters.FTCluteringAlgorithm.FTCLUSTERING_NOOP;
                /**
                 * Task failure rate for each level
                 *
                 */
                failureGenerators = new DistributionGenerator[1][1];
//                failureGenerators[0][0] = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL,
//                        100, 1.0, 30, 300, 0.78);
                failureGenerators[0][0] = new DistributionGenerator(DistributionGenerator.DistributionFamily.WEIBULL,
                        100, 1.0, 30, 300, 0.78);
            }
            //SchedulingAlgorithm是一个枚举类型的变量
            //使用minmin算法
            //把minmin改成了 VMJUSTTRY
//            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.VMJUSTTRY;
            Parameters.SchedulingAlgorithm sch_method = WFCConstants.assignedAlgorithm;
            //PlanningAlgorithm指定的算法不会对stage-in任务进行调度，因为算法调度的时候还不存在stage-in任务
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;//global-stage
            //表示数据都是本地的？
            //这里从LOCAL改成了SHARED
//            WFCReplicaCatalog.FileSystem file_system = WFCReplicaCatalog.FileSystem.LOCAL;
            WFCReplicaCatalog.FileSystem file_system = WFCReplicaCatalog.FileSystem.SHARED;
            //没有额外开销
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
            //没有clustering
            //Clustering是什么？看WorkflowSim的论文
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            //这个参数影响了WFCEngineClustering
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            if (failure_flag) {
                FailureParameters.init(ftc_method, ftc_monitor, ftc_failure, failureGenerators);
            }
            //因为已经有dax文件包含了运行时间、数据大小的数据，所以有的参数不需要写，为null

//            Parameters.init(WFCConstants.WFC_NUMBER_VMS, daxPath, null,
//                    null, op, cp, sch_method, pln_method,
//                    null, 0);
            //如果换成重载的init()函数，可以读取多个dax路径
            //这里把收费模型从默认Datacenter改为了VM
            Parameters.init(Parameters.CostModel.VM, WFCConstants.WFC_NUMBER_VMS, daxPathList, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
//            WFCReplicaCatalog.init(file_system);

            if (failure_flag) {
                FailureMonitor.init();
                FailureGenerator.init();
            }
            //为什么这又出现了一个？
            //为什么没有创建这个对象，直接使用类名？因为初始化的对象是静态的
            WFCReplicaCatalog.init(file_system);

            Calendar calendar = Calendar.getInstance();

            CloudSim.init(num_user, calendar, trace_flag);
            //PowerContainerAllocationPolicySimple里没有什么实质性内容,它的父类中已经定义了find VM的方法，就是最简单的首次适应
            PowerContainerAllocationPolicy containerAllocationPolicy = new PowerContainerAllocationPolicySimple();
            //VmSelectionPolicy是用来干嘛的？是用来迁移VM的
            PowerContainerVmSelectionPolicy vmSelectionPolicy = new PowerContainerVmSelectionPolicyMaximumUsage();
            HostSelectionPolicy hostSelectionPolicy = new HostSelectionPolicyFirstFit();

            String logAddress = "~/Results";

            hostList = new ArrayList<ContainerHost>();
            hostList = createHostList(WFCConstants.WFC_NUMBER_HOSTS);
            containerList = new ArrayList<Container>();
            //负责虚拟机分配给host的策略
            //为什么是PowerContainerVmAllocationPolicyMigrationAbstractHostSelection？它的父类不可以吗？
            ContainerVmAllocationPolicy vmAllocationPolicy = new
                    PowerContainerVmAllocationPolicyMigrationAbstractHostSelection(hostList, vmSelectionPolicy,
                    hostSelectionPolicy, WFCConstants.WFC_CONTAINER_OVER_UTILIZATION_THRESHOLD, WFCConstants.WFC_CONTAINER_UNDER_UTILIZATION_THRESHOLD);
            //这里面有个WFC_DC_SCHEDULING_INTERVAL，表示调度的间隔？
            WFCDatacenter datacenter = (WFCDatacenter) createDatacenter("datacenter_0",
                    PowerContainerDatacenterCM.class, hostList, vmAllocationPolicy, containerList, containerAllocationPolicy,
                    getExperimentName(experimentName, String.valueOf(WFCConstants.OVERBOOKING_FACTOR)),
                    WFCConstants.WFC_DC_SCHEDULING_INTERVAL, logAddress,
                    WFCConstants.WFC_VM_STARTTUP_DELAY,
                    WFCConstants.WFC_CONTAINER_STARTTUP_DELAY);
            //其中调用了WFCEngine的构造函数，该构造函数中创建了vmlist，并提交到了scheduler
            //WFCPlanner的startEntity()中输出了“String WorkflowSim......”
            WFCPlanner wfPlanner = new WFCPlanner("planner_0", 1);//指定只有1个scheduler

            WFCEngine wfEngine = wfPlanner.getWorkflowEngine();
            //vmList = createVmList(wfEngine.getSchedulerId(0), Parameters.getVmNum());
            //wfEngine.submitVmList(wfEngine.getVmList(), 0);
            wfEngine.bindSchedulerDatacenter(datacenter.getId(), 0);

            //限制仿真中的时间，不是真实时间
            CloudSim.terminateSimulation(WFCConstants.SIMULATION_LIMIT);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();


            List<Job> outputList0 = wfEngine.getJobsReceivedList();


//                args[0] = args[0].concat(printJobList(outputList0, datacenter));
            printJobList(outputList0, datacenter, daxPathList);



            Log.printLine(experimentName + "finished!");
            //outputByRunnerAbs();
            long endTime = System.currentTimeMillis();
            Log.printConcatLine("程序运行时间： " + (endTime - startTime) / 1000.0 / 60 + "分钟");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
    }

    public static WFCDatacenter createDatacenter(String name, Class<? extends WFCDatacenter> datacenterClass,
                                                 List<ContainerHost> hostList,
                                                 ContainerVmAllocationPolicy vmAllocationPolicy,
                                                 List<Container> containerList,
                                                 ContainerAllocationPolicy containerAllocationPolicy,
                                                 String experimentName, double schedulingInterval, String logAddress, double VMStartupDelay,
                                                 double ContainerStartupDelay) throws Exception {

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).

        LinkedList<Storage> storageList = new LinkedList<Storage>();
        WFCDatacenter datacenter = null;

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        //int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            //添加存储，WFCDatacenter的storageList只在这里添加过
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(WFCConstants.WFC_DC_MAX_TRANSFER_RATE);
            storageList.add(s1);

            ContainerDatacenterCharacteristics characteristics = new
                    ContainerDatacenterCharacteristics(WFCConstants.WFC_DC_ARCH, WFCConstants.WFC_DC_OS, WFCConstants.WFC_DC_VMM,
                    hostList, WFCConstants.WFC_DC_TIME_ZONE, WFCConstants.WFC_DC_COST, WFCConstants.WFC_DC_COST_PER_MEM,
                    WFCConstants.WFC_DC_COST_PER_STORAGE, WFCConstants.WFC_DC_COST_PER_BW);

            datacenter = new WFCDatacenter(name,
                    characteristics,
                    vmAllocationPolicy,
                    containerAllocationPolicy,
                    storageList,
                    schedulingInterval,
                    experimentName,
                    logAddress
            );

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return datacenter;
    }


    public static List<ContainerHost> createHostList(int hostsNumber) {

        ArrayList<ContainerHost> hostList = new ArrayList<ContainerHost>();
        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.

        try {
            for (int i = 1; i <= WFCConstants.WFC_NUMBER_HOSTS; i++) {
                ArrayList<ContainerVmPe> peList = new ArrayList<ContainerVmPe>();
                // 3. Create PEs and add these into the list.
                //for a quad-core machine, a list of 4 PEs is required:
                for (int p = 0; p < WFCConstants.WFC_NUMBER_HOST_PES; p++) {
                    peList.add(new ContainerVmPe(p, new ContainerVmPeProvisionerSimple(WFCConstants.WFC_HOST_MIPS))); // need to store Pe id and MIPS Rating
                }

                //把host的构造换成下面的
//                hostList.add(new PowerContainerHostUtilizationHistory(IDs.pollId(ContainerHost.class),
//                        new ContainerVmRamProvisionerSimple(WFCConstants.WFC_HOST_RAM),
//                        new ContainerVmBwProvisionerSimple(WFCConstants.WFC_HOST_BW), WFCConstants.WFC_HOST_STORAGE, peList,
//                        new ContainerVmSchedulerTimeSharedOverSubscription(peList),
//                        //new ContainerVmSchedulerTimeShared(peList),
//                        WFCConstants.HOST_POWER[2]));
                hostList.add(new JustToTryPowerContainerHost(IDs.pollId(ContainerHost.class),
                        new ContainerVmRamProvisionerSimple(WFCConstants.WFC_HOST_RAM),
                        new ContainerVmBwProvisionerSimple(WFCConstants.WFC_HOST_BW), WFCConstants.WFC_HOST_STORAGE, peList,
                        new ContainerVmSchedulerTimeSharedOverSubscription(peList),
                        WFCConstants.HOST_POWER[2]
                ));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
            Log.printLine(e.getMessage());
            System.exit(0);
        }
        return hostList;
    }

    private static String getExperimentName(String... args) {
        StringBuilder experimentName = new StringBuilder();

        for (int i = 0; i < args.length; ++i) {
            if (!args[i].isEmpty()) {
                if (i != 0) {
                    experimentName.append("_");
                }

                experimentName.append(args[i]);
            }
        }

        return experimentName.toString();
    }

    /**
     * Gets the maximum number of GB ever used by the application's heap.
     *
     * @return the max heap utilization in GB
     * @see <a href="https://www.oracle.com/webfolder/technetwork/tutorials/obe/java/gc01/index.html">Java Garbage Collection Basics (for information about heap space)</a>
     */
    private static double getMaxHeapUtilizationGB() {
        final double memoryBytes =
                ManagementFactory.getMemoryPoolMXBeans()
                        .stream()
                        .filter(bean -> bean.getType() == MemoryType.HEAP)
                        .filter(bean -> bean.getName().contains("Eden Space") || bean.getName().contains("Survivor Space"))
                        .map(MemoryPoolMXBean::getPeakUsage)
                        .mapToDouble(MemoryUsage::getUsed)
                        .sum();

        return Conversion.bytesToGigaBytes(memoryBytes);
    }

    //在WFCEngine中使用了WFCExample1的createVmList()方法创建VM，这种相当于内核反而在借助example，应该改一下
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
                vm[i] = new PowerContainerVm(IDs.pollId(ContainerVm.class), brokerId, WFCConstants.WFC_VM_MIPS, (float) WFCConstants.WFC_VM_RAM,
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
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static String printJobList(List<Job> list, WFCDatacenter datacenter, List<String> daxPathList) {
        double maxHeapUtilizationGB = getMaxHeapUtilizationGB();
        String indent = "    ";
        double cost = 0.0;
        double time = 0.0;
        double length = 0.0;
        int counter = 1;
        int success_counter = 0;
        int redundant_success_counter = 0; //多余的成功的数量
        boolean[] showOrNot = new boolean[(WFCConstants.WFC_NUMBER_CLOUDLETS - 1) * WFCConstants.WFC_NUMBER_WORKFLOW + 1];
        Set<Integer> failedJobIds = new HashSet<>();
        Map<Cloudlet, Boolean> redundantMap = new HashMap<>();
        for(Job job : list){
            if (job.getStatus() == Cloudlet.FAILED) {
                failedJobIds.add(job.getCloudletId());
            }
        }


        Log.printLine();
        DecimalFormat dft0 = new DecimalFormat("###.###");
        DecimalFormat dft = new DecimalFormat("####.###");
        if (WFCConstants.PRINT_JOBLIST) {
            Log.printLine("========== OUTPUT ==========");
            Log.printLine("Cloudlet Column=Task=>Length,WFType,Impact # Times of Task=>Actual,Exec,Finish.");//,CloudletOutputSize
            Log.printLine();
            Log.printLine(indent + "Row" + indent + "JOB ID" + indent + indent + "CLOUDLET" + indent + indent
                    + "STATUS" + indent
                    + "Data CENTER ID"
                    //+ indent + indent + "HOST ID"
                    + indent + "VM ID" + indent + indent + "CONTAINER ID" + indent + indent
                    + "TIME" + indent + indent + indent + "START TIME" + indent + indent + "FINISH TIME" + indent + "DEPTH" + indent + indent + "Cost");

            for (Job job : list) {
                Log.print(String.format("%6d |", counter++) + indent + job.getCloudletId() + indent + indent);
                if (job.getClassType() == ClassType.STAGE_IN.value) {
                    Log.print("STAGE-IN");
                } else {
                    //这里多加了个else，打印的时候就能对齐一些
                    Log.print(indent);
                    Log.print(indent);
                }
                Log.print(indent);
                cost += job.getProcessingCost();
                time += job.getActualCPUTime();
                length += job.getCloudletLength();
                if (job.getStatus() == Cloudlet.SUCCESS) {
                    Log.print("     SUCCESS");
                    //datacenter.getContainerAllocationPolicy().getContainerVm(job.getContainerId(), job.getUserId()).getHost().getId()
                    Log.printLine(indent + indent + indent + job.getResourceId()
                            //+ indent + indent  + indent + indent + datacenter.getVmAllocationPolicy().getHost(job.getVmId(), job.getUserId()).getId()
                            + indent + indent + indent + job.getVmId()
                            + indent + indent + indent + job.getContainerId()
                            + indent + indent + indent + dft.format(job.getCloudletLength()) + indent + dft.format(job.getActualCPUTime())
//                        + indent + indent + indent + dft.format()
                            + indent + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                            + indent + indent + indent
                            + dft.format(job.getProcessingCost()

                    ));
                } else if (job.getStatus() == Cloudlet.FAILED) {
                    Log.print("      FAILED");
                    Log.printLine(indent + indent + indent + job.getResourceId()
//                        + indent + indent + indent + indent + datacenter.getVmAllocationPolicy().getHost(job.getVmId(), job.getUserId()).getId()
                            + indent + indent + indent + job.getVmId()
                            + indent + indent + indent + job.getContainerId()
                            + indent + indent + indent + dft.format(job.getActualCPUTime())
                            + indent + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                            + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                            + indent + indent + indent + dft.format(job.getProcessingCost()

                    ));
                }
            }
        }

//        for (Job job : list) {
//            if (job.getStatus() == Cloudlet.SUCCESS) {
//                if(WFCReplication.getSlaveToMasterMap().containsKey(job)){
//                    Integer majorId = WFCReplication.getSlaveToMasterMap().get(job).getCloudletId();
//                    if (!failedJobIds.contains(majorId)) {
//                        redundant_success_counter++;
//                        redundantMap.put(job, true);
//                    } else {
//                        success_counter++;
//                        redundantMap.put(job, false);
//                        int index = -1;
//                        if(WFCReplication.getToOrigin().containsKey(job)) {
//                            index = WFCReplication.getToOrigin().get(job).getCloudletId();
//                        }else{
//                            index = job.getCloudletId();
//                        }
//                        showOrNot[index - 1] = !showOrNot[index - 1];
////                        Log.printLine(job.getCloudletId());
//                    }
//                }else{
//                    success_counter++;
//                    redundantMap.put(job, false);
//                    int index = -1;
//                    if(WFCReplication.getToOrigin().containsKey(job)) {
//                        index = WFCReplication.getToOrigin().get(job).getCloudletId();
//                    }else{
//                        index = job.getCloudletId();
//                    }
//                    showOrNot[index - 1] = !showOrNot[index - 1];
////                    Log.printLine(job.getCloudletId());
//                }
//            }
//        }

        int sumCloudlets = 0;
        for(int i = 0; i < WFCConstants.WFC_NUMBER_WORKFLOW; i++){
            sumCloudlets += WFCDeadline.cloudletNumOfWorkflow.get(i + 1);
        }

        boolean[] counter22 = new boolean[WFCDeadline.numOfAllCloudlets];
        Arrays.fill(counter22, false);

        for (Job job : list) {
            counter22[job.getCloudletId() - 1] = true;
            if (job.getStatus() == Cloudlet.SUCCESS) {
                if (WFCReplication.getSlaveToMasterMap().containsKey(job)) {
                    Integer majorId = WFCReplication.getSlaveToMasterMap().get(job).getCloudletId();
                    if (!failedJobIds.contains(majorId)) {
                        redundant_success_counter++;
                    } else {
                        success_counter++;
                        // 记录工作流的完成时间
                        if(!WFCDeadline.finishTimeOfWorkflow.containsKey(job.getWorkflowId())
                                || WFCDeadline.finishTimeOfWorkflow.get(job.getWorkflowId()) < job.getFinishTime()){
                            WFCDeadline.finishTimeOfWorkflow.put(job.getWorkflowId(), job.getFinishTime());
                        }
                    }
                } else {
                    success_counter++;
                    // 记录工作流的完成时间
                    if(!WFCDeadline.finishTimeOfWorkflow.containsKey(job.getWorkflowId())
                            || WFCDeadline.finishTimeOfWorkflow.get(job.getWorkflowId()) < job.getFinishTime()){
                        WFCDeadline.finishTimeOfWorkflow.put(job.getWorkflowId(), job.getFinishTime());
                    }
                }
            }
        }

        Map<String, int[]> satisfyRateByWorkflowType = new HashMap<>();
        int satisfyDeadlineNum = 0;
        Log.printLine(indent + "Row" + indent + "WORKFLOW ID"
                + indent + "WORKFLOW NAME" + indent + indent + indent +
                "FINISH TIME" + indent + "EXECUTION TIME" + indent + "DEADLINE" + indent + "BEST TIME");
        DecimalFormat df = new DecimalFormat("#.00");
        int rowNum = 1;
        for(Map.Entry<Integer, Double> entry : WFCDeadline.finishTimeOfWorkflow.entrySet()){
            if(entry.getKey() == 0){
                Log.printLine(indent + rowNum + indent + indent + entry.getKey()
                        + indent + indent + indent + "stage-in"
                        + indent + indent + indent + indent + indent + df.format(entry.getValue()));
            }else{
                String workflowPath = daxPathList.get(entry.getKey() - 1);
                String workflowName = workflowPath.substring(workflowPath.lastIndexOf("/") + 1);
                Log.printLine(indent + rowNum + indent + indent + entry.getKey()
                        + indent + indent + indent + String.format("%-20s", workflowName)
                        + indent + indent + df.format(entry.getValue()) + indent + indent
                        + df.format(entry.getValue() - WFCConstants.ARRIVAL_TIME[entry.getKey() - 1])
                        + indent + indent + df.format(WFCConstants.DEADLINES[entry.getKey() - 1] * WFCConstants.DEADLINE_RATE)
                        + indent + indent + df.format(WFCDeadline.workflowFastDeadlineMap.get(workflowName))
                );

                String workflowType = workflowName.substring(0, workflowName.indexOf('_'));
                int[] sumAndSuccessNum = satisfyRateByWorkflowType.getOrDefault(workflowType, new int[]{0, 0});
                sumAndSuccessNum[0]++;
                // 判断该工作流是否在截止时间之前完成
                if(entry.getValue() - WFCConstants.ARRIVAL_TIME[entry.getKey() - 1]
                        <= WFCConstants.DEADLINES[entry.getKey() - 1] * WFCConstants.DEADLINE_RATE){
                    satisfyDeadlineNum++;
                    sumAndSuccessNum[1]++;
                }
                satisfyRateByWorkflowType.put(workflowType, sumAndSuccessNum);
            }
            rowNum++;
        }
        Log.printLine("============================================");

        rowNum = 1;
        Log.printLine(indent + "Row" + indent + indent + "WORKFLOW TYPE"
                + indent + "WORKFLOW NUM"
                + indent + "SATISFY NUM"
                + indent + "SATISFY RATE(%)");
        for(Map.Entry<String, int[]> entry : satisfyRateByWorkflowType.entrySet()){
            Log.printLine(indent + rowNum + indent + indent + String.format("%-20s", entry.getKey())
                    + indent + indent + entry.getValue()[0]
                    + indent + indent + entry.getValue()[1]
                    + indent + indent + df.format((double)entry.getValue()[1] / entry.getValue()[0] * 100)
            );
        }
        Log.printLine("============================================");

        Log.printLine();
        if(counter22.length <= 0){
            Log.printLine("没有丢失任务");
        }
        for(int j = 0; j < counter22.length; j++){
            if(false == counter22[j]){
                Log.printLine("丢失了任务：" + (j + 1));
            }
        }
        Log.printLine("============================================");

        Log.printConcatLine("满足截止时间的比例为：", (double)satisfyDeadlineNum / (WFCDeadline.finishTimeOfWorkflow.size() - 1));
        Log.printLine("The total cost is " + dft.format(WFCDatacenter.totalCost));
//        Log.printLine("MinTimeBetweenEvents is " + dft.format(WFCConstants.MIN_TIME_BETWEEN_EVENTS));
//        Log.printLine("The total actual cpu time is " + dft.format(time));
//        Log.printLine("The length cloudlets is " + dft.format(length));
        Log.printConcatLine("一共租用过", IDs.pollId(ContainerVm.class) - 1, "个VM");
        Log.printLine("The total failed counter is " + dft.format(failedJobIds.size()));
        Log.printLine("The success counter is " + dft.format(success_counter));
        Log.printLine("The redundant success counter is " + dft.format(redundant_success_counter));
        Log.printLine("The sum of above 3 numbers is " + dft.format(failedJobIds.size() + success_counter + redundant_success_counter));
        Log.printLine("重新提交次数是：" + WFCReplication.retry);
        Log.printLine("初始任务总数是：" + (sumCloudlets + 1) + "（包含stage-in任务）");
        Log.printLine("产生过的任务总数（包含复制和重新提交，以及stage-in任务）是：" + WFCDeadline.numOfAllCloudlets);
//        Log.printConcatLine("WFCEngine发送了", WFCDeadline.wfcEngineSendJobNum, "个任务到Scheduler");
//        Log.printConcatLine("WFCScheduler发送了", WFCDeadline.wfcSchedulerSendJobNum, "个任务到WFCDatacenter");
//        Log.printConcatLine("WFCDatacenter分配了", WFCDeadline.wfcDatacenterAllocateJobNum, "个任务到容器上");
//        Log.printConcatLine("list中有", list.size(), "个任务");
//        Log.printConcatLine("容器中成功运行过", WFCDeadline.withinContainerSuccJobNum, "个任务");
//        Log.printConcatLine("处理过", WFCDeadline.withinContainerFailJobNum, "个容器中失败的任务");
//        Log.printConcatLine("销毁旧容器时，丢失了", WFCDeadline.lossJobNumWhenChangeContainer, "个任务");
//        Log.printConcatLine("容器给WFCDatacenter返回了一共", WFCDeadline.containerSchedulerSendJobNum, "个任务");
        Log.printConcatLine("最小deadline为：", WFCDeadline.smallestNewDead);
        for(Map.Entry<Double, int[]> entry : WFCDeadline.vmNumCloudletNum.entrySet()){
            Log.printConcatLine("时间：" + new DecimalFormat("#.00").format(entry.getKey()),
                    "  VM数量：", entry.getValue()[0],
                    "  VM上任务数量：", entry.getValue()[1], "  待调度的任务数量：", entry.getValue()[2]);
        }
        return String.valueOf(success_counter);
    }
}