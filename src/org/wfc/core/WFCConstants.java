package org.wfc.core;

import org.cloudbus.cloudsim.power.models.PowerModel;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G4Xeon3040;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G5Xeon3075;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerIbmX3550XeonX5670;
import org.workflowsim.ContainerVmType;
import org.workflowsim.utils.Parameters;

/**
 * In this class the specifications of the Cloudlets, Containers, VMs and Hosts are coded.
 * Regarding to the hosts, the powermodel of each type of the hosts are all included in this class.
 */
public  class WFCConstants {
  /**
     * The available virtual machine types along with the specs.
     */

    public static   PowerModel[] HOST_POWER = new PowerModel[]{new PowerModelSpecPowerHpProLiantMl110G4Xeon3040(),
            new PowerModelSpecPowerHpProLiantMl110G5Xeon3075(), new PowerModelSpecPowerIbmX3550XeonX5670()};

//-----------------------Simulation
    
    public static  int OVERBOOKING_FACTOR= 80;
    public static  int STAGE_IN_JOB_LENGTH = 110;  //stage-in任务的任务长度
    
    public static  boolean CAN_PRINT_SEQ_LOG = false;
    public static  boolean CAN_PRINT_SEQ_LOG_Just_Step = false;
    
    public static  boolean ENABLE_OUTPUT = false;
    public static  boolean FAILURE_FLAG = false;
    public static  boolean RUN_AS_STATIC_RESOURCE = true;
    public static  boolean POWER_MODE = false;

    /**
     * 自己添加几个参数，用于动态创建和销毁的开关
     */
    public static final boolean ENABLE_DYNAMIC_VM_CREATE = true; //是否动态创建VM
    public static final boolean ENABLE_DYNAMIC_VM_DESTROY = true; //是否动态销毁VM
    public static final boolean ENABLE_VM_SHUTDOWN = true; //是否有虚拟机宕机，为true时需要上一行也为true
    public static final boolean CONSIDER_TRANSFER_TIME = true; //是否考虑数据传输时间
    public static final boolean ENABLE_SPECIFY_WORKFLOW_ID = true; //区分工作流类型的开关

    /**
     * 用于控制台打印信息的开关
     */
    public static final boolean PRINT_JOBLIST = false; //是否打印所有任务完成后，任务的完成信息列表
    public static final boolean PRINT_SCHEDULE_WAITING_LIST = false; //是否打印正在等到调度的任务的信息
    public static final boolean PRINT_SCHEDULE_RESULT = false; //是否打印调度算法对任务的分配结果
    public static final boolean PRINT_CLOUDLET_RETURN = true; //是否打印关于任务返回的信息
    public static final boolean PRINT_CONTAINER_ALLOCATED = false; //是否打印关于容器分配的信息（在WFCDatacenter）
    public static final boolean PRINT_CONTAINER_CREATE = false; //是否打印关于容器创建的信息
    public static final boolean PRINT_CONTAINER_DESTROY = false; //是否打印关于容器删除的信息
    public static final boolean PRINT_COST_CALCULATE = false; //是否打印成本累加的信息
    public static final boolean PRINT_REPLICATE_RETRY = false; //是否打印复制任务和重新提交任务的信息
    public static final boolean PRINT_VM_SHUTDOWN = true; //是否打印VM宕机的信息
    public static final boolean PRINT_VM_NUM = false; //是否打印当前VM数量的信息
    public static final boolean PRINT_DETAINED_LIST = false; //是否打印任务进入滞留队列的信息
    public static final boolean PRINT_THREE_FUNCTION = false;

    /**
     * 关于时间的参数
     */
    public static final int WORKFLOW_ARRIVE_INTERVAL = 200; //工作流到达的间隔时间
    public static final double MIN_TIME_BETWEEN_EVENTS = 0.5; //以多大的时间间隔进行仿真
    public static final double HEFT_INTERVAL = 30;
    public static final double RECORD_NUM_INTERVAL = 200; //最后打印VM和任务数量时，记录的时间之间的间隔
    // deadline乘的系数
    public static final double DEADLINE_RATE = 1.3;
    // 使用的截止时间是随机分布乘系数，还是最严格的deadline乘系数
    // 修改工作负载后不需要修改这项
    public static final double[] DEADLINES = WFCDeadline.deadlinesFastest2000;

    /**
     * 指定调度算法
     */
    public static final Parameters.SchedulingAlgorithm assignedAlgorithm = Parameters.SchedulingAlgorithm.GREEDY;
    /**
     * 当指定的任务为MAJOR或FASTMAJOR时，可以指定VM上的任务队列长度
     */
    public static final int QUEUE_LENGTH = 1;
    /**
     * 关于容错的参数
     */
    public static final double VM_FAILURE_RATE = 0.11;
    public static final int VM_MINIMUM_NUMBER = 2; //最小的VM数量

//-----------------------Number
    // 工作流的数量，应与ARRIVAL_TIME同步
    public static final int WFC_NUMBER_WORKFLOW = 334;
    // 工作流的到达时间，应与WFC_NUMBER_WORKFLOW同步
    public static final int[] ARRIVAL_TIME = WFCDeadline.arrivalTime2PerMinuteAll334;
    public static final  int WFC_NUMBER_SCHEDULER = 1;
    public static final  int WFC_NUMBER_HOSTS = 1;
    // 第一批VM的数量
    public static final  int WFC_NUMBER_VMS = 2;
    // 第一批容器的数量
    public static final  int WFC_NUMBER_CONTAINER = 2;
    //第一批VM的类型
    public static final ContainerVmType DEFAULT_VM_TYPE = ContainerVmType.EXTRA_LARGE;
    public static final  int WFC_NUMBER_USERS = 1;
    public static final  int WFC_NUMBER_CLOUDLETS = 101;
    public static final  int WFC_NUMBER_CLOUDLET_PES = 1;
    public static final  int WFC_NUMBER_CONTAINER_PES = 1;
    public static final  int WFC_CONTAINER_PES_NUMBER = 1;
    public static final int  WFC_NUMBER_VM_PES = 1; //number of cpus  !!!!!!!!!!!!!!!!
    //host核心数原来是12
    public static final int WFC_NUMBER_HOST_PES = 1; //number of cpus

    //仿真的最大时间，这个很重要，可能会导致仿真意外结束
    public static final double SIMULATION_LIMIT = 87400.0D;
    public static final double CONTAINER_INITIAL_TIME = 10; //容器初始化所需时间
    public static final double VM_INITIAL_TIME = 100; //虚拟机初始化所需时间
    public static final double RENT_INTERVAL = 60 * 60; //虚拟机计费和租用周期
    public static final double WFC_DC_MAX_TRANSFER_RATE= 62.5; //VM之间最大传输速度,MB/s

    /**
     * 关于计费的参数
     */
    public static final double EXTRA_LARGE_PRICE = 8;
    public static final double LARGE_PRICE = 4;
    public static final double MEDIUM_PRICE = 2;
    public static final double SMALL_PRICE = 1;
    /**
     * 关于不同类型VM的性能的参数
     */
    public static final double EXTRA_LARGE_MIPS = 16000; //这里的1000算是1 MIPS
    public static final double LARGE_MIPS = 8000; //预处理，估计处理时间时用到了
    public static final double MEDIUM_MIPS = 4000;
    public static final double SMALL_MIPS = 2000;

    //-----------------------Delay

    public static  double WFC_CONTAINER_STARTTUP_DELAY = 0.6;
    public static  double WFC_VM_STARTTUP_DELAY = 1;

//-----------------------Cloudlet
    
    public static   int CLOUDLET_LENGTH = 30;    
    public static  long CLOUDLET_FILESIZE= 300;
    public static  long CLOUDLET_OUTPUTSIZE= 300;

//-----------------------Container    
     
    public static   long  WFC_CONTAINER_SIZE = 100;
    public static   int   WFC_CONTAINER_RAM = 8;
    // 在JustToTryPowerContainerVm中已经让创建的容器的性能和VM本身保持一致
    // 只要用JustToTryPowerContainerVm，这个参数就没用
    public static   int   WFC_CONTAINER_MIPS = 1000; //性能
//    public static   long  WFC_CONTAINER_BW = 100;
    // 如果修改了VM的带宽，容器的带宽也要更改，否则可能没法创建容器
    public static   long  WFC_CONTAINER_BW = 20;
    public static   double WFC_CONTAINER_RATIO = 1.0;
    public static   String WFC_CONTAINER_VMM = "Xen"; 
    public static   double WFC_CONTAINER_OVER_UTILIZATION_THRESHOLD = 0.80D;
    public static   double WFC_CONTAINER_UNDER_UTILIZATION_THRESHOLD = 0.70D;
    
//----------------------- VM    
    
    public static  long WFC_VM_SIZE = 10000; //image size (MB)
    public static  int  WFC_VM_RAM = 1024; //vm memory (MB)
    //public static  int  WFC_VM_MIPS = 100000; //这是每个核的性能

    //参考论文里MIPS只有2~16，为什么这里的值这么大？
    //PlanningAlgorithm中用这个来计算性能
    public static  int  WFC_VM_MIPS = 16000; //每个核的性能，一共有4个核，原来的值是100000   PPPPPPPPPPPPPPPP

//    public static  long WFC_VM_BW = 10000;//单位是MB
    public static  long WFC_VM_BW = 500;//在PlanningAlgorithm中，单位是Mb/s   PPPPPPPPPPPPP
    public static  double WFC_VM_RATIO = 1.0;
    public static  String WFC_VM_VMM = "Xen"; //VMM name
    
//----------------------- HOST    
    
     public static  long WFC_HOST_STORAGE = 10000 * (WFC_NUMBER_VMS + 100000);
    public static  long WFC_HOST_SIZE = 100000; //image size (MB)
    //MIPS增加了
    public static  int WFC_HOST_MIPS = 10000 * (WFC_NUMBER_VMS + 100000);
    //BW增加了
    public static  long WFC_HOST_BW = 500 * (WFC_NUMBER_VMS + 100000);
    //原来内存是1024*3
    public static  int WFC_HOST_RAM = 1024 * (WFC_NUMBER_VMS + 100000); //vm memory (MB)
    public static  String WFC_HOST_VMM = "Xen"; //VMM name    
    public static  double WFC_HOST_RATIO = 1.0;
        
//----------------------- DataCenter_Characteristics    
    
    
    public static  String WFC_DC_ARCH = "x86";      // system architecture
    public static  String WFC_DC_OS = "Linux";          // operating system
    public static  String WFC_DC_VMM = "Xen";
    public static  double WFC_DC_TIME_ZONE = 10.0;         // time zone this resource located
    public static  double WFC_DC_COST = 3.0;              // the cost of using processing in this resource
    public static  double WFC_DC_COST_PER_MEM = 0.05;		// the cost of using memory in this resource
    public static  double WFC_DC_COST_PER_STORAGE = 0.1;	// the cost of using storage in this resource
    public static  double WFC_DC_COST_PER_BW = 0.1;	

    public static    double WFC_DC_SCHEDULING_INTERVAL = 0.1D;
        
//-----------------------The Addresses
    public WFCConstants() {
    }
 }
