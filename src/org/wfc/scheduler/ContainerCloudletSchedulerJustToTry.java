package org.wfc.scheduler;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.wfc.core.WFCConstants;
import org.wfc.core.WFCDeadline;
import org.workflowsim.utils.Parameters;

import java.sql.Array;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ContainerCloudletSchedulerJustToTry extends ContainerCloudletScheduler {

    /** 每个核的mips */
    private double mips;

    /** The number of PEs. */
    private int numberOfPes;

    /** The total mips. */
    private double totalMips;

    private int workflowId;

    public ContainerCloudletSchedulerJustToTry(double mips, int numberOfPes){
        super();
        setMips(mips);
        setNumberOfPes(numberOfPes);
        setTotalMips(getNumberOfPes() * getMips());
    }

    public ContainerCloudletSchedulerJustToTry(int workflowId, double mips, int numberOfPes){
        super();
        this.workflowId = workflowId;
        setMips(mips);
        setNumberOfPes(numberOfPes);
        setTotalMips(getNumberOfPes() * getMips());
    }

    public int getWorkflowId() {
        return workflowId;
    }

    /**
     * Sets the pes number.
     *
     * @param pesNumber the new pes number
     */
    public void setNumberOfPes(int pesNumber) {
        numberOfPes = pesNumber;
    }

    /**
     * Sets the mips.
     *
     * @param mips the new mips
     */
    public void setMips(double mips) {
        this.mips = mips;
    }

    /**
     * Gets the pes number.
     *
     * @return the pes number
     */
    public int getNumberOfPes() {
        return numberOfPes;
    }

    /**
     * Gets the mips.
     *
     * @return the mips
     */
    public double getMips() {
        return mips;
    }

    /**
     * Sets the total mips.
     *
     * @param mips the new total mips
     */
    public void setTotalMips(double mips) {
        totalMips = mips;
    }

    /**
     * 获取该虚拟机上，完成现有任务还需要多长时间
     */
    @Override
    public double getRemainingTime(){
        //第二个参数目前没什么用
        updateContainerProcessing(CloudSim.clock(), new ArrayList<>());
        double finishTime = 0.0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            finishTime = getEstimatedFinishTime(rcl, CloudSim.clock()) - CloudSim.clock();
        }
        for (ResCloudlet rcl : getCloudletWaitingList()){
            finishTime += rcl.getRemainingCloudletLength() / getTotalCurrentAllocatedMipsForCloudlet(rcl, CloudSim.clock());
        }
        return finishTime;
    }

    /**
     * 这里和ContainerCloudletSchedulerDynamicWorkload中的不同在于完成任务后，要把等待队列中的任务加入到运行队列
     * @param currentTime current simulation time
     * @param mipsShare array with MIPS share of each processor available to the scheduler
     * @return
     */
    @Override
    public double updateContainerProcessing(double currentTime, List<Double> mipsShare) {
        setCurrentMipsShare(mipsShare);

        double timeSpan = currentTime - getPreviousTime();
        double nextEvent = Double.MAX_VALUE;
        List<ResCloudlet> cloudletsToFinish = new ArrayList<>();

        for (ResCloudlet rcl : getCloudletExecList()) {
            //计算这段时间完成了多少length，在原来的基础上加上
            rcl.updateCloudletFinishedSoFar((long) (timeSpan
                    * getTotalCurrentAllocatedMipsForCloudlet(rcl, getPreviousTime()) * Consts.MILLION));
            if (rcl.getRemainingCloudletLength() == 0) { // finished: remove from the list
                cloudletsToFinish.add(rcl);
                WFCDeadline.withinContainerSuccJobNum++;
            } else { // not finish: estimate the finish time
                double estimatedFinishTime = getEstimatedFinishTime(rcl, currentTime);
                if (estimatedFinishTime - currentTime < WFCConstants.MIN_TIME_BETWEEN_EVENTS) {
                    estimatedFinishTime = currentTime + WFCConstants.MIN_TIME_BETWEEN_EVENTS;
                }
                if (estimatedFinishTime < nextEvent) {
                    nextEvent = estimatedFinishTime;
                }
            }
        }

        //从运行队列中去掉已经完成的任务
        for (ResCloudlet rgl : cloudletsToFinish) {
            getCloudletExecList().remove(rgl);
            cloudletFinish(rgl);
        }

        /**
         * 添加的步骤：把等待的任务加入到运行队列（有问题，如果此时cloudletsToFinish为空，也应该添加新任务的运行队列）
         */
//        if(cloudletsToFinish.size() > 0 && getCloudletWaitingList().size() > 0 && getCloudletExecList().size() == 0){
        if(getCloudletWaitingList().size() > 0 && getCloudletExecList().size() == 0){
            ResCloudlet resCloudlet = getCloudletWaitingList().get(0);
            if(WFCConstants.PRINT_SCHEDULE_WAITING_LIST){
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "虚拟机", resCloudlet.getCloudlet().getVmId() , ":任务", resCloudlet.getCloudletId(), "转正");
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "=================================");
            }
            getCloudletWaitingList().remove(resCloudlet);
            resCloudlet.setCloudletStatus(Cloudlet.INEXEC);
            getCloudletExecList().add(resCloudlet);

            // 把等待的任务转正之后，还要计算下一个事项的发生时间
            double estimatedFinishTime = getEstimatedFinishTime(resCloudlet, currentTime);
//            if (estimatedFinishTime - currentTime < WFCConstants.MIN_TIME_BETWEEN_EVENTS) {
//                estimatedFinishTime = currentTime + WFCConstants.MIN_TIME_BETWEEN_EVENTS;
//            }
            if (estimatedFinishTime < nextEvent) {
                nextEvent = estimatedFinishTime;
            }
        }

        setPreviousTime(currentTime);
        if (getCloudletExecList().isEmpty()) {
            return 0;
        }
        cloudletsToFinish.clear();
        //对于这个容器来说，下一个事项发生的时间
        return nextEvent;
    }

    /**
     * 覆写cloudletSubmit(Cloudlet cl, double fileTransferTime)，这个函数在WFCDatacenter.processCloudletSubmit()中，提交前驱已完成的任务时用到了
     */
    @Override
    public double cloudletSubmit(Cloudlet cl, double fileTransferTime) {
        if(WFCConstants.PRINT_SCHEDULE_WAITING_LIST){
            Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), ":容器的Scheduler: 虚拟机", cl.getVmId(),"接收了任务", cl.getCloudletId());
//            if(cl.getVmId() == 3){
//                Log.printConcatLine(CloudSim.clock(), ":虚拟机", cl.getVmId(),"当前有", getCloudletExecList().size(), "个运行的任务，", getCloudletWaitingList().size(), "个等待的任务");
//            }
        }
        //考虑到传输时间
        if(WFCConstants.CONSIDER_TRANSFER_TIME){
            cl.setCloudletLength((long)(cl.getCloudletLength() + fileTransferTime * getMips()));
        }

        if(cl.getClassType() == Parameters.ClassType.STAGE_IN.value){
            cl.setCloudletLength(0);
        }

        ResCloudlet rcl = new ResCloudlet(cl);
        for (int i = 0; i < cl.getNumberOfPes(); i++) {
            rcl.setMachineAndPeId(0, i);//这是做什么？
        }
        if(getCloudletExecList().size() > 0){
            if(WFCConstants.PRINT_SCHEDULE_WAITING_LIST){
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "出现了任务", rcl.getCloudletId(), "等待的情况");
                Log.printConcatLine(new DecimalFormat("#.00").format(CloudSim.clock()), "=================================");
            }
            rcl.setCloudletStatus(Cloudlet.QUEUED);
            getCloudletWaitingList().add(rcl);
            //这个等待的任务的完成时间应该是运行任务的完成时间加上等待任务的运行时间
            return getEstimatedFinishTime(getCloudletExecList().get(0), getPreviousTime()) +
                    (rcl.getRemainingCloudletLength()) / getTotalMips();
        }else{
//            if(cl.getVmId() == 3){
//                Log.printConcatLine(CloudSim.clock(), ":虚拟机", cl.getVmId(),"开始运行任务", rcl.getCloudletId());
//            }
            rcl.setCloudletStatus(Cloudlet.INEXEC);
            getCloudletExecList().add(rcl);
            return getEstimatedFinishTime(rcl, getPreviousTime());
        }
    }

    /**
     * 这个方法没有用过
     * @param gl the submited cloudlet
     * @return
     */
    @Override
    public double cloudletSubmit(Cloudlet gl) {
        return 0;
    }

    @Override
    public double getEstimatedFinishTime(ResCloudlet rcl, double time) {
        //getRemainingCloudletLength()得到的是文件中的任务的运行时间，换算成毫秒的数字,一般是一万到几万，容器的MIPS暂时设为

        double esFinishTime = time + ((rcl.getRemainingCloudletLength()) / getTotalCurrentAllocatedMipsForCloudlet(rcl, time));
        return esFinishTime;
    }

    @Override
    public Cloudlet cloudletCancel(int clId) {
        return null;
    }

    @Override
    public boolean cloudletPause(int clId) {
        return false;
    }

    @Override
    public double cloudletResume(int clId) {
        return 0;
    }

    @Override
    public void cloudletFinish(ResCloudlet rcl) {
        rcl.setCloudletStatus(Cloudlet.SUCCESS);
        rcl.finalizeCloudlet();
        getCloudletFinishedList().add(rcl);
    }

    @Override
    public int getCloudletStatus(int clId) {
        for (ResCloudlet rcl : getCloudletExecList()) {
            if (rcl.getCloudletId() == clId) {
                return rcl.getCloudletStatus();
            }
        }
        for (ResCloudlet rcl : getCloudletPausedList()) {
            if (rcl.getCloudletId() == clId) {
                return rcl.getCloudletStatus();
            }
        }
        return -1;
    }

    @Override
    public boolean isFinishedCloudlets() {
        return getCloudletFinishedList().size() > 0;
    }

    @Override
    public Cloudlet getNextFinishedCloudlet() {
        if (getCloudletFinishedList().size() > 0) {
            WFCDeadline.containerSchedulerSendJobNum++;
            return getCloudletFinishedList().remove(0).getCloudlet();
        }
        return null;
    }

    @Override
    public int runningCloudlets() {
        return 0;
    }

    @Override
    public Cloudlet migrateCloudlet() {
        return null;
    }

    @Override
    public double getTotalUtilizationOfCpu(double time) {
        double totalUtilization = 0;
        for (ResCloudlet rcl : getCloudletExecList()) {
            totalUtilization += rcl.getCloudlet().getUtilizationOfCpu(time);
        }
        return totalUtilization;
    }

    /**
     * 这个函数是container用来获取总的需要的mips和几个核心中最大的mips，
     * 所以对于单核的container来说不需要写得复杂
     * @return
     */
    @Override
    public List<Double> getCurrentRequestedMips() {
        List<Double> list = new ArrayList<Double>();
        list.add(getTotalMips());
        return list;
    }

    /**
     * 因为一个容器独占一个虚拟机，所以可用的mips就是所有mips
     * @param rcl the rcl
     * @param mipsShare the mips share
     * @return
     */
    @Override
    public double getTotalCurrentAvailableMipsForCloudlet(ResCloudlet rcl, List<Double> mipsShare) {
        return totalMips;
    }

    @Override
    public double getTotalCurrentRequestedMipsForCloudlet(ResCloudlet rcl, double time) {
        return rcl.getCloudlet().getUtilizationOfCpu(time) * getTotalMips();
    }

    /**
     * Gets the total mips.
     *
     * @return the total mips
     */
    public double getTotalMips() {
        return totalMips;
    }

    /**
     * 一个容器独占一个虚拟机，所以直接返回totalMips
     * @param rcl the rcl
     * @param time the time
     * @return
     */
    @Override
    public double getTotalCurrentAllocatedMipsForCloudlet(ResCloudlet rcl, double time) {
        return totalMips;
    }

    @Override
    public double getCurrentRequestedUtilizationOfRam() {
        double ram = 0;
        for (ResCloudlet cloudlet : cloudletExecList) {
            ram += cloudlet.getCloudlet().getUtilizationOfRam(CloudSim.clock());
        }
        return ram;
    }

    @Override
    public double getCurrentRequestedUtilizationOfBw() {
        double bw = 0;
        for (ResCloudlet cloudlet : cloudletExecList) {
            bw += cloudlet.getCloudlet().getUtilizationOfBw(CloudSim.clock());
        }
        return bw;
    }


}
