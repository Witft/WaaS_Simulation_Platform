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
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.container.core.Container;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCDeadline;

/**
 * The base scheduler has implemented the basic features. Every other scheduling method
 * should extend from BaseSchedulingAlgorithm but should not directly use it. 
 *
 * @author Arman Riazi
 * @since WorkflowSim Toolkit 1.0
 * @date March 29, 2020
 */
public abstract class BaseSchedulingAlgorithm implements SchedulingAlgorithmInterface {

//    /**
//     * 为了测试动态删除虚拟机，临时用的
//     */
//    private boolean hasDestroyedVm;
//    public void setHasDestroyedVm(boolean hasDestroyedVm){
//        this.hasDestroyedVm = hasDestroyedVm;
//    }
//    public boolean getHasDestroyedVm() {
//        return hasDestroyedVm;
//    }


    /**
     * the job list.
     */
    private List<? extends Cloudlet> cloudletList;
    /**
     * the vm list.
     */
    private List<? extends ContainerVm> vmList;
    /**
     * the scheduled job list.
     */
    private List<Cloudlet> scheduledList;
    /**
     * 调度算法想要新创建的VM列表
     */
    private List<? extends ContainerVm> toCreateVmList;

    /**
     * 为了加速搜索，改为Map
     */
    private Map<Integer, ContainerVm> toCreateVmMap;

    /**
     * 调度算法想销毁的VM列表
     */
    private List<? extends ContainerVm> toDestroyVmList;

    /**
     * Meng:如果要考虑容器的调度，还需要有containerList
     */
    private List<? extends Container> containerList;

    /**
     * 调度算法想创建的Container列表
     */
    private List<? extends Container> toCreateContainerList;
    /**
     * 调度算法想销毁的Container列表
     */
    private List<? extends Container> toDestroyContainerList;
    /**
     * 因为创建VM或容器而滞留的任务
     */
    private List<? extends ContainerCloudlet> detainedCloudletList;
    /**
     * 因为创建VM或容器而滞留的任务
     */
    private List<? extends ContainerCloudlet> lastSentCloudlets;
    /**
     * 需要复制的任务
     */
    private List<? extends ContainerCloudlet> replicateCloudlets;
    /**
     * 已经被复制的任务
     */
    private Set<? extends ContainerCloudlet> hasSentReplicateCloudlets;
    // 储存导致新VM租用的任务
    protected Map<Integer, ContainerVm> cloudletsLeadToNewVm;
    /**
     * 正在创建的所有VM的平均“伪空闲时间”，是一个绝对时间点
     */
    private List<Double> averageDummyIdleTime;
    /**
     * 存储WFCScheduler传来的正在切换容器的VM
     */
    private Map<Integer, Integer> vmsCreatingContainer;
    /**
     * 存储调度算法打算在VM上创建的容器类型
     */
    private Map<Integer, Integer> vmPlanToCreateContainer;
    /**
     * 存储资源调度算法中因其后继任务，而部署新容器的任务
     */
    protected Set<Integer> alreadyNewContainerCloudletId;
    /**
     * 存储资源调度算法中因其后继任务，而租用新VM的任务
     */
    protected Set<Integer> alreadyNewVmCloudletId;
    /**
     * 存储已经完成的任务id
     */
    protected Set<ContainerCloudlet> completedCloudlets;



    /**
     * Initialize a BaseSchedulingAlgorithm
     */
    public BaseSchedulingAlgorithm() {
        this.scheduledList = new ArrayList();
    }

    public void setCompletedCloudlets(Set<ContainerCloudlet> completedCloudlets) {
        this.completedCloudlets = completedCloudlets;
    }

    public Set<ContainerCloudlet> getCompletedCloudlets() {
        return completedCloudlets;
    }

    public Set<Integer> getAlreadyNewVmCloudletId() {
        return alreadyNewVmCloudletId;
    }

    public void setAlreadyNewVmCloudletId(Set<Integer> alreadyNewVmCloudletId) {
        this.alreadyNewVmCloudletId = alreadyNewVmCloudletId;
    }

    public void setAlreadyNewContainerCloudletId(Set<Integer> alreadyNewContainerCloudletId) {
        this.alreadyNewContainerCloudletId = alreadyNewContainerCloudletId;
    }

    public Set<Integer> getAlreadyNewContainerCloudletId() {
        return alreadyNewContainerCloudletId;
    }

    public void setVmPlanToCreateContainer(Map<Integer, Integer> vmPlanToCreateContainer) {
        this.vmPlanToCreateContainer = vmPlanToCreateContainer;
    }

    public Map<Integer, Integer> getVmPlanToCreateContainer() {
        return vmPlanToCreateContainer;
    }

    public Map<Integer, Integer> getVmsCreatingContainer() {
        return vmsCreatingContainer;
    }

    public void setVmsCreatingContainer(Map<Integer, Integer> vmsCreatingContainer) {
        this.vmsCreatingContainer = vmsCreatingContainer;
    }

    public void setAverageDummyIdleTime(List<Double> averageDummyIdleTime) {
        this.averageDummyIdleTime = averageDummyIdleTime;
    }

    public List<Double> getDummyIdleTime() {
        return averageDummyIdleTime;
    }

    public void setToCreateVmMap(Map<Integer, ContainerVm> toCreateVmMap) {
        this.toCreateVmMap = toCreateVmMap;
    }

    public Map<Integer, ContainerVm> getToCreateVmMap() {
        return toCreateVmMap;
    }

    public <T extends ContainerVm> void setToCreateVmList(Map<Integer, ContainerVm> map) {
        toCreateVmList = new ArrayList<>(map.values());
    }


    public <T extends ContainerVm> void setToDestroyVmList(List<T> list) {
        toDestroyVmList = list;
    }

    public <T extends Container> void setToCreateContainerList(List<T> list) {
        toCreateContainerList = list;
    }

    public <T extends ContainerCloudlet> void setDetainedCloudletList(List<T> list) {
        detainedCloudletList = list;
    }

//    public <T extends Container> void setToDestroyContainerList(List<T> list){
//        toDestroyContainerList = list;
//    }
//
//    public <T extends Container> List<T> getToCreateContainerList(){
//        return (List<T>)toCreateContainerList;
//    }
//
//    public <T extends Container> List<T> getToDestroyContainerList(){
//        return (List<T>)toDestroyContainerList;
//    }

    public <T extends ContainerVm> List<T> getToCreateVmList() {
        return (List<T>) toCreateVmList;
    }

    public <T extends ContainerVm> List<T> getToDestroyVmList() {
        return (List<T>) toDestroyVmList;
    }

    public <T extends ContainerCloudlet> List<T> getDetainedCloudletList() {
        return (List<T>) detainedCloudletList;
    }

    /**
     * Sets the job list.
     *
     * @param list
     */
    @Override
    public void setCloudletList(List list) {
        this.cloudletList = list;
    }

    /**
     * Sets the vm list
     *
     * @param list
     */
    @Override
    public void setVmList(List list) {
        this.vmList = new ArrayList(list);
    }

    /**
     * Gets the job list.
     *
     * @return the job list
     */
    @Override
    public List getCloudletList() {
        return this.cloudletList;
    }

    /**
     * Gets the vm list
     *
     * @return the vm list
     */
    @Override
    public List getVmList() {
        return this.vmList;
    }

    /**
     * The main function
     * @throws java.lang.Exception
     */
    @Override
    public abstract void run() throws Exception;

    public void run(int brokerId) {
    }

    ;

    /**
     * Gets the scheduled job list
     *
     * @return job list
     */
    @Override
    public List getScheduledList() {
        return this.scheduledList;
    }

    public <T extends ContainerCloudlet> void setLastSentCloudlets(List<T> lastSentCloudlets) {
        this.lastSentCloudlets = lastSentCloudlets;
    }

    public <T extends ContainerCloudlet> List<T> getLastSentCloudlets() {
        return (List<T>) lastSentCloudlets;
    }

    public <T extends ContainerCloudlet> void setReplicateCloudlets(List<T> replicateCloudlets) {
        this.replicateCloudlets = replicateCloudlets;
    }

    public <T extends ContainerCloudlet> List<T> getReplicateCloudlets() {
        return (List<T>) replicateCloudlets;
    }

    public <T extends ContainerCloudlet> void setHasSentReplicateCloudlets(Set<T> hasSentReplicateCloudlets) {
        this.hasSentReplicateCloudlets = hasSentReplicateCloudlets;
    }

    public <T extends ContainerCloudlet> Set<T> getHasSentReplicateCloudlets() {
        return (Set<T>) hasSentReplicateCloudlets;
    }

    public Map<Integer, ContainerVm> getCloudletsLeadToNewVm() {
        return cloudletsLeadToNewVm;
    }

    public void setCloudletsLeadToNewVm(Map<Integer, ContainerVm> cloudletsLeadToNewVm) {
        this.cloudletsLeadToNewVm = cloudletsLeadToNewVm;
    }

    /**
     * 每隔一段时间记录虚拟机数量和任务数量
     * @param vmNum 要记录的虚拟机数量
     * @param cloudletNum 要记录的任务数量
     * @param interval 记录的时间间隔
     */
    protected void recordVmNumCloudletNum(int vmNum, int cloudletNum, int numCloudletRemain, double interval) {
        double time = CloudSim.clock();
        if (time - WFCDeadline.lastRecordNumTime < interval){
            return;
        }
        WFCDeadline.vmNumCloudletNum.put(time, new int[]{vmNum, cloudletNum, numCloudletRemain});
        WFCDeadline.lastRecordNumTime = time;
    }
}
