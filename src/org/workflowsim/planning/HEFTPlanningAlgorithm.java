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
package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * The HEFT planning algorithm.
 *
 * @author Pedro Paulo Vezzá Campos
 * @date Oct 12, 2013
 */
public class HEFTPlanningAlgorithm extends BasePlanningAlgorithm {

    private Map<Task, Map<ContainerVm, Double>> computationCosts;
    private Map<Task, Map<Task, Double>> transferCosts;
    private Map<Task, Double> rank;
    private Map<ContainerVm, List<Event>> schedules;
    private Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    private class Event {

        public double start;
        public double finish;

        public Event(double start, double finish) {
            this.start = start;
            this.finish = finish;
        }
    }

    private class TaskRank implements Comparable<TaskRank> {

        public Task task;
        public Double rank;

        public TaskRank(Task task, Double rank) {
            this.task = task;
            this.rank = rank;
        }

        @Override
        public int compareTo(TaskRank o) {
            return o.rank.compareTo(rank);
        }
    }

    public HEFTPlanningAlgorithm() {
        computationCosts = new HashMap<>();
        transferCosts = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
        schedules = new HashMap<>();
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Log.printLine("HEFT planner running with " + getTaskList().size()
                + " tasks.");

        //VM的BW可以在WFCConstants中设定
        //这里带宽的单位是Mb/s
        averageBandwidth = calculateAverageBandwidth();

        for (Object vmObject : getVmList()) {
            ContainerVm vm = (ContainerVm) vmObject;
            schedules.put(vm, new ArrayList<>());
        }

        // Prioritization phase
        //计算出了一个表格，包括每个任务在每个虚拟机上的时间
        calculateComputationCosts();
        //计算出了一个表格，包括每个父任务到它的每个子任务的传输时间
        calculateTransferCosts();
        //计算每一个任务的rank，rank的计算方式为取该任务的每个子任务，加对应的传输时间，取最大
        calculateRanks();
        // Selection phase
        // 因为WFCDatacenter中更新任务运行情况的粒度较粗，可能产生误差，
        // 所以修改findFinishTime()中“插空”的条件，加上WFCConstants.MIN_TIME_BETWEEN_EVENTS
        allocateTasks();
        Map<Integer, Integer> res = new HashMap<>();
        for(Task task : getTaskList()){
            res.put(task.getCloudletId(), task.getVmId());
        }
        Log.printConcatLine(CloudSim.clock(), ":");
    }

    /**
     * Calculates the average available bandwidth among all VMs in Mbit/s
     *
     * @return Average available bandwidth in Mbit/s
     */
    private double calculateAverageBandwidth() {
        double avg = 0.0;
        for (Object vmObject : getVmList()) {
            ContainerVm vm = (ContainerVm) vmObject;
            avg += vm.getBw();
        }
        return avg / getVmList().size();
    }

    /**
     * Populates the computationCosts field with the time in seconds to compute
     * a task in a vm.
     */
    private void calculateComputationCosts() {
        for (Task task : getTaskList()) {
            //通过查看该任务的所有输入数据和输出数据，计算传输时间，作为多余的任务长度
            double acc = 0.0;
            List<FileItem> fileItemList = task.getFileList();
            for(FileItem fileItem : fileItemList){
                //检查这个文件是不是input标签的，并且没有和另一个output标签的文件重名
                if(fileItem.isRealInputFile(fileItemList)){
                    acc += fileItem.getSize();
                }else if(fileItem.getType() == Parameters.FileType.OUTPUT){
                    acc += fileItem.getSize();
                }
            }
            acc = acc / Consts.MILLION;
            double extraCloudletLength = (acc * 8 / averageBandwidth) * WFCConstants.EXTRA_LARGE_MIPS;


            Map<ContainerVm, Double> costsVm = new HashMap<>();
            for (Object vmObject : getVmList()) {
                ContainerVm vm = (ContainerVm) vmObject;
                if (vm.getNumberOfPes() < task.getNumberOfPes()) {
                    costsVm.put(vm, Double.MAX_VALUE);
                } else {
                    //这里加上自己计算写的额外长度，实际上是传输数据的时间
                    costsVm.put(vm,
                            (task.getCloudletTotalLength() + extraCloudletLength) / vm.getType().getMips());
                }
            }
            computationCosts.put(task, costsVm);
        }
    }

    /**
     * Populates the transferCosts map with the time in seconds to transfer all
     * files from each parent to each child
     */
    private void calculateTransferCosts() {
        // Initializing the matrix
        for (Task task1 : getTaskList()) {
            Map<Task, Double> taskTransferCosts = new HashMap<>();
            for (Task task2 : getTaskList()) {
                taskTransferCosts.put(task2, 0.0);
            }
            transferCosts.put(task1, taskTransferCosts);
        }

        // Calculating the actual values
        // 这段注释掉，因为数据传输的时间已经算在额外的任务长度里了
//        for (Task parent : getTaskList()) {
//            for (Task child : parent.getChildList()) {
//                transferCosts.get(parent).put(child,
//                        calculateTransferCost(parent, child));
//            }
//        }
    }

    /**
     * Accounts the time in seconds necessary to transfer all files described
     * between parent and child
     *
     * @param parent
     * @param child
     * @return Transfer cost in seconds
     */
    private double calculateTransferCost(Task parent, Task child) {
        List<FileItem> parentFiles = parent.getFileList();
        List<FileItem> childFiles = child.getFileList();

        double acc = 0.0;

        for (FileItem parentFile : parentFiles) {
            if (parentFile.getType() != Parameters.FileType.OUTPUT) {
                continue;
            }

            for (FileItem childFile : childFiles) {
                if (childFile.getType() == Parameters.FileType.INPUT
                        && childFile.getName().equals(parentFile.getName())) {
                    acc += childFile.getSize();
                    break;
                }
            }
        }

        //file Size is in Bytes, acc in MB
        acc = acc / Consts.MILLION;
        // acc in MB, averageBandwidth in Mb/s
        return acc * 8 / averageBandwidth;
    }

    /**
     * Invokes calculateRank for each task to be scheduled
     */
    private void calculateRanks() {
        for (Task task : getTaskList()) {
            calculateRank(task);
        }
    }

    /**
     * Populates rank.get(task) with the rank of task as defined in the HEFT
     * paper.
     *
     * @param task The task have the rank calculates
     * @return The rank
     */
    private double calculateRank(Task task) {
        if (rank.containsKey(task)) {
            return rank.get(task);
        }

        double averageComputationCost = 0.0;

        for (Double cost : computationCosts.get(task).values()) {
            averageComputationCost += cost;
        }

        averageComputationCost /= computationCosts.get(task).size();

        double max = 0.0;
        for (Task child : task.getChildList()) {
            double childCost = transferCosts.get(task).get(child)
                    + calculateRank(child);
            max = Math.max(max, childCost);
        }

        rank.put(task, averageComputationCost + max);

        return rank.get(task);
    }

    /**
     * Allocates all tasks to be scheduled in non-ascending order of schedule.
     */
    private void allocateTasks() {
        List<TaskRank> taskRank = new ArrayList<>();
        for (Task task : rank.keySet()) {
            taskRank.add(new TaskRank(task, rank.get(task)));
        }

        // Sorting in non-ascending order of rank
        Collections.sort(taskRank);
        for (TaskRank rank : taskRank) {
            allocateTask(rank.task);
        }

    }

    /**
     * Schedules the task given in one of the VMs minimizing the earliest finish
     * time
     *
     * @param task The task to be scheduled
     * @pre All parent tasks are already scheduled
     */
    private void allocateTask(Task task) {
        ContainerVm chosenVM = null;
        double earliestFinishTime = Double.MAX_VALUE;
        double bestReadyTime = 0.0;
        double finishTime;

        //依次尝试每个VM，得到在该VM上的最早的完成时间，选择其中最早的
        for (Object vmObject : getVmList()) {
            ContainerVm vm = (ContainerVm) vmObject;
            double minReadyTime = 0.0;
            //如果任务分配到这个VM，这个任务准备好的时间
            for (Task parent : task.getParentList()) {
                double readyTime = earliestFinishTimes.get(parent);
//                if (parent.getVmId() != vm.getId()) {
//                    readyTime += transferCosts.get(parent).get(task);
//                }
                //因为实际上使用中心存储，所以不允许通过两个任务在同一个VM来节省传输时间
                readyTime += transferCosts.get(parent).get(task);
                minReadyTime = Math.max(minReadyTime, readyTime);
            }

            // 找到这个任务在这个VM上最早完成的时间，看看是不是所有VM中最早的
            // 因为WFCDatacenter中更新任务运行情况的粒度较粗，可能产生误差，
            // 所以修改findFinishTime()中“插空”的条件，加上WFCConstants.MIN_TIME_BETWEEN_EVENTS
            finishTime = findFinishTime(task, vm, minReadyTime, false);
            if (finishTime < earliestFinishTime) {
                bestReadyTime = minReadyTime;
                earliestFinishTime = finishTime;
                chosenVM = vm;
            }
        }

        findFinishTime(task, chosenVM, bestReadyTime, true);
        earliestFinishTimes.put(task, earliestFinishTime);

        task.setVmId(chosenVM.getId());
    }

    /**
     * Finds the best time slot available to minimize the finish time of the
     * given task in the vm with the constraint of not scheduling it before
     * readyTime. If occupySlot is true, reserves the time slot in the schedule.
     *
     * @param task The task to have the time slot reserved
     * @param vm The vm that will execute the task
     * @param readyTime The first moment that the task is available to be
     * scheduled
     * @param occupySlot If true, reserves the time slot in the schedule.
     * @return The minimal finish time of the task in the vmn
     */
    private double findFinishTime(Task task, ContainerVm vm, double readyTime,
                                  boolean occupySlot) {
        List<Event> sched = schedules.get(vm);
        double computationCost = computationCosts.get(task).get(vm);
        double start, finish;
        int pos;

        if (sched.isEmpty()) {
            if (occupySlot) {
                sched.add(new Event(readyTime, readyTime + computationCost));
            }
            return readyTime + computationCost;
        }
        //安排新任务到VM的时候，要不然是在已有的间隔之后，要不然在之前，默认在之后
        if (sched.size() == 1) {
            if (readyTime >= sched.get(0).finish + WFCConstants.HEFT_INTERVAL) {
                pos = 1;
                start = readyTime;
            } else if (readyTime + computationCost <= sched.get(0).start - WFCConstants.HEFT_INTERVAL) {
                pos = 0;
                start = readyTime;
            } else {
                pos = 1;
                start = sched.get(0).finish + WFCConstants.HEFT_INTERVAL;
            }

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }

        //如果该VM上已安排的超过了1个，就找空隙，尝试插入该任务
        // Trivial case: Start after the latest task scheduled
        start = Math.max(readyTime, sched.get(sched.size() - 1).finish + WFCConstants.HEFT_INTERVAL);
        finish = start + computationCost;
        int i = sched.size() - 1;
        int j = sched.size() - 2;
        pos = i + 1;
        while (j >= 0) {
            Event current = sched.get(i);
            Event previous = sched.get(j);

            if (readyTime > previous.finish + WFCConstants.HEFT_INTERVAL) {
                if (readyTime + computationCost <= current.start - WFCConstants.HEFT_INTERVAL) {
                    start = readyTime;
                    finish = readyTime + computationCost;
                }
                break;
            }
            if (previous.finish + WFCConstants.HEFT_INTERVAL + computationCost <= current.start - WFCConstants.HEFT_INTERVAL) {
                start = previous.finish + WFCConstants.HEFT_INTERVAL;
                finish = previous.finish + computationCost + WFCConstants.HEFT_INTERVAL;
                pos = i;
            }
            i--;
            j--;
        }

        if (readyTime + computationCost <= sched.get(0).start - WFCConstants.HEFT_INTERVAL) {
            pos = 0;
            start = readyTime;

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }
        if (occupySlot) {
            sched.add(pos, new Event(start, finish));
        }
        return finish;
    }
}
