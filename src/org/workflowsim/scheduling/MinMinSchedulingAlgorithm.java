/**
 * Copyright 2019-2020 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.WorkflowSimTags;
import org.cloudbus.cloudsim.container.core.*;

/**
 * MinMin algorithm.
 *
 * @author Arman Riazi
 * @since WorkflowSim Toolkit 1.0
 * @date March 29, 2020
 */
public class MinMinSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public MinMinSchedulingAlgorithm() {
        super();
    }
    private final List<Boolean> hasChecked = new ArrayList<>();

    //为什么这个方法中为任务分配VM没有考虑VM的性能是否足够？
    @Override
    public void run() {

        int size = getCloudletList().size();
        hasChecked.clear();
        for (int t = 0; t < size; t++) {
            hasChecked.add(false);
        }
        //每运行一次，确定一个任务的VM
        for (int i = 0; i < size; i++) {
            int minIndex = 0;
            Cloudlet minCloudlet = null;
            //找到第一个还没有分配VM的任务
            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = (Cloudlet) getCloudletList().get(j);
                if (!hasChecked.get(j)) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                    break;
                }
            }
            //如果没找到没有被分配的任务，就中断循环
            if (minCloudlet == null) {
                break;
            }

            //找长度最小的任务
            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = (Cloudlet) getCloudletList().get(j);
                if (hasChecked.get(j)) {
                    continue;
                }
                long length = cloudlet.getCloudletLength();
                if (length < minCloudlet.getCloudletLength()) {
                    minCloudlet = cloudlet;
                    minIndex = j;
                }
            }
            hasChecked.set(minIndex, true);

            int vmSize = getVmList().size();
            ContainerVm firstIdleVm = null;//(CondorVM)getVmList().get(0);
            //找到第一个空闲的VM
            for (int j = 0; j < vmSize; j++) {
                ContainerVm vm = (ContainerVm) getVmList().get(j);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {
                    firstIdleVm = vm;
                    break;
                }
            }
            //如果没找到空闲的VM，本次分配任务的过程结束
            if (firstIdleVm == null) {
                break;
            }
            //找到一个空闲并且需求的MIPS最大的VM
            for (int j = 0; j < vmSize; j++) {
                ContainerVm vm = (ContainerVm) getVmList().get(j);
                if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)
                        && vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips()) {
                    firstIdleVm = vm;
                }
            }
            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);
            //为上面找到的最短的任务分配虚拟机
            minCloudlet.setVmId(firstIdleVm.getId());

        // minCloudlet.setContainerId(firstIdleVm.getId());
            
            getScheduledList().add(minCloudlet);
        }
    }
}
