package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;

/**
 * 这个算法什么都不做，直接执行PlanningAlgorithm的结果
 */
public class InvalidSchedulingAlgorithm extends JustToTryAlgorithm{
    @Override
    public void run(int brokerId) {
        int numCloudlet = getCloudletList().size();
        for(int i = 0; i < numCloudlet; i++){
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            if(cloudlet.getVmId() >= 0 && cloudlet.getVmId() <= WFCConstants.WFC_NUMBER_VMS){
//                Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "已经分配了虚拟机", cloudlet.getVmId());
            }else{
                cloudlet.setVmId(1);
                Log.printConcatLine(CloudSim.clock(), "任务", cloudlet.getCloudletId(), "没有分配虚拟机，因此默认为虚拟机1");
            }
        }
    }
}
