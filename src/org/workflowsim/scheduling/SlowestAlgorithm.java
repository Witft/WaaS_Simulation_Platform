package org.workflowsim.scheduling;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.container.core.ContainerCloudlet;
import org.cloudbus.cloudsim.container.core.ContainerVm;
import org.cloudbus.cloudsim.container.schedulers.ContainerCloudletScheduler;
import org.cloudbus.cloudsim.core.CloudSim;
import org.wfc.core.WFCConstants;

import java.text.DecimalFormat;

public class SlowestAlgorithm extends JustToTryAlgorithm{
    @Override
    public void run(int brokerId) {
        int numCloudlet = getCloudletList().size();
        int numVM = getVmList().size();
        if (WFCConstants.PRINT_SCHEDULE_WAITING_LIST) {
            Log.printLine(new DecimalFormat("#.00").format(CloudSim.clock()) + ":还有" + numCloudlet + "个任务等待调度");
        }
        //在任务正式调度前，先默认它们没有找到适合的VM
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            cloudlet.setVmId(-1000);
        }

        //对于每一个待调度的任务
        for (int i = 0; i < numCloudlet; i++) {
            ContainerCloudlet cloudlet = (ContainerCloudlet) getCloudletList().get(i);
            //单独处理stage-in任务
            if (cloudlet.getWorkflowId() == 0 && numVM > 0) {
                ContainerVm containerVm = (ContainerVm) getVmList().get(0);
                cloudlet.setVmId(containerVm.getId());
                break;
            }
            ContainerVm containerVm = (ContainerVm) getVmList().get(0);
            ContainerCloudletScheduler containerCloudletScheduler = containerVm.getContainerList().get(0).getContainerCloudletScheduler();
            if(containerCloudletScheduler.getCloudletExecList().size() == 0 && containerCloudletScheduler.getCloudletWaitingList().size() == 0){
                cloudlet.setVmId(1);
            }
            break;
        }
    }
}
