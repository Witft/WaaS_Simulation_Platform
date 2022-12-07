# WaaS_Simulation_Platform
This project is directly based on Cloudsim-Workflow-Function-Container, by **Arman Riazi**. 

Modeling And Simulation Of Running Workflow In the **Workflow as a Service platforms**.

## Getting Started

You can just open this project with IDEA.

The simulation can be started by running of src/org/wfc/examples/MyWFCExample3.java.

There are parameters you can set in src/org/wfc/core/WFCConstants.java.

## New Features

To simulate scheduling of real-time workflows, I added some features:

1. Create new virtual machines during processing of workflows.
2. Destroy virtual machines during processing of workflows.
3. Create new containers during processing of workflows.
4. Change containers during processing of workflows. (It is supposed that there is only one container on each virtual machine)
5. Each container can only process tasks of one specific workflow. (The **workflow id** of a cloudlet and container should be the same.)
6. New workflows continuously arrives during processing of existing workflows.
7. Simulation of fault of virtual machines.

## Builted With

* [CloudSim](https://github.com/Cloudslab/cloudsim) - The simulator framework used
* [WorkflowSim](https://github.com/WorkflowSim/) - The simulator workflow framework used
* [ContainerCloudsim](https://github.com/Cloudslab/cloudsim/tree/master/modules/cloudsim-examples/src/main/java/org/cloudbus/cloudsim/examples/container) - The container examples used

## Authors

* **Arman Riazi** - *Initial work* - [ArmanRiazi](https://github.com/armanriazi/)
* **Witft** - Added some code to simulate WaaS platform

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
*Make it with ❤️ for you

## Acknowledgments
