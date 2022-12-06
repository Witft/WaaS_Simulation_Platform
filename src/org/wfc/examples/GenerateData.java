package org.wfc.examples;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.wfc.core.WFCDeadline;
import org.wfc.core.WFCWorkload;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateData {

    private static PoissonDistribution numGenPoisson = null;
    private String[] workflows = {"CyberShake_30.xml"};

    public GenerateData() {
    }

    private static double getPossionProbability(int k, double lamda) {
        double c = Math.exp(-lamda), sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= lamda / i;
        }
        return sum * c;
    }

    public static void main(String[] args) {
        System.out.println(WFCDeadline.deadlinesMontageFastest334.length);
    }


    public static void generateDeadlineAccordingToWorkload() {
        DecimalFormat df = new DecimalFormat("#.00");
        Random random = new Random();
        // 生成对应2000个工作流的截止时间，在最短时间与最长时间之间均匀分布
        for(int i = 0; i < 2000; i++){
            double fastTime = WFCDeadline.workflowFastDeadlineMap.get(WFCWorkload.workload2000[i]);
            double slowTime = WFCDeadline.workflowSlowDeadlineMap.get(WFCWorkload.workload2000[i]);
            double realTime = random.nextDouble() * (slowTime - fastTime) + fastTime;
            System.out.print(df.format(realTime));
            System.out.print(",");
            if ((i + 1) % 10 == 0) {
                System.out.println();
            }
        }
    }

    public static void generateDeadlineFast() {
        DecimalFormat df = new DecimalFormat("#.00");
        Random random = new Random();
        // 生成对应2000个工作流的截止时间，为最短的时间
        for(int i = 0; i < 2000; i++){
            double fastTime = WFCDeadline.workflowFastDeadlineMap.get(WFCWorkload.workload2000[i]);
            System.out.print(df.format(fastTime));
            System.out.print(",");
            if ((i + 1) % 10 == 0) {
                System.out.println();
            }
        }
    }

    public static void generateDeadlineMontageFast() {
        DecimalFormat df = new DecimalFormat("#.00");
        Random random = new Random();
        // 生成对应334个Montage工作流的截止时间，为最短的时间
        for(int i = 0; i < 334; i++){
            double fastTime = WFCDeadline.workflowFastDeadlineMap.get(WFCWorkload.workloadMontage334[i]);
            System.out.print(df.format(fastTime));
            System.out.print(",");
            if ((i + 1) % 10 == 0) {
                System.out.println();
            }
        }
    }


    /**
     * 每分钟的均值为 mean
     * @param size
     */
    public static void generateNumPerMinute(int size, double mean) {
        numGenPoisson = new PoissonDistribution(mean);
        int sum = 0;
        int[] samples = numGenPoisson.sample((int)(size / mean));
        for (int i = 0; i < samples.length; i++) {
            sum += samples[i];
            System.out.print(samples[i]);
            System.out.print(",");
            if ((i + 1) % 30 == 0) {
                System.out.println();
            }
        }
        System.out.println("总和：" + sum);
    }

    //origin中记录了每个一分钟里到达了多少个工作流
    public static void generateArrivalTime(int[] origin) {
        List<Integer> res = new ArrayList<>();
        int base = 0;
        for (int i = 0; i < origin.length; i++) {
            base = i * 60;
            for (int j = 0; j < origin[i]; j++) {
                base += 60 / origin[i];
                res.add(base);
            }
        }
        for (int i = 0; i < res.size(); i++) {
            System.out.print(res.get(i));
            System.out.print(",");
            if ((i + 1) % 20 == 0) {
                System.out.println();
            }
        }
    }


    public static void sumCheck(int[] target) {
        int sum = 0;
        for (int i = 0; i < target.length; i++) {
            sum += target[i];
        }
        System.out.println("总和：" + sum);
    }

    public static void generateWorkload(int num) {
        Random random = new Random();
        for (int i = 0; i < num; i++) {
            System.out.print("\"" + WFCWorkload.basicWorkflows[random.nextInt(20)] + "\"");
            System.out.print(",");
            if ((i + 1) % 4 == 0) {
                System.out.println();
            }
        }
    }

    public static void generateWorkloadMontage(int num) {
        Random random = new Random();
        for (int i = 0; i < num; i++) {
            System.out.print("\"" + WFCWorkload.basicWorkflows[random.nextInt(4) + 12] + "\"");
            System.out.print(",");
            if ((i + 1) % 4 == 0) {
                System.out.println();
            }
        }
    }

    public static void readAllFiles(String path) {
        System.out.println("文件有如下：");
        //表示一个文件路径
//        File file = new File("D:\\IDEAwork\\CloudSim_Learning\\Cloudsim-Workflow-Function-Container-master\\config\\dax");
        File file = new File(path);
        //用数组把文件夹下的文件存起来
        File[] files = file.listFiles();
        //foreach遍历数组
        for (File file2 : files) {
            //打印文件列表：只读取名称使用getName();
            if (file2.getName().lastIndexOf("(") == -1) {
                System.out.print("\"" + file2.getName() + "\"");
                System.out.print(",");
            }
        }
    }

}
