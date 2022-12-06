package org.wfc.core;

import org.cloudbus.cloudsim.Cloudlet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WFCReplication {
    // 记录使用复制容错时，从副本到主副本的映射
    private static Map<Cloudlet, Cloudlet> slaveToMasterMap = new HashMap<>();
    // 后来复制或者重新提交的任务，到它的起源任务的映射
    private static Map<Cloudlet, Cloudlet> toOrigin = new HashMap<>();
    // 记录使用复制容错时，主副本到从副本的映射
    private static Map<Cloudlet, Set<Cloudlet>> masterToSlaveMap = new HashMap<>();
    // 重新提交容错时，旧任务到新任务的映射
    private static Map<Cloudlet, Cloudlet> oldResubmitToNew = new HashMap<>();
    // 记录重新提交任务的次数
    public static int retry = 0;
    // 记录还有任务未提交到WFCScheduler的工作流id
    public static Set<Integer> unFinishedWorkflowIds = new HashSet<>();

    public static Map<Cloudlet, Cloudlet> getSlaveToMasterMap() {
        return slaveToMasterMap;
    }

    public static Map<Cloudlet, Cloudlet> getToOrigin() {
        return toOrigin;
    }

    public static Map<Cloudlet, Cloudlet> getOldResubmitToNew() {
        return oldResubmitToNew;
    }

    public static Map<Cloudlet, Set<Cloudlet>> getMasterToSlaveMap() {
        return masterToSlaveMap;
    }




}
