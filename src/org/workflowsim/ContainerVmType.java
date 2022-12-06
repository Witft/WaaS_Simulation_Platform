package org.workflowsim;

import org.wfc.core.WFCConstants;

/**
 * 这个类是新增的，用来计算每个VM的花费
 */
public enum ContainerVmType {
    EXTRA_LARGE(WFCConstants.EXTRA_LARGE_PRICE, WFCConstants.RENT_INTERVAL, WFCConstants.EXTRA_LARGE_MIPS),
    LARGE(WFCConstants.LARGE_PRICE, WFCConstants.RENT_INTERVAL, WFCConstants.LARGE_MIPS),
    MEDIUM(WFCConstants.MEDIUM_PRICE, WFCConstants.RENT_INTERVAL, WFCConstants.MEDIUM_MIPS),
    SMALL(WFCConstants.SMALL_PRICE, WFCConstants.RENT_INTERVAL, WFCConstants.SMALL_MIPS);

    private double costPerInterval;
    private double interval;
    private double mips;

    ContainerVmType(double cost, double interval, double mips) {
        this.costPerInterval = cost;
        this.interval = interval;
        this.mips = mips;
    }

    public double getMips() {
        return mips;
    }

    public double getCostPerInterval(){
        return costPerInterval;
    }

    public double getInterval(){
        return interval;
    }
}
