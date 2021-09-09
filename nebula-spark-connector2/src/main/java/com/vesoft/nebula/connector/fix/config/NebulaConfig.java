/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.config;

import java.io.Serializable;

/**
 * Description  NebulaConfig is used for
 *
 * @author huangzhaolai-jk
 * Date  2021/7/28 - 14:27
 * @version 1.0.0
 */
public class NebulaConfig implements Serializable {

    private String metaAddress;

    private String graphAddress;


    private int connectionRetryTime = 3;

    private String space;

    private String userName;

    private String password;

    private int partitions = 10;

    private boolean idAsProperty = true;

    private int batchWriteSize = 1000;

    private String rateTimeOut = "500";

    private String rateLimit = "1024";

    public String getRateTimeOut() {
        return rateTimeOut;
    }

    public void setRateTimeOut(String rateTimeOut) {
        this.rateTimeOut = rateTimeOut;
    }

    public String getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(String rateLimit) {
        this.rateLimit = rateLimit;
    }

    public String getMetaAddress() {
        return metaAddress;
    }

    public void setMetaAddress(String metaAddress) {
        this.metaAddress = metaAddress;
    }

    public String getGraphAddress() {
        return graphAddress;
    }

    public void setGraphAddress(String graphAddress) {
        this.graphAddress = graphAddress;
    }

    public int getConnectionRetryTime() {
        return connectionRetryTime;
    }

    public void setConnectionRetryTime(int connectionRetryTime) {
        this.connectionRetryTime = connectionRetryTime;
    }

    public String getSpace() {
        return space;
    }

    public void setSpace(String space) {
        this.space = space;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public boolean isIdAsProperty() {
        return idAsProperty;
    }

    public void setIdAsProperty(boolean idAsProperty) {
        this.idAsProperty = idAsProperty;
    }

    public int getBatchWriteSize() {
        return batchWriteSize;
    }

    public void setBatchWriteSize(int batchWriteSize) {
        this.batchWriteSize = batchWriteSize;
    }
}
