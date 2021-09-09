/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.config;

import java.io.Serializable;

/**
 * Description  EdgeConfig is used for
 *
 * @author huangzhaolai-jk
 * Date  2021/7/28 - 16:01
 * @version 1.0.0
 */
public class EdgeConfig implements Serializable {

    private String edge;

    private String srcKeyPolicy;

    private String dstKeyPolicy;

    private String srcIdField;

    private String dstIdField;

    private Long rank;

    public String getEdge() {
        return edge;
    }

    public void setEdge(String edge) {
        this.edge = edge;
    }

    public String getSrcKeyPolicy() {
        return srcKeyPolicy;
    }

    public void setSrcKeyPolicy(String srcKeyPolicy) {
        this.srcKeyPolicy = srcKeyPolicy;
    }

    public String getDstKeyPolicy() {
        return dstKeyPolicy;
    }

    public void setDstKeyPolicy(String dstKeyPolicy) {
        this.dstKeyPolicy = dstKeyPolicy;
    }

    public String getSrcIdField() {
        return srcIdField;
    }

    public void setSrcIdField(String srcIdField) {
        this.srcIdField = srcIdField;
    }

    public String getDstIdField() {
        return dstIdField;
    }

    public void setDstIdField(String dstIdField) {
        this.dstIdField = dstIdField;
    }

    public Long getRank() {
        return rank;
    }

    public void setRank(Long rank) {
        this.rank = rank;
    }
}
