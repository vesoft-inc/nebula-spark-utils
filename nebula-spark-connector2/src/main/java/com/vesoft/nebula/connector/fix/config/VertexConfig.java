/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package com.vesoft.nebula.connector.fix.config;


import java.io.Serializable;

/**
 * Description  VertexConfig is used for
 *
 * @author huangzhaolai-jk
 * Date  2021/7/28 - 16:01
 * @version 1.0.0
 */
public class VertexConfig implements Serializable {

    private String tag;

    private String idField;

    private String keyPolicy;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }

    public String getKeyPolicy() {
        return keyPolicy;
    }

    public void setKeyPolicy(String keyPolicy) {
        this.keyPolicy = keyPolicy;
    }
}
