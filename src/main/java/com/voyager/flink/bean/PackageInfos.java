package com.voyager.flink.bean;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.Serializable;

public class PackageInfos implements Serializable {

    private IntType id;
    private VarCharType code;
    private VarCharType name;

    public PackageInfos() {
    }

    public PackageInfos(IntType id, VarCharType code, VarCharType name) {
        this.id = id;
        this.code = code;
        this.name = name;
    }

    public IntType getId() {
        return id;
    }

    public void setId(IntType id) {
        this.id = id;
    }

    public VarCharType getCode() {
        return code;
    }

    public void setCode(VarCharType code) {
        this.code = code;
    }

    public VarCharType getName() {
        return name;
    }

    public void setName(VarCharType name) {
        this.name = name;
    }
}
