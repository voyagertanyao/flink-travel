package com.voyager.flink.bean;

import java.io.Serializable;

public class Device implements Serializable {
    private String mac;
    private String modelName;
    private String sendDate;

    public String getMac() {
        return mac;
    }

    public String getModelName() {
        return modelName;
    }

    public String getSendDate() {
        return sendDate;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public void setSendDate(String sendDate) {
        this.sendDate = sendDate;
    }

    public Device() {
    }

    public Device(String mac, String modelName, String sendDate) {
        this.mac = mac;
        this.modelName = modelName;
        this.sendDate = sendDate;
    }

    @Override
    public String toString() {
        return "Devicel{" +
                "mac='" + mac + '\'' +
                ", modelName='" + modelName + '\'' +
                ", sendDate='" + sendDate + '\'' +
                '}';
    }
}
