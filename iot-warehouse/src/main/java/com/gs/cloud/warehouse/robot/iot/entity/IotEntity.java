package com.gs.cloud.warehouse.robot.iot.entity;

public class IotEntity {

  String productKey;

  String deviceId;

  String version;

  String namespace;

  String objectName;

  String cldUnixTimestamp;

  String method;

  String data;

  String pt;

  public String getProductKey() {
    return productKey;
  }

  public void setProductKey(String productKey) {
    this.productKey = productKey;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getObjectName() {
    return objectName;
  }

  public void setObjectName(String objectName) {
    this.objectName = objectName;
  }

  public String getCldUnixTimestamp() {
    return cldUnixTimestamp;
  }

  public void setCldUnixTimestamp(String cldUnixTimestamp) {
    this.cldUnixTimestamp = cldUnixTimestamp;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public String getPt() {
    return pt;
  }

  public void setPt(String pt) {
    this.pt = pt;
  }

  @Override
  public String toString() {
    return "IotEntity{" +
        "productKey='" + productKey + '\'' +
        ", deviceId='" + deviceId + '\'' +
        ", version='" + version + '\'' +
        ", namespace='" + namespace + '\'' +
        ", objectName='" + objectName + '\'' +
        ", cldUnixTimestamp='" + cldUnixTimestamp + '\'' +
        ", method='" + method + '\'' +
        ", data='" + data + '\'' +
        ", pt='" + pt + '\'' +
        '}';
  }
}
