/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.ivyft.katta.protocol;

@SuppressWarnings("all")
/** Katta Process Interface */
@org.apache.avro.specific.AvroGenerated
public interface KattaClientProtocol {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"KattaClientProtocol\",\"namespace\":\"com.ivyft.katta.protocol\",\"doc\":\"Katta Process Interface\",\"name\":\"KattaClient\",\"types\":[{\"type\":\"record\",\"name\":\"Message\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"payload\",\"type\":\"bytes\"}]}],\"messages\":{\"add\":{\"doc\":\"插入单条数据\",\"request\":[{\"name\":\"message\",\"type\":\"Message\"}],\"response\":\"int\"},\"addList\":{\"doc\":\"批次插入(List)\",\"request\":[{\"name\":\"messages\",\"type\":{\"type\":\"array\",\"items\":\"Message\"}}],\"response\":\"int\"},\"commit\":{\"doc\":\"提交\",\"request\":[],\"response\":\"int\"},\"rollback\":{\"doc\":\"提交失败回滚\",\"request\":[],\"response\":\"int\"},\"close\":{\"doc\":\"关闭接口\",\"request\":[],\"response\":\"int\"}}}");
  /** 插入单条数据 */
  int add(com.ivyft.katta.protocol.Message message) throws org.apache.avro.AvroRemoteException;
  /** 批次插入(List) */
  int addList(java.util.List<com.ivyft.katta.protocol.Message> messages) throws org.apache.avro.AvroRemoteException;
  /** 提交 */
  int commit() throws org.apache.avro.AvroRemoteException;
  /** 提交失败回滚 */
  int rollback() throws org.apache.avro.AvroRemoteException;
  /** 关闭接口 */
  int close() throws org.apache.avro.AvroRemoteException;

  @SuppressWarnings("all")
  /** Katta Process Interface */
  public interface Callback extends KattaClientProtocol {
    public static final org.apache.avro.Protocol PROTOCOL = com.ivyft.katta.protocol.KattaClientProtocol.PROTOCOL;
    /** 插入单条数据 */
    void add(com.ivyft.katta.protocol.Message message, org.apache.avro.ipc.Callback<java.lang.Integer> callback) throws java.io.IOException;
    /** 批次插入(List) */
    void addList(java.util.List<com.ivyft.katta.protocol.Message> messages, org.apache.avro.ipc.Callback<java.lang.Integer> callback) throws java.io.IOException;
    /** 提交 */
    void commit(org.apache.avro.ipc.Callback<java.lang.Integer> callback) throws java.io.IOException;
    /** 提交失败回滚 */
    void rollback(org.apache.avro.ipc.Callback<java.lang.Integer> callback) throws java.io.IOException;
    /** 关闭接口 */
    void close(org.apache.avro.ipc.Callback<java.lang.Integer> callback) throws java.io.IOException;
  }
}