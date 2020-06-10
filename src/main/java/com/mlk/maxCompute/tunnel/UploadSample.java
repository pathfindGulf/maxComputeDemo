package com.mlk.maxCompute.tunnel;


import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;
import java.util.Date;


/**
 *功能描述 Tunnel SDK简单上传
 *@date 2020/6/10
 */
public class UploadSample {
    private static String accessId = "LTAI4G92AqeB3rGHyGbXrG9Z";
    private static String accessKey = "8IsqCwsNbt7FM89efZiWeT9o72nqXo";
    private static String tunnelUrl = "http://dt.cn-beijing.maxcompute.aliyun.com";
    private static String odpsUrl = "http://service.cn-beijing.maxcompute.aliyun.com/api";
    private static String project = "mlktest";
    private static String table = "test";
//    private static String partition = "D:\\maxCompute\\conf\\odps_config - 主账号.ini";
    public static void main(String args[]) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
        try {
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(tunnelUrl);
//            PartitionSpec partitionSpec = new PartitionSpec(partition);
            UploadSession uploadSession = tunnel.createUploadSession(project, table);
            System.out.println("Session Status is : "
                    + uploadSession.getStatus().toString());
            TableSchema schema = uploadSession.getSchema();
            // 准备数据后打开Writer开始写入数据，准备数据后写入一个Block
            // 单个Block内写入数据过少会产生大量小文件严重影响计算性能，强烈建议每次写入64MB以上数据(100GB以内数据均可写入同一Block)
            // 可通过数据的平均大小与记录数量大致计算总量即 64MB < 平均记录大小*记录数 < 100GB
            RecordWriter recordWriter = uploadSession.openRecordWriter(0);
            Record record = uploadSession.newRecord();
            for (int i = 0; i < schema.getColumns().size(); i++) {
                Column column = schema.getColumn(i);
                switch (column.getType()) {
                    case BIGINT:
                        record.setBigint(i, 1L);
                        break;
                    case BOOLEAN:
                        record.setBoolean(i, true);
                        break;
                    case DATETIME:
                        record.setDatetime(i, new Date());
                        break;
                    case DOUBLE:
                        record.setDouble(i, 0.0);
                        break;
                    case STRING:
                        record.setString(i, "sample");
                        break;
                    default:
                        throw new RuntimeException("Unknown column type: "
                                + column.getType());
                }
            }
            for (int i = 0; i < 10; i++) {
                // Write数据至服务端，每写入8KB数据会进行一次网络传输
                // 若120s没有网络传输服务端将会关闭连接，届时该Writer将不可用，需要重新写入
                recordWriter.write(record);
            }
            recordWriter.close();
            uploadSession.commit(new Long[]{0L});
            System.out.println("upload success!");
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}




