package com.mlk.maxCompute.tunnel;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;
import java.util.Date;

public class DownloadSample {
    private static String accessId = "LTAI4G92AqeB3rGHyGbXrG9Z";
    private static String accessKey = "8IsqCwsNbt7FM89efZiWeT9o72nqXo";
    private static String tunnelUrl = "http://dt.cn-beijing.maxcompute.aliyun.com";
    private static String odpsUrl = "http://service.cn-beijing.maxcompute.aliyun.com/api";
    private static String project = "mlktest";
    private static String table = "test";
//    private static String partition = "<your partition spec>";

    public static void main(String args[]) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelUrl);
//        PartitionSpec partitionSpec = new PartitionSpec(partition);
        try {
            DownloadSession downloadSession = tunnel.createDownloadSession(project, table);
            System.out.println("Session Status is : "
                    + downloadSession.getStatus().toString());
            long count = downloadSession.getRecordCount();
            System.out.println("RecordCount is: " + count);
            RecordReader recordReader = downloadSession.openRecordReader(0, count);
            Record record;
            while ((record = recordReader.read()) != null) {
                consumeRecord(record, downloadSession.getSchema());
            }
            recordReader.close();
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
    private static void consumeRecord(Record record, TableSchema schema) { for (int i = 0; i < schema.getColumns().size(); i++) {
        Column column = schema.getColumn(i); String colValue = null;
        switch (column.getType()) {
            case BIGINT: {
                Long v = record.getBigint(i);
                colValue = v == null ? null : v.toString();
                break;
            }
            case BOOLEAN: {
                Boolean v = record.getBoolean(i); colValue = v == null ? null : v.toString();
                break;
            }
            case DATETIME: {
                Date v = record.getDatetime(i); colValue = v == null ? null : v.toString();
                break;
            }
            case DOUBLE: {
                Double v = record.getDouble(i); colValue = v == null ? null : v.toString();
                break;
            }
            case STRING: {
                String v = record.getString(i);
                colValue = v == null ? null : v.toString();
                break;
            }
            default:
                throw new RuntimeException("Unknown column type: "
                        + column.getType());
        }
        System.out.print(colValue == null ? "null" : colValue);
        if (i != schema.getColumns().size())
            System.out.print("\t");
    }
        System.out.println();
    }
}