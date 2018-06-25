package com.iuicity.bulkImport.rowkey;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class HashChoreWoker implements SplitKeysCalculator{
    private long baseRecord;
    private RowKeyGenerator rkGen;
    private long splitKeysBase;
    private int splitKeysNumber;
    private byte[][] splitKeys;

    public HashChoreWoker(long baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;
        rkGen = new HashRowKeyGenerator();
        splitKeysNumber = prepareRegions - 1;
        splitKeysBase = baseRecord / prepareRegions;
    }

    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        int pointer = 0;

        Iterator<byte[]> rowKeyIter = rows.iterator();

        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index ++;
                }
            }
            pointer ++;
        }

        rows.clear();
        rows = null;
        return splitKeys;
    }

    public static void main(String[] args) throws Exception {
    	System.out.println("start ====================");
        HashChoreWoker worker = new HashChoreWoker(5000,10);
        byte [][] splitKeys = worker.calcSplitKeys();
    	Configuration conf = HBaseConfiguration.create();
    	conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
    	conf.set("hbase.zookeeper.property.clientPort", "2181");
        
        @SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(conf);
        TableName tableName = TableName.valueOf("phone_md5");
        
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (Exception e) {
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("md5"));
        columnDesc.setMaxVersions(1);
        tableDesc.addFamily(columnDesc);
        admin.createTable(tableDesc ,splitKeys);
        admin.close();
        System.out.println("end ====================");
    }

}
