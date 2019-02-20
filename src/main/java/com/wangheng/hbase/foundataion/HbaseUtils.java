package com.wangheng.hbase.foundataion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
//import org.apache.hadoop.

public class HbaseUtils {
    private static Configuration conf;
    private static Connection conn;


    @Before
    public void before() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conn = ConnectionFactory.createConnection(conf);
    }


    /**
     * list all tables in HBse.
     * @throws IOException
     */
    @Test
    public void list() throws IOException {
        Admin admin = conn.getAdmin();
        System.out.println("list the table");
        for(TableName name: admin.listTableNames()){
            System.out.println(name);
        }
        conn.close();
    }

    /**
     * Creating table
     */
    @Test
    public void createTable() throws IOException {
        String tableName = "table_test";
        String[] familyNames = {"name", "score", "passwd"};
        Admin admin = conn.getAdmin();
        System.out.println("Creating table");
        TableName tn = TableName.valueOf("table_test");
        if(admin.tableExists(tn)){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        //HTableDescribe includes all table's name and it's cols
        HTableDescriptor htd = new HTableDescriptor(tn);
        for(String family: familyNames){
            htd.addFamily(new HColumnDescriptor(family));
        }
        admin.createTable(htd);
        conn.close();
        System.out.println("create success!");

    }

    /**
     * Describing table
     */
    @Test
    public void describeTable() throws IOException{
        String tableName = "table_test";
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        System.out.println("==================describing " + tableName + ":======================");
        for(HColumnDescriptor hcd: htd.getColumnFamilies()){
            System.out.println(hcd.getNameAsString());
        }
        System.out.println("=============================end====================================");
        conn.close();
    }

    /**
     * Adding column family
     */
    @Test
    public void addColumnFamily() throws IOException{
        String table_name = "table_test";
        String []adds = {"add1", "add2", "add3"};
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(table_name);
        HTableDescriptor htd = new HTableDescriptor(tn);
        for(String add:adds){
            htd.addFamily(new HColumnDescriptor(add));
        }
        admin.modifyTable(tn, htd);
        conn.close();
        System.out.println("modify success!");
    }


    /**
     *
     * @param args
     * @throws IOException
     */

    public static void main(String[] args) throws IOException {

    }




}
