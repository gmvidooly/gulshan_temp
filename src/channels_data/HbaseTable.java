package channels_data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseTable {
	 private static Configuration conf = null;
	private static boolean isTableAvailable;
	    /**
	     * Initialization
	     */
	    static {
	        conf = HBaseConfiguration.create();
	    }
	 
	    /**
	     * Create a table
	     */
	    public static void creatTable(String tableName, String[] familys)
	            throws Exception {
	    	Connection connection = ConnectionFactory.createConnection(conf);
	    	Admin admin = connection.getAdmin();
	        if (admin.tableExists(TableName.valueOf(tableName))) {
	            System.out.println("table already exists!");
	        } else {
	            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
	            for (int i = 0; i < familys.length; i++) {
	                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
	            }
	            admin.createTable(tableDesc);
	            System.out.println("create table " + tableName + " ok.");
	        }
	    }
	    
	    public static void createTable(String tableName, String dnsId, int hbaseRestPort) {
			Configuration config = HBaseConfiguration.create();
//			config.addResource(file);
			 config.clear();
			 config.set("hbase.master", "172-31-7-228:60010"); 
             config.set("hbase.zookeeper.quorum", "172-31-7-228");
             config.set("hbase.zookeeper.property.clientPort","2181");
			RemoteAdmin admin = new RemoteAdmin(new Client(new Cluster().add(dnsId, hbaseRestPort)), config);
			String [] families = {"user", "address", "contact", "likes"};
			try {
				System.out.println("admin "+admin.getClusterStatus());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				if (admin.isTableAvailable(tableName)) {
					System.out.println("table already exists!");
					return;
				} else {
					HTableDescriptor tableDesc = new HTableDescriptor(tableName);
					for (int i = 0; i < families.length; i++) {
						tableDesc.addFamily(new HColumnDescriptor(families[i]));
					}
					admin.createTable(tableDesc);
					isTableAvailable = true;
					System.out.println("create table " + tableName + " ok.");
				} 

			} catch (IOException e) {
				e.printStackTrace(); 
			}

		}
	 
	    /**
	     * Delete a table
	     */
	    public static void deleteTable(String tableName) throws Exception {
	        try {
	            HBaseAdmin admin = new HBaseAdmin(conf);
	            admin.disableTable(tableName);
	            admin.deleteTable(tableName);
	            System.out.println("delete table " + tableName + " ok.");
	        } catch (MasterNotRunningException e) {
	            e.printStackTrace();
	        } catch (ZooKeeperConnectionException e) {
	            e.printStackTrace();
	        }
	    }
	 
	    /**
	     * Put (or insert) a row
	     */
	    public static void addRecord(String tableName, String rowKey,
	            String family, String qualifier, String value) throws Exception {
	        try {
	            HTable table = new HTable(conf, tableName);
	            Put put = new Put(Bytes.toBytes(rowKey));
	            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
	                    .toBytes(value));
	            table.put(put);
	            System.out.println("insert recored " + rowKey + " to table "
	                    + tableName + " ok.");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	 
	    /**
	     * Delete a row
	     */
	    public static void delRecord(String tableName, String rowKey)
	            throws IOException {
	        HTable table = new HTable(conf, tableName);
	        List<Delete> list = new ArrayList<Delete>();
	        Delete del = new Delete(rowKey.getBytes());
	        list.add(del);
	        table.delete(list);
	        System.out.println("del recored " + rowKey + " ok.");
	    }
	 
	    /**
	     * Get a row
	     */
	    public static void getOneRecord (String tableName, String rowKey) throws IOException{
	        HTable table = new HTable(conf, tableName);
	        Get get = new Get(rowKey.getBytes());
	        Result rs = table.get(get);
	        for(KeyValue kv : rs.raw()){
	            System.out.print(new String(kv.getRow()) + " " );
	            System.out.print(new String(kv.getFamily()) + ":" );
	            System.out.print(new String(kv.getQualifier()) + " " );
	            System.out.print(kv.getTimestamp() + " " );
	            System.out.println(new String(kv.getValue()));
	        }
	    }
	    /**
	     * Scan (or list) a table
	     */
	    public static void getAllRecord (String tableName) {
	        try{
	             HTable table = new HTable(conf, tableName);
	             Scan s = new Scan();
	             ResultScanner ss = table.getScanner(s);
	             for(Result r:ss){
	                 for(KeyValue kv : r.raw()){
	                    System.out.print(new String(kv.getRow()) + " ");
	                    System.out.print(new String(kv.getFamily()) + ":");
	                    System.out.print(new String(kv.getQualifier()) + " ");
	                    System.out.print(kv.getTimestamp() + " ");
	                    System.out.println(new String(kv.getValue()));
	                 }
	             }
	        } catch (IOException e){
	            e.printStackTrace();
	        }
	    }
	 
	    public static void main(String[] agrs) {
	        try {
	            String tablename = "scores";
	            String[] familys = { "grade", "course" };
//	            HbaseTable.creatTable(tablename, familys);
	            HbaseTable.createTable(tablename, "ec2-54-183-198-42.us-west-1.compute.amazonaws.com", 8080);
	 
//	            // add record zkb
//	            HbaseTable.addRecord(tablename, "zkb", "grade", "", "5");
//	            HbaseTable.addRecord(tablename, "zkb", "course", "", "90");
//	            HbaseTable.addRecord(tablename, "zkb", "course", "math", "97");
//	            HbaseTable.addRecord(tablename, "zkb", "course", "art", "87");
//	            // add record baoniu
//	            HbaseTable.addRecord(tablename, "baoniu", "grade", "", "4");
//	            HbaseTable.addRecord(tablename, "baoniu", "course", "math", "89");
//	 
//	            System.out.println("===========get one record========");
//	            HbaseTable.getOneRecord(tablename, "zkb");
//	 
//	            System.out.println("===========show all record========");
//	            HbaseTable.getAllRecord(tablename);
//	 
//	            System.out.println("===========del one record========");
//	            HbaseTable.delRecord(tablename, "baoniu");
//	            HbaseTable.getAllRecord(tablename);
//	 
//	            System.out.println("===========show all record========");
//	            HbaseTable.getAllRecord(tablename);
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
}
