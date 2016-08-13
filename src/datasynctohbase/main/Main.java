package datasynctohbase.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import net.sf.json.JSONObject;

@SuppressWarnings("deprecation")
public class Main {

	public static Configuration HbaseConfig;
	public static HTablePool tablePool;
	private static String tableName = "IRSTempOnOfflineList";
	private static String path = "/root/datasync/log/test.txt";
	static {
//		System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
		HbaseConfig = HBaseConfiguration.create();
		HbaseConfig.addResource("config/hbase-site.xml");
		tablePool=new HTablePool(HbaseConfig,3000);
	}
	
	public static void doUpdate(){
		HBaseAdmin admin = null;
		HTableInterface table = null;
		FileReader reader = null;
		BufferedReader bufReader = null;
		try {
			//hbase表不存在先创建表
			admin = new HBaseAdmin(HbaseConfig);
			if (!admin.tableExists(tableName)) {
				HTableDescriptor  desc = new HTableDescriptor(tableName);
				String[] familys = {"detail"};
				for (int i = 0; i < familys.length; i++) {
					HColumnDescriptor family = new HColumnDescriptor(familys[i]);			
					desc.addFamily(family);
				}
				admin.createTable(desc);
				System.out.println("Create table \'" + tableName + "\' OK!");
			}
			table = tablePool.getTable(tableName);
			
			reader = new FileReader(new File(path));
			bufReader = new BufferedReader(reader);
			
			for(String line = bufReader.readLine(); line != null;
					line = bufReader.readLine()) {
				if(StringUtils.isNotEmpty(line)){
					String data = line.split(" --- ")[1];
					JSONObject object = JSONObject.fromObject(data);
					Put put = new Put(Bytes.toBytes(object.getString("contentId")));
					Set<String> keys = object.keySet();
					Iterator<String> iterator = keys.iterator();
					while(iterator.hasNext()){
						String key = iterator.next();
						put.add(Bytes.toBytes("detail"), Bytes.toBytes(key), Bytes.toBytes(object.getString(key)));
					}
					table.put(put);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
				if(reader != null)
					reader.close();
				if(bufReader != null)
					bufReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}		
	}
	
	public static void main(String[] args) {
		doUpdate();
	}
}
