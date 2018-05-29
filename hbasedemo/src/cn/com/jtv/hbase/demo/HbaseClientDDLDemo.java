package cn.com.jtv.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDDLDemo {
	
	private Connection con = null;
	
	@Before
	public void init() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		// 只需要添加此配置信息，因为每个 regionserver 上线会在 zookeeper 上注册，数据都在 regionserver 上
		conf.set("hbase.zookeeper.quorum", "zk-01:2181,zk-02:2181,zk-03:2181");
		
		con = ConnectionFactory.createConnection(conf);
	}
	
	@After
	public void close() throws Exception{
		con.close();
	}
	
	/**
	 * 创建表
	 * Admin : 用于DDL操作
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception{
		
		Admin admin = con.getAdmin();
		// 创建表名
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("t_eclipse"));
		
		// 创建列族描述器
		HColumnDescriptor f = new HColumnDescriptor("f1");
		// hbase 中可以对 kv 数据保存最近的N个版本，也就是设置一条数据的版本，默认是三个版本
		// 在查询时，默认给的是最新版本的数据
		// 设置最小版本号
		f.setMinVersions(1);
		// 设置最大版本号
		f.setMaxVersions(3);
		tableDescriptor.addFamily(f);
		
		admin.createTable(tableDescriptor);
		admin.close();
		
	}
	
	/**
	 * 删除表
	 * @throws Exception
	 */
	@Test
	public void testDropTable() throws Exception{
		Admin admin = con.getAdmin();
		// 禁用表，hbase 在删除表之前要将表禁用
		admin.disableTable(TableName.valueOf("t_eclipse"));
		admin.deleteTable(TableName.valueOf("t_eclipse"));
		admin.close();
	}
	
	/**
	 * 修改表
	 * @throws Exception
	 */
	@Test
	public void testModifyTable() throws Exception{
		
		Admin admin = con.getAdmin();
		
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("t_eclipse"));
		
		HColumnDescriptor f2 = new HColumnDescriptor("f2");
		f2.setBlocksize(131072);
		f2.setBloomFilterType(BloomType.ROWCOL);
		tableDescriptor.addFamily(f2);
		admin.modifyTable(TableName.valueOf("t_eclipse"), tableDescriptor);
		
		admin.close();
		
	}
	
	/**
	 * 清楚表数据
	 * @throws Exception
	 */
	@Test
	public void testTruncateTable() throws Exception{
		Admin admin = con.getAdmin();
		admin.disableTable(TableName.valueOf("t_eclipse"));
		admin.truncateTable(TableName.valueOf("t_eclipse".getBytes()), true);
		admin.close();
	}
	
	

}
