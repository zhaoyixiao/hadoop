package cn.com.jtv.hbase.demo;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientDMLDemo {

	private Connection con = null;

	@Before
	public void init() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		// 只需要添加此配置信息，因为每个 regionserver 上线会在 zookeeper 上注册，数据都在 regionserver 上
		conf.set("hbase.zookeeper.quorum", "zk-01:2181,zk-02:2181,zk-03:2181");

		con = ConnectionFactory.createConnection(conf);
	}

	@After
	public void close() throws Exception {
		con.close();
	}

	/**
	 * 插入数据(单行)
	 * 虽然hbase支持多类型的字段属性，但是在平常使用中尽量都存为String类型的数据，在读取后做处理
	 * 不然就要在读取数据是进行格式的判断，比较麻烦
	 * @throws Exception
	 */
	@Test
	public void testPut() throws Exception {

		// 获取表对象
		Table table = con.getTable(TableName.valueOf("t_eclipse"));

		// 创建 rowkey
		Put put = new Put("zhang-bj00-18-01".getBytes());
		// 添加数据；参数依次为：列族名称、字段名称、值
		put.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(1));
		put.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan"));
		put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));

		table.put(put);
		table.close();

	}

	/**
	 * 插入数据(多行)
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPuts() throws Exception {

		// 获取表对象
		Table table = con.getTable(TableName.valueOf("t_eclipse"));

		// 创建 rowkey
		Put put = new Put("zhang-bj00-18-02".getBytes());
		// 添加数据；参数依次为：列族名称、字段名称、值
		put.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(2));
		put.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan1"));
		put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
		put.addColumn("f1".getBytes(), "addr".getBytes(), Bytes.toBytes("北京-昌平"));

		// 创建 rowkey
		Put put1 = new Put("zhang-bj00-18-03".getBytes());
		// 添加数据；参数依次为：列族名称、字段名称、值
		put1.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(2));
		put1.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan2"));
		put1.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
		put1.addColumn("f1".getBytes(), "addr".getBytes(), Bytes.toBytes("北京-朝阳"));

		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(put);
		puts.add(put1);

		table.put(puts);
		table.close();

	}

	/**
	 * 更新数据
	 * @throws Exception
	 */
	@Test
	public void testUpdate() throws Exception {
		// 获取表对象
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		// 更新数据实质上就是重新构造一个相同的key，但value 不同的 put 对象，插入数据库
		Put put = new Put("zhang-bj00-18-03".getBytes());
		put.addColumn("f1".getBytes(), "uname".getBytes(), "zhangsan4".getBytes());
		table.put(put);
		table.close();
		
	}
	
	/**
	 * 删除数据一行中的特定数据
	 * @throws Exception
	 */
	@Test
	public void testDeleteKV() throws Exception{
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		
		// 删除不是删除一行，只是删除一行中的某个字段数据
		Delete delete = new Delete("zhang-bj00-18-03".getBytes());
		delete.addColumn("f1".getBytes(), "uname".getBytes());
		delete.addColumn("f1".getBytes(), "uid".getBytes());
		
		Delete delete2 = new Delete("zhang-bj00-18-02".getBytes());
		delete2.addColumn("f1".getBytes(), "uid".getBytes());
		
		ArrayList<Delete> list = new ArrayList<Delete>();
		list.add(delete);
		list.add(delete2);
		
		table.delete(list);
		table.close();
		
	}
	
	
	/**
	 * 删除一行,在delete对象中不指定具体的column 就是删除一整行数据
	 * @throws Exception
	 */
	@Test
	public void testDeleteRow() throws Exception{
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		Delete delete = new Delete("zhang-bj00-18-03".getBytes());
		
		table.delete(delete);
		table.close();
		
	}
	
}
