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
		// ֻ��Ҫ��Ӵ�������Ϣ����Ϊÿ�� regionserver ���߻��� zookeeper ��ע�ᣬ���ݶ��� regionserver ��
		conf.set("hbase.zookeeper.quorum", "zk-01:2181,zk-02:2181,zk-03:2181");

		con = ConnectionFactory.createConnection(conf);
	}

	@After
	public void close() throws Exception {
		con.close();
	}

	/**
	 * ��������(����)
	 * ��Ȼhbase֧�ֶ����͵��ֶ����ԣ�������ƽ��ʹ���о�������ΪString���͵����ݣ��ڶ�ȡ��������
	 * ��Ȼ��Ҫ�ڶ�ȡ�����ǽ��и�ʽ���жϣ��Ƚ��鷳
	 * @throws Exception
	 */
	@Test
	public void testPut() throws Exception {

		// ��ȡ�����
		Table table = con.getTable(TableName.valueOf("t_eclipse"));

		// ���� rowkey
		Put put = new Put("zhang-bj00-18-01".getBytes());
		// ������ݣ���������Ϊ���������ơ��ֶ����ơ�ֵ
		put.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(1));
		put.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan"));
		put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));

		table.put(put);
		table.close();

	}

	/**
	 * ��������(����)
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPuts() throws Exception {

		// ��ȡ�����
		Table table = con.getTable(TableName.valueOf("t_eclipse"));

		// ���� rowkey
		Put put = new Put("zhang-bj00-18-02".getBytes());
		// ������ݣ���������Ϊ���������ơ��ֶ����ơ�ֵ
		put.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(2));
		put.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan1"));
		put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
		put.addColumn("f1".getBytes(), "addr".getBytes(), Bytes.toBytes("����-��ƽ"));

		// ���� rowkey
		Put put1 = new Put("zhang-bj00-18-03".getBytes());
		// ������ݣ���������Ϊ���������ơ��ֶ����ơ�ֵ
		put1.addColumn("f1".getBytes(), "uid".getBytes(), Bytes.toBytes(2));
		put1.addColumn("f1".getBytes(), "uname".getBytes(), Bytes.toBytes("zhangsan2"));
		put1.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
		put1.addColumn("f1".getBytes(), "addr".getBytes(), Bytes.toBytes("����-����"));

		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(put);
		puts.add(put1);

		table.put(puts);
		table.close();

	}

	/**
	 * ��������
	 * @throws Exception
	 */
	@Test
	public void testUpdate() throws Exception {
		// ��ȡ�����
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		// ��������ʵ���Ͼ������¹���һ����ͬ��key����value ��ͬ�� put ���󣬲������ݿ�
		Put put = new Put("zhang-bj00-18-03".getBytes());
		put.addColumn("f1".getBytes(), "uname".getBytes(), "zhangsan4".getBytes());
		table.put(put);
		table.close();
		
	}
	
	/**
	 * ɾ������һ���е��ض�����
	 * @throws Exception
	 */
	@Test
	public void testDeleteKV() throws Exception{
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		
		// ɾ������ɾ��һ�У�ֻ��ɾ��һ���е�ĳ���ֶ�����
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
	 * ɾ��һ��,��delete�����в�ָ�������column ����ɾ��һ��������
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
