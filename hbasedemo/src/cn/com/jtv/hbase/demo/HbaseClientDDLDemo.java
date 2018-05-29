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
		// ֻ��Ҫ��Ӵ�������Ϣ����Ϊÿ�� regionserver ���߻��� zookeeper ��ע�ᣬ���ݶ��� regionserver ��
		conf.set("hbase.zookeeper.quorum", "zk-01:2181,zk-02:2181,zk-03:2181");
		
		con = ConnectionFactory.createConnection(conf);
	}
	
	@After
	public void close() throws Exception{
		con.close();
	}
	
	/**
	 * ������
	 * Admin : ����DDL����
	 * 
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception{
		
		Admin admin = con.getAdmin();
		// ��������
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("t_eclipse"));
		
		// ��������������
		HColumnDescriptor f = new HColumnDescriptor("f1");
		// hbase �п��Զ� kv ���ݱ��������N���汾��Ҳ��������һ�����ݵİ汾��Ĭ���������汾
		// �ڲ�ѯʱ��Ĭ�ϸ��������°汾������
		// ������С�汾��
		f.setMinVersions(1);
		// �������汾��
		f.setMaxVersions(3);
		tableDescriptor.addFamily(f);
		
		admin.createTable(tableDescriptor);
		admin.close();
		
	}
	
	/**
	 * ɾ����
	 * @throws Exception
	 */
	@Test
	public void testDropTable() throws Exception{
		Admin admin = con.getAdmin();
		// ���ñ�hbase ��ɾ����֮ǰҪ�������
		admin.disableTable(TableName.valueOf("t_eclipse"));
		admin.deleteTable(TableName.valueOf("t_eclipse"));
		admin.close();
	}
	
	/**
	 * �޸ı�
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
	 * ���������
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
