package cn.com.jtv.hbase.demo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseClientQuery {
	
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
	 * ���� rowkey ��ѯ
	 * @throws Exception
	 */
	@Test
	public void testGet() throws Exception{
		
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		Get get = new Get("zhang-bj00-18-02".getBytes());
		Result result = table.get(get);
		
		/*byte[] value = result.getValue("f1".getBytes(), "age".getBytes());
		System.out.println(Bytes.toInt(value));*/
		
		printResult(result);
		table.close();
		
	}

	
	
	/**
	 * ��ѯ����
	 * @throws Exception
	 */
	@Test
	public void testScan() throws Exception{
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		
		Scan scan = new Scan("rk001".getBytes(), ("rk003"+"\000").getBytes());
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while(iterator.hasNext()){
			Result result = iterator.next();
			printResult(result);
			
		}
		
	}
	
	
	/**
	 *  filter
	 * @throws Exception
	 */
	@Test
	public void testScanFilter() throws Exception{
		
		Table table = con.getTable(TableName.valueOf("t_eclipse"));
		
		ByteArrayComparable x = null;
		
		// rowkey ������
		// ��rk001 ��ʼ������ rk004 �Ľ����
		//RowFilter filter = new RowFilter(CompareOp.GREATER, new BinaryComparator("rk004".getBytes()));
		// �� rk001 ��ʼ���� rk ��ͷ�Ľ����
		//RowFilter filter = new RowFilter(CompareOp.EQUAL,new BinaryPrefixComparator("rk".getBytes()));
		
		// ���������
		//FamilyFilter filter = new FamilyFilter(CompareOp.EQUAL,new BinaryComparator("f1".getBytes()));
		
		// ��(qualifier) ������
		//QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("uname".getBytes()));
		
		// ֵ������
		ValueFilter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator("1".getBytes()));
		
		
		Scan scan = new Scan("rk001".getBytes(), filter);
		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while(iterator.hasNext()){
			Result result = iterator.next();
			printResult(result);
			
		}
		
	}
	
	/**
	 * ��ӡ result ���
	 * @param result
	 * @throws IOException
	 */
	private void printResult(Result result) throws IOException {
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()){
			// ȡ��һ����Ԫ��
			Cell cell = cellScanner.current();
			
			// ��ȡrowkey
			// ����������Ϊ��ȡֵ��ƫ����������ʼλ�õ�����
			byte[] rowArr = cell.getRowArray();
			String rowkey = Bytes.toString(rowArr,cell.getRowOffset(),cell.getRowLength());
			
			// ��ȡ������
			byte[] familyArray = cell.getFamilyArray();
			String f = Bytes.toString(familyArray,cell.getFamilyOffset(),cell.getFamilyLength());
			
			// ��ȡ�ֶ���
			byte[] qualifierArray = cell.getQualifierArray();
			String q = Bytes.toString(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength());
			
			// ��ȡֵ
			byte[] valueArray = cell.getValueArray();
			String v = Bytes.toString(valueArray,cell.getValueOffset(),cell.getValueLength());
			
			System.out.println(rowkey+","+f+":"+q+","+v);
			
		}
	}

}
