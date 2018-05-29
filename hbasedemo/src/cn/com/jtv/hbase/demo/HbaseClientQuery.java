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
		// 只需要添加此配置信息，因为每个 regionserver 上线会在 zookeeper 上注册，数据都在 regionserver 上
		conf.set("hbase.zookeeper.quorum", "zk-01:2181,zk-02:2181,zk-03:2181");

		con = ConnectionFactory.createConnection(conf);
	}

	@After
	public void close() throws Exception {
		con.close();
	}
	
	/**
	 * 根据 rowkey 查询
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
	 * 查询多行
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
		
		// rowkey 过滤器
		// 从rk001 开始，大于 rk004 的结果集
		//RowFilter filter = new RowFilter(CompareOp.GREATER, new BinaryComparator("rk004".getBytes()));
		// 从 rk001 开始，以 rk 开头的结果集
		//RowFilter filter = new RowFilter(CompareOp.EQUAL,new BinaryPrefixComparator("rk".getBytes()));
		
		// 列族过滤器
		//FamilyFilter filter = new FamilyFilter(CompareOp.EQUAL,new BinaryComparator("f1".getBytes()));
		
		// 列(qualifier) 过滤器
		//QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator("uname".getBytes()));
		
		// 值过滤器
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
	 * 打印 result 结果
	 * @param result
	 * @throws IOException
	 */
	private void printResult(Result result) throws IOException {
		CellScanner cellScanner = result.cellScanner();
		while(cellScanner.advance()){
			// 取到一个单元格
			Cell cell = cellScanner.current();
			
			// 获取rowkey
			// 后连个参数为获取值的偏移量，从起始位置到长度
			byte[] rowArr = cell.getRowArray();
			String rowkey = Bytes.toString(rowArr,cell.getRowOffset(),cell.getRowLength());
			
			// 获取列族名
			byte[] familyArray = cell.getFamilyArray();
			String f = Bytes.toString(familyArray,cell.getFamilyOffset(),cell.getFamilyLength());
			
			// 获取字段名
			byte[] qualifierArray = cell.getQualifierArray();
			String q = Bytes.toString(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength());
			
			// 获取值
			byte[] valueArray = cell.getValueArray();
			String v = Bytes.toString(valueArray,cell.getValueOffset(),cell.getValueLength());
			
			System.out.println(rowkey+","+f+":"+q+","+v);
			
		}
	}

}
