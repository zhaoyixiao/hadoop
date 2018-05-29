package cn.com.jtv.mr.flowprovince;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 此类为重写hadoop的Partitioner类，旨在设置reduce输出
 * @author Administrator
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	private static HashMap<String, Integer> provinceCode = new HashMap<String,Integer>();
	
	static{
		provinceCode.put("0", 0);
		provinceCode.put("1", 1);
		provinceCode.put("2", 2);
		provinceCode.put("3", 3);
		provinceCode.put("4", 4);
		provinceCode.put("5", 5);
		provinceCode.put("6", 6);
		provinceCode.put("7", 7);
		provinceCode.put("8", 8);
		provinceCode.put("9", 9);
		
	}
	
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		Integer code = provinceCode.get(key.toString().substring(0, 1));
		
		
		return code==null?10:code;
	}

}
