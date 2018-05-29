package cn.com.jtv.mr.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * ����1��KEYIN����MR����ṩ�ĳ��򣬶�ȡ��һ�����ݵ���ʼƫ����
 * ����2��VALUEIN����MR����ṩ�ĳ��򣬶�ȡ��һ�����ݵ�����   String
 * ����3��KEYOUT,���û����߼����������������֮�󷵻ظ������KEY�е�����
 * ����4��VALUEOUT,������ɺ󷵻ظ����VALUE������    Integer
 * 
 * 
 * Long��String��Integer ����ֱ����hadoop��ֱ��ʹ�ã���Ϊ��Щ���ݻᱻ����ڻ����ͻ���֮��������紫�ͣ�Ҳ����˵��������ҪƵ�������л�
 * Ȼ����javaԭ�������л��ͷ����л����Ʒǳ�ӷ�ף�����hadoop������һ���Լ������л�����
 * 
 * Long : LongWritable
 * String : Text
 * Integer : IntWritable
 * 
 * @author Administrator
 *
 */
public class PvMapper extends Mapper<LongWritable, Text, Text,IntWritable >{

	/**
	 * MR����ṩ�ĳ���ÿ��һ�����ݾ͵���һmap()����
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String val = value.toString();
		String[] split = val.split(" ");
		String ip = split[0];
		
		context.write(new Text(ip), new IntWritable(1));
		
		
	}
	
	
	
	
	
	
	
	
}
