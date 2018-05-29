package cn.com.jtv.mr.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * ����1��KEYIN����Ӧ����map�׶���������ݵ�key������ ����2��VALUEIN����Ӧ����map�׶����������value������
 * ����3��KEYOUT�����û���reduce�׶ε��߼��������е�key������
 * ����4��VALUEOUT�����û���reduce�׶ε��߼���������value������
 * 
 * @author Administrator
 *
 */
public class PvReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	/**
	 * MR����ṩ��redurce�˳����������һ����ͬkey�����ݺ󣬵���һ��reduce()����
	 * 
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		context.write(key, new IntWritable(count));

	}
}
