package cn.com.jtv.mr.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * 参数1：KEYIN，对应的是map阶段输出的数据的key的类型 参数2：VALUEIN，对应的是map阶段输出的数据value的类型
 * 参数3：KEYOUT，是用户的reduce阶段的逻辑处理结果中的key的类型
 * 参数4：VALUEOUT，是用户的reduce阶段的逻辑处理结果中value的类型
 * 
 * @author Administrator
 *
 */
public class PvReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	/**
	 * MR框架提供的redurce端程序在整理好一组相同key的数据后，调用一次reduce()方法
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
