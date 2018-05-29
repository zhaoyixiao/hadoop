package cn.com.jtv.mr.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * 参数1：KEYIN，是MR框架提供的程序，读取到一行数据的起始偏移量
 * 参数2：VALUEIN，是MR框架提供的程序，读取到一行数据的内容   String
 * 参数3：KEYOUT,是用户的逻辑处理方法，处理完成之后返回给框架中KEY中的内容
 * 参数4：VALUEOUT,处理完成后返回给框架VALUE的内容    Integer
 * 
 * 
 * Long、String、Integer 不能直接在hadoop中直接使用，因为这些数据会被框架在机器和机器之间进行网络传送，也就是说，数据需要频繁的序列化
 * 然而，java原生的序列化和反序列化机制非常臃肿，所以hadoop开发了一个自己的序列化机制
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
	 * MR框架提供的程序每读一行数据就调用一map()方法
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
