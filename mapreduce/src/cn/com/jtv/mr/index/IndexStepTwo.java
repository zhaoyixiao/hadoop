package cn.com.jtv.mr.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexStepTwo {
	
	
	static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/**
			 *  im-b.txt	1
				is-b.txt	1
				is-c.txt	2
			 */
			String line = value.toString();
			String[] split = line.split("-");
			String word = split[0];
			String fileName = split[1].split("\t")[0];
			String count = split[1].split("\t")[1];
			
			context.write(new Text(word), new Text(fileName+"-->"+count));
		}
	}
	
	
	static class IndexStepTwoReduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value.toString()).append(" ");
			}
			
			context.write(key, new Text(sb.toString()));
		}
		
	}
	
	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//加载job执行类
		job.setJarByClass(IndexStepTwo.class);
		
		//加载map类
		job.setMapperClass(IndexStepTwoMapper.class);
		
		//加载reduce类
		job.setReducerClass(IndexStepTwoReduce.class);
		
		//map阶段输出的key、value类型
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
		
		//reduce阶段输出的key、value类型
		//如果输出的K、V类型是一致的那么，输入类型可以不用设置
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//设置读取文件的的类型     TextInputFormat是执行普通文本文件类型
		//如果输入文件是文本文件，那么setInputFormatClass()方法可以省略，默认问文本输入
		//job.setInputFormatClass(TextInputFormat.class);
		
		//设置读取hdfs文件的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//设置输出文件类型
		//如果输出文件是文本文件，那么setOutputFormatClass()方法可以省略，默认为文本输出
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输出文件的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置reduce个数
		//此参数如果不设置，默认reduce为1
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//参数：是否在客户端打印进度信息
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	
	}

}
