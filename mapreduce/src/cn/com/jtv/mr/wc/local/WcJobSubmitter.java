package cn.com.jtv.mr.wc.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.com.jtv.mr.pv.PvMapper;
import cn.com.jtv.mr.pv.PvReduce;

public class WcJobSubmitter {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WcJobSubmitter.class);
		
		//加载map类
		job.setMapperClass(WcMapper.class);
		
		//加载reduce类
		job.setReducerClass(WcReduce.class);
		
		//map阶段输出的key、value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reduce阶段输出的key、value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置读取文件的的类型     TextInputFormat是执行普通文本文件类型
		job.setInputFormatClass(TextInputFormat.class);
		//设置读取hdfs文件的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//设置输出文件类型
		job.setOutputFormatClass(TextOutputFormat.class);
		//设置输出文件的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//参数：是否在客户端打印进度信息
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	}
}
