package cn.com.jtv.mr.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class IndexStepOne {
	
	//内部Mapper类
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		String fileName;
		
		//一个map程序运行实例在调用我们的自定义mapper逻辑类时，首先会调用一次setup()方法,而且只调用一次
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			//context.getInputSplit();方法返回的是InputSplit抽象类，可以使用实现类进行操作
			//此类的包含的是当前切片文件的信息
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			
			//获取当前切片的文件名
			fileName = inputSplit.getPath().getName();
		}
		
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			

			String line = value.toString();
			
			//context.getInputSplit();方法返回的是InputSplit抽象类，可以使用实现类进行操作
			//此类的包含的是当前切片文件的信息
			//此方法如果卸载map里，那么每读取一行数据就会调用一次getInputSplit（），这样就会造成性能的降低，
			//所以，在利用setup机制，重写setup方法，这样每个map实例就只调用一次此方法
			/*FileSplit inputSplit = (FileSplit)context.getInputSplit();
			inputSplit.getPath().getName();*/
			
			
			String[] words = line.split(" ");
			for (String word : words) {
				context.write(new Text(word+"-"+fileName), new IntWritable(1));
			}
			
		}
		
		//当一个map程序实例处理完自己所负责的整个map数据后，会调用一次cleanup（）方法
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
	}
	
	
	//内部Reducer类
	public static class IndexStepOneReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {

		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//加载job执行类
		job.setJarByClass(IndexStepOne.class);
		
		//加载map类
		job.setMapperClass(IndexStepOneMapper.class);
		
		//加载reduce类
		job.setReducerClass(IndexStepOneReduce.class);
		
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
		
		//设置reduce个数
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//参数：是否在客户端打印进度信息
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	
	}

	
	
}
