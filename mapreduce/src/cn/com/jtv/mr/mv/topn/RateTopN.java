package cn.com.jtv.mr.mv.topn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import cn.com.jtv.mr.wc.local.WcJobSubmitter;
import cn.com.jtv.mr.wc.local.WcMapper;
import cn.com.jtv.mr.wc.local.WcReduce;

public class RateTopN {
	
	
	static class RateTopNMapper extends Mapper<LongWritable,Text , RateBean, NullWritable>{
		/*ObjectMapper objectMapper;
		@Override
		protected void setup(Mapper<LongWritable, Text, RateBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			objectMapper = new ObjectMapper();
		}*/
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, RateBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] split = line.split(" ");
			RateBean bean = new RateBean(split[0],split[1],split[2]);
			
			context.write(bean, NullWritable.get());
			
		}
		
		
	}
	
	static class RateTopNReduce extends Reducer<RateBean, NullWritable, RateBean, NullWritable>{
		
		@Override
		protected void reduce(RateBean key, Iterable<NullWritable> values,
				Reducer<RateBean, NullWritable, RateBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			int topn = context.getConfiguration().getInt("rate.top", 20);
			
			int count = 0;
			for (NullWritable nullWritable : values) {
				context.write(key, NullWritable.get());
				count++;
				if(count == topn){return;}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.addResource("userConf.xml");
		conf.set("rate.top", "5");
		//conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(RateTopN.class);
		
		//加载map类
		job.setMapperClass(RateTopNMapper.class);
		
		//加载reduce类
		job.setReducerClass(RateTopNReduce.class);

		
		//map阶段输出的key、value类型
		job.setMapOutputKeyClass(RateBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//reduce阶段输出的key、value类型
		job.setOutputKeyClass(RateBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置读取文件的的类型     TextInputFormat是执行普通文本文件类型
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setPartitionerClass(UidPartitioner.class);
		job.setGroupingComparatorClass(UidGroupingComparator.class);
		
		//设置读取hdfs文件的路径
		//FileInputFormat.setInputPaths(job, new Path("d:/mv2.txt"));
		FileInputFormat.setInputPaths(job, new Path("/mv/input"));
		
		//设置输出文件类型
		job.setOutputFormatClass(TextOutputFormat.class);
		//设置输出文件的路径
		//FileOutputFormat.setOutputPath(job, new Path("d:/mv/output5"));
		FileOutputFormat.setOutputPath(job, new Path("/mv/output"));
		//job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//参数：是否在客户端打印进度信息
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
		
	}

}
