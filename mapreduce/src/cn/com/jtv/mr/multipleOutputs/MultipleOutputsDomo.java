package cn.com.jtv.mr.multipleOutputs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * MultipleOutputs  分路输出器，当有需求将数据分散输出到不同的地方，可以使用MultipleOutputs
 * @author Administrator
 *
 */
public class MultipleOutputsDomo {

	static class MultipleOutputsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private MultipleOutputs<Text, NullWritable> mos;

		/**
		 * 初始化创建MultipleOutputs
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			mos = new MultipleOutputs(context);

		}
		
		/**
		 * mapper
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				if(word.startsWith("a")){
					
					// "A" 代表别名
					mos.write("A",new Text(word), NullWritable.get(), "a/");
				}else if(word.startsWith("b")){
					
					mos.write("B",new Text(word), NullWritable.get(), "b/");

				}else{
					mos.write("OTHER",new Text(word), NullWritable.get(), "other/");
				}
			}
			
		}
		
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			mos.close();
		}

	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MultipleOutputsDomo.class);
		job.setMapperClass(MultipleOutputsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/word/"));
		FileOutputFormat.setOutputPath(job,new Path("/word/output1"));
		
		//将自定义的 MultipleOutputs 添加到job中
		MultipleOutputs.addNamedOutput(job, "A",TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "B",TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "OTHER",TextOutputFormat.class, Text.class, NullWritable.class);
		
		job.setNumReduceTasks(0);
		boolean b = job.waitForCompletion(true);
		System.out.println(b?0:1);
		
		
	}

}
