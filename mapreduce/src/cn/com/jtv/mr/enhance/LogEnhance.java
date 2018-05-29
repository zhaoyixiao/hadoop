package cn.com.jtv.mr.enhance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhance {

	static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private Map<String, String> map = new HashMap<String, String>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// 在 setup 初始化构建数据库连接，查处字典数据存入map中
			/**
			 * 
			 * DBLoader.load(map);
			 * 
			 */

		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, " ");

			// 获取全局计数器 参数1：计数器组名 参数2：计数器名
			Counter counter = context.getCounter("dataException", "line-malform");

			try {
				String ip = fields[0];
				String url = fields[1];

				// 在这里真是情况是在数据库里匹配出url的名称去匹配，加载到内存当中。在 setup 中初始化

				String content = map.get(url);

				if (StringUtils.isNotBlank(content)) {
					// 如果此url在字典表中存在
					context.write(new Text(line + "\t" + content), NullWritable.get());

				} else {

					// 如果字典数据中不存在改url，此时可以将字典表中不存在的url单独放置于一个表中，用于完善后期字典数据
					// \001tocrawl 是给数据做个表示，在重新写的 outputFormat类中根据此表示进行处理
					context.write(new Text(url + "\001tocrawl"), NullWritable.get());

				}
			} catch (Exception e) {
				counter.increment(1);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();

		job.setJarByClass(LogEnhance.class);

		job.setMapperClass(LogEnhanceMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(LogEnhanceOutputFormat.class);

		boolean b = job.waitForCompletion(true);
		System.out.println(b ? 0 : 1);
	}

}
