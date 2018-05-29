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
			// �� setup ��ʼ���������ݿ����ӣ��鴦�ֵ����ݴ���map��
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

			// ��ȡȫ�ּ����� ����1������������ ����2����������
			Counter counter = context.getCounter("dataException", "line-malform");

			try {
				String ip = fields[0];
				String url = fields[1];

				// ��������������������ݿ���ƥ���url������ȥƥ�䣬���ص��ڴ浱�С��� setup �г�ʼ��

				String content = map.get(url);

				if (StringUtils.isNotBlank(content)) {
					// �����url���ֵ���д���
					context.write(new Text(line + "\t" + content), NullWritable.get());

				} else {

					// ����ֵ������в����ڸ�url����ʱ���Խ��ֵ���в����ڵ�url����������һ�����У��������ƺ����ֵ�����
					// \001tocrawl �Ǹ�����������ʾ��������д�� outputFormat���и��ݴ˱�ʾ���д���
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
