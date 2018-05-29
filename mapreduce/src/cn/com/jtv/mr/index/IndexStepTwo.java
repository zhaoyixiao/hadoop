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
		
		//����jobִ����
		job.setJarByClass(IndexStepTwo.class);
		
		//����map��
		job.setMapperClass(IndexStepTwoMapper.class);
		
		//����reduce��
		job.setReducerClass(IndexStepTwoReduce.class);
		
		//map�׶������key��value����
		/*job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);*/
		
		//reduce�׶������key��value����
		//��������K��V������һ�µ���ô���������Ϳ��Բ�������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//���ö�ȡ�ļ��ĵ�����     TextInputFormat��ִ����ͨ�ı��ļ�����
		//��������ļ����ı��ļ�����ôsetInputFormatClass()��������ʡ�ԣ�Ĭ�����ı�����
		//job.setInputFormatClass(TextInputFormat.class);
		
		//���ö�ȡhdfs�ļ���·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//��������ļ�����
		//�������ļ����ı��ļ�����ôsetOutputFormatClass()��������ʡ�ԣ�Ĭ��Ϊ�ı����
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		//��������ļ���·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//����reduce����
		//�˲�����������ã�Ĭ��reduceΪ1
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//�������Ƿ��ڿͻ��˴�ӡ������Ϣ
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	
	}

}
