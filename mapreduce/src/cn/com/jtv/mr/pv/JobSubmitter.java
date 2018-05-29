package cn.com.jtv.mr.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * JobSubmitter����ʵ��һ��yarn�Ŀͻ���
 * �����ǣ������ǵ�mapreduce����jar���ύ��yarn����yarn��ȥ��jar���ַ����ܶ��nodemanager��ȥִ��
 * @author Administrator
 *
 */
public class JobSubmitter {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//job.setJar("/root/pv.jar");
		job.setJarByClass(JobSubmitter.class);
		
		//����map��
		job.setMapperClass(PvMapper.class);
		
		//����reduce��
		job.setReducerClass(PvReduce.class);
		
		//map�׶������key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reduce�׶������key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//���ö�ȡ�ļ��ĵ�����     TextInputFormat��ִ����ͨ�ı��ļ�����
		job.setInputFormatClass(TextInputFormat.class);
		//���ö�ȡhdfs�ļ���·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//��������ļ�����
		job.setOutputFormatClass(TextOutputFormat.class);
		//��������ļ���·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//����reduce����
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//�������Ƿ��ڿͻ��˴�ӡ������Ϣ
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	}
}
