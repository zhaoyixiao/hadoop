package cn.com.jtv.mr.wc;

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
		Job job = Job.getInstance(conf);
		job.setJar("/root/pv.jar");
		
		//����map��
		job.setMapperClass(WcMapper.class);
		
		//����reduce��
		job.setReducerClass(WcReduce.class);
		
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
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//�������Ƿ��ڿͻ��˴�ӡ������Ϣ
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
	}
}
