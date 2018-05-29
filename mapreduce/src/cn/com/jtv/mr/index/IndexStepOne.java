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
	
	//�ڲ�Mapper��
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		String fileName;
		
		//һ��map��������ʵ���ڵ������ǵ��Զ���mapper�߼���ʱ�����Ȼ����һ��setup()����,����ֻ����һ��
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			//context.getInputSplit();�������ص���InputSplit�����࣬����ʹ��ʵ������в���
			//����İ������ǵ�ǰ��Ƭ�ļ�����Ϣ
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			
			//��ȡ��ǰ��Ƭ���ļ���
			fileName = inputSplit.getPath().getName();
		}
		
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			

			String line = value.toString();
			
			//context.getInputSplit();�������ص���InputSplit�����࣬����ʹ��ʵ������в���
			//����İ������ǵ�ǰ��Ƭ�ļ�����Ϣ
			//�˷������ж��map���ôÿ��ȡһ�����ݾͻ����һ��getInputSplit�����������ͻ�������ܵĽ��ͣ�
			//���ԣ�������setup���ƣ���дsetup����������ÿ��mapʵ����ֻ����һ�δ˷���
			/*FileSplit inputSplit = (FileSplit)context.getInputSplit();
			inputSplit.getPath().getName();*/
			
			
			String[] words = line.split(" ");
			for (String word : words) {
				context.write(new Text(word+"-"+fileName), new IntWritable(1));
			}
			
		}
		
		//��һ��map����ʵ���������Լ������������map���ݺ󣬻����һ��cleanup��������
		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
		
	}
	
	
	//�ڲ�Reducer��
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
		
		//����jobִ����
		job.setJarByClass(IndexStepOne.class);
		
		//����map��
		job.setMapperClass(IndexStepOneMapper.class);
		
		//����reduce��
		job.setReducerClass(IndexStepOneReduce.class);
		
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
