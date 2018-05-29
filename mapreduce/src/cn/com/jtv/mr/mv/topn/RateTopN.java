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
		
		//����map��
		job.setMapperClass(RateTopNMapper.class);
		
		//����reduce��
		job.setReducerClass(RateTopNReduce.class);

		
		//map�׶������key��value����
		job.setMapOutputKeyClass(RateBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//reduce�׶������key��value����
		job.setOutputKeyClass(RateBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//���ö�ȡ�ļ��ĵ�����     TextInputFormat��ִ����ͨ�ı��ļ�����
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setPartitionerClass(UidPartitioner.class);
		job.setGroupingComparatorClass(UidGroupingComparator.class);
		
		//���ö�ȡhdfs�ļ���·��
		//FileInputFormat.setInputPaths(job, new Path("d:/mv2.txt"));
		FileInputFormat.setInputPaths(job, new Path("/mv/input"));
		
		//��������ļ�����
		job.setOutputFormatClass(TextOutputFormat.class);
		//��������ļ���·��
		//FileOutputFormat.setOutputPath(job, new Path("d:/mv/output5"));
		FileOutputFormat.setOutputPath(job, new Path("/mv/output"));
		//job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//�������Ƿ��ڿͻ��˴�ӡ������Ϣ
		boolean ret = job.waitForCompletion(true);
		System.out.println(ret?0:1);
		
	}

}
