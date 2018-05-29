package cn.com.jtv.mr.join.mapside;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapSideJoin {

	public static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private HashMap<String, String> map = new HashMap<String,String>();
		private Text k = new Text();
		
		// 先获取小表的文件加载到内存中
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			FileReader fileReader = new FileReader("user.txt");
			BufferedReader br = new BufferedReader(fileReader);
			String line = "";
			while(StringUtils.isNotBlank(line=br.readLine())){
				
				//005 liangjiahui 60
				String[] split = line.split(" ");
				map.put(split[0], split[1] + "\t" + split[2]);
				
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//1001 005 
			String line = value.toString();
			String[] split = line.split(" ");
			String userInfo = map.get(split[1]);
			k.set(split[0] + "\t" + userInfo);
			context.write(k, NullWritable.get());
			
		}
		
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		//第二个参数是在运行mr时，yarn上面显示本mr的名称
		Job job = Job.getInstance(conf, "MapSideJoin");
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, "/join/input");
		FileOutputFormat.setOutputPath(job, new Path("/join/output"));
		
		//因为此次分析不需要用到reduce阶段，所以把参数设置为0
		job.setNumReduceTasks(0);
		
		job.addCacheFile(new URI("hdfs://hdp-nn-01:9000/join/cache/user.txt"));
		
		boolean b = job.waitForCompletion(true);
		System.out.println(b?0:1);
		

	}

}
