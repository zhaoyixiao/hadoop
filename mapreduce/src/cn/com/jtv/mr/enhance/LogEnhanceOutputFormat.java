package cn.com.jtv.mr.enhance;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhanceOutputFormat extends FileOutputFormat<Text, NullWritable>{
	
	
	static class LogEnhanceRecordWriter extends RecordWriter<Text, NullWritable>{
		FSDataOutputStream tocrawlOs = null;
		FSDataOutputStream enhanceOs = null;
		
		public LogEnhanceRecordWriter(FSDataOutputStream tocrawlOs,FSDataOutputStream enhanceOs) {
			this.tocrawlOs = tocrawlOs;
			this.enhanceOs = enhanceOs;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			
			// .....存入未找到url表中，后期可以根据这url做爬虫，进行数据爬取
			if(key.toString().contains("\001tocrawl")){
				
				//写入待爬清单文件中
				tocrawlOs.write(key.toString().getBytes());
				tocrawlOs.write("\n".getBytes());
				tocrawlOs.flush();
				
			}else{
				
				//写入增强结果日志文件
				enhanceOs.write(key.toString().getBytes());
				enhanceOs.write("\n".getBytes());
				enhanceOs.flush();
			}
			
		}

		/**
		 * 关闭流
		 */
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			if(tocrawlOs!=null){
				tocrawlOs.close();
			}
			if(enhanceOs!=null){
				enhanceOs.close();
			}
			
		}

		
		
	}
	
	

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream tocrawlOs = fs.create(new Path("hdfs://hdp-nn-01:9000/tocrawlOs/result.dat"));
		FSDataOutputStream enhanceOs = fs.create(new Path("hdfs://hdp-nn-01:9000/enhanceOs/result.dat"));

		
		return new LogEnhanceRecordWriter(tocrawlOs,enhanceOs);
	}
	
	
	
	
	
	

}
