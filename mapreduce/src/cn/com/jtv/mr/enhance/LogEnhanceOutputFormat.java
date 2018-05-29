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
			
			// .....����δ�ҵ�url���У����ڿ��Ը�����url�����棬����������ȡ
			if(key.toString().contains("\001tocrawl")){
				
				//д������嵥�ļ���
				tocrawlOs.write(key.toString().getBytes());
				tocrawlOs.write("\n".getBytes());
				tocrawlOs.flush();
				
			}else{
				
				//д����ǿ�����־�ļ�
				enhanceOs.write(key.toString().getBytes());
				enhanceOs.write("\n".getBytes());
				enhanceOs.flush();
			}
			
		}

		/**
		 * �ر���
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
