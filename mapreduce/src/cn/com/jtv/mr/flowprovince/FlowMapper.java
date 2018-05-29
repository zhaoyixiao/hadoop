package cn.com.jtv.mr.flowprovince;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	private FlowBean bean;
	private Text k = new Text();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		k = new Text();
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(" ");
		
		String phoneId = split[0].trim();
		long upFlow = Long.parseLong(split[1]);
		long dFlow = Long.parseLong(split[2]);
		
		bean = new FlowBean(upFlow, dFlow);
		//FlowBean bean = new FlowBean(upFlow, dFlow);
		k.set(phoneId);
		context.write(k, bean);
		
	}

}
