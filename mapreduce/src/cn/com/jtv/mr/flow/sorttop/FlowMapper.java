package cn.com.jtv.mr.flow.sorttop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	private Text k = new Text();
	private FlowBean bean = new FlowBean();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(" ");
		
		String phoneId = split[0].trim();
		long upFlow = Long.parseLong(split[1]);
		long dFlow = Long.parseLong(split[2]);
		
		bean.set(upFlow, dFlow);
		k.set(phoneId);
		context.write(k, bean);
		
		
		
	}

}
