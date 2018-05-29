package cn.com.jtv.mr.flowprovince;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean>{
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context content)
			throws IOException, InterruptedException {
		
		long upFlowSum = 0L;
		long dFlowSum = 0L;
		for (FlowBean bean : values) {
			upFlowSum += bean.getUpFlow();
			dFlowSum += bean.getdFlow();
		}
		
		content.write(key, new FlowBean(upFlowSum,dFlowSum));
		
	}

}
