package cn.com.jtv.mr.flow.sorttop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean>{
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context content)
			throws IOException, InterruptedException {
		
		List<FlowBean> list = new ArrayList<FlowBean>();
		FlowBean newBean;
		for (FlowBean bean : values) {
			newBean = new FlowBean(bean.getUpFlow(), bean.getdFlow());
			list.add(newBean);
		}
		Collections.sort(list);
		
		Configuration conf = new Configuration();
		
		int top = conf.getInt("flow.top", 3);
		for(int i=0;i<=top;i++){
			FlowBean sortBean = list.get(i);
			content.write(key, new FlowBean(sortBean.getUpFlow(),sortBean.getdFlow()));
		}
		
		
		
		
	}

}
