package cn.com.jtv.mr.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	
	private long upFlow;
	
	private long dFlow;
	
	private long flowSum;
	
	//�������Թ���һ���ղι�������
	public FlowBean() {
		
	}

	public FlowBean(long upFlow,long dFlow){
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.flowSum = upFlow + dFlow;
		
	}
	

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getFlowSum() {
		return flowSum;
	}

	public void setFlowSum(long flowSum) {
		this.flowSum = flowSum;
	}
	
	//hadoop�����ʱ��һ���ı��ļ���Ĭ�ϵ��õ������toString����������Ҫ��дbean��toString
	@Override
	public String toString() {
		return upFlow + "\t" + dFlow + "\t" + flowSum;
	}

	//hadoop���л�����ڷ����л�ʱ���õķ���
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.flowSum = in.readLong();
		
	}

	//hadoop���л����������л����õķ���
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(flowSum);
		
	}

}
