package cn.com.jtv.mr.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	
	private long upFlow;
	
	private long dFlow;
	
	private long flowSum;
	
	//必须显试构造一个空参沟构造器
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
	
	//hadoop在输出时是一个文本文件，默认调用的是类的toString方法，所以要重写bean的toString
	@Override
	public String toString() {
		return upFlow + "\t" + dFlow + "\t" + flowSum;
	}

	//hadoop序列化框架在反序列化时调用的方法
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.flowSum = in.readLong();
		
	}

	//hadoop序列化程序在序列化调用的方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(flowSum);
		
	}

}
