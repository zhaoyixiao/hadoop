package cn.com.jtv.mr.mv.topn;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * ����ʵ�� WritableComparable �ӿڵĺ��壺
 * 1��Writable �������л��ͷ����л� 
 * 2��Comparable �����ڱȽ�����
 * @author Administrator
 *
 */
public class RateBean implements WritableComparable<RateBean>{
	
	private String uid;
	private String mid;
	private String rate;
	
	public RateBean() {
		
	}
	
	public RateBean(String uid, String mid, String rate) {
		this.uid = uid;
		this.mid = mid;
		this.rate = rate;
	}



	public void set(String uid,String mid,String rate){
		this.uid = uid;
		this.mid = mid;
		this.rate = rate;
	}
	
	
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	public String getMid() {
		return mid;
	}
	public void setMid(String mid) {
		this.mid = mid;
	}
	public String getRate() {
		return rate;
	}
	public void setRate(String rate) {
		this.rate = rate;
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.uid = in.readUTF();
		this.mid = in.readUTF();
		this.rate = in.readUTF();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.uid);
		out.writeUTF(this.mid);
		out.writeUTF(this.rate);
		
	}
	@Override
	public int compareTo(RateBean o) {
		
		//ʵ���߼������uid��ͬ�ͱȽ����֣����uid����ͬ�ͱȽ�uid
		if(this.uid.equals(o.getUid())){
			return -this.rate.compareTo(o.getRate());
		}else{
			return this.uid.compareTo(o.getUid());
		}
	}
	
	@Override
	public String toString() {
		return this.uid + "\t" + this.mid + "\t" +this.rate;
	}
	
	
	

}
