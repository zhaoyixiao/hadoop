package cn.com.jtv.mr.mv.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author Administrator
 *
 */
public class UidGroupingComparator extends WritableComparator {

	public UidGroupingComparator() {
		//����true ��ʾ�Ƿ�Ҫ����ʵ����Ҫ����Ϊtrue
		super(RateBean.class,true);
	}
	
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		RateBean key1 = (RateBean)a;
		RateBean key2 = (RateBean)b;
		
		return key1.getUid().compareTo(key2.getUid());
	}

}
