package cn.com.jtv.mr.mv.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * �� partitioner ��������Ǳ�֤��ͬ��uid��ͬһ��partitioner��
 * @author Administrator
 *
 */
public class UidPartitioner extends Partitioner<RateBean, NullWritable>{

	
	@Override
	public int getPartition(RateBean key, NullWritable value, int numPartitions) {
		
		// key.getUid().hashCode() & Integer.MAX_VALUE ���壺
		// hashCode���ܳ���Integer�����ֵ����������ͻ���ָ���
		return (key.getUid().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
