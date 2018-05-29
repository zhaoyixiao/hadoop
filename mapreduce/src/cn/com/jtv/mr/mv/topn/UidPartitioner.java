package cn.com.jtv.mr.mv.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 此 partitioner 的意义就是保证相同的uid在同一个partitioner内
 * @author Administrator
 *
 */
public class UidPartitioner extends Partitioner<RateBean, NullWritable>{

	
	@Override
	public int getPartition(RateBean key, NullWritable value, int numPartitions) {
		
		// key.getUid().hashCode() & Integer.MAX_VALUE 意义：
		// hashCode可能超过Integer的最大值，如果超过就会出现负数
		return (key.getUid().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
