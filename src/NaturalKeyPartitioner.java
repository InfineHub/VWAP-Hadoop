

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitions key based on "natural" key of {@link StockKey} (which
 * is the symbol).
 * @author Jee Vang
 *
 */
public class NaturalKeyPartitioner extends Partitioner<IsinKey, Tick> {

	@Override
	public int getPartition(IsinKey key, Tick val, int numPartitions) {
		int hash = key.getKey().hashCode();
		return hash % numPartitions;
	}

}
