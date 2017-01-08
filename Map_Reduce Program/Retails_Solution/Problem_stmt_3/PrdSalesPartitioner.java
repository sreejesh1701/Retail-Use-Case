package lesson1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PrdSalesPartitioner extends Partitioner<TextPair, LongWritable> {
	  
    @Override
    public int getPartition(TextPair key, LongWritable val, int numPartitions) 
    {
    	
        int hash1 = key.getFirst().toString().hashCode() & Integer.MAX_VALUE;   
    
    	int partition = hash1 % numPartitions;
        return partition;
    }
 
}
