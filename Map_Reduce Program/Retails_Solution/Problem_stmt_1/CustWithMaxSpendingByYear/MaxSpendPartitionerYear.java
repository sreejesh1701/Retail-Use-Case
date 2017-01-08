package lesson1;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxSpendPartitionerYear extends Partitioner<TextPair, LongWritable> {
	  
    @Override
    public int getPartition(TextPair key, LongWritable val, int numPartitions) 
    {
    	SimpleDateFormat format1 = new SimpleDateFormat("YYYY");
    	
    	Date date1=null;
    	try
    	{
    		date1=format1.parse(key.getFirst().toString().substring(0,4));
    		
    	}
    	catch(ParseException e)
        {
        e.printStackTrace();
        }
    	int hash1 = date1.hashCode() & Integer.MAX_VALUE;
    	int partition = hash1 % numPartitions;
        return partition;
    }
 
}
