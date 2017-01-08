package lesson1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxSpendPartitioner extends Partitioner<TextPair, LongWritable> {
	  
    @Override
    public int getPartition(TextPair key, LongWritable val, int numPartitions) 
    {
    	/*SimpleDateFormat format1 = new SimpleDateFormat("YYYY");
    	SimpleDateFormat format2 = new SimpleDateFormat("MM");
    	
    	Date date1=null,month1=null;*/
    	//try
    	//{
    		String date1=key.getFirst().toString().substring(0,4);
    		String month1=key.getFirst().toString().substring(5,7);
    		
    	/*}
    	catch(ParseException e)
        {
        e.printStackTrace();
        }*/
    	
    	int hash1 = date1.hashCode();
    			
    	int hash2 = month1.hashCode();   	
    	
    	
        int partition = ((hash1+hash2) % numPartitions);
        
       // System.out.println("H1:"+hash1+"\tH2:"+hash2+"\tPartitions:"+partition);
        return partition;
    }
 
}


