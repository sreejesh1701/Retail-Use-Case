/* Customer with Maximum Spending By Month */

package lesson1;

import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import lesson1.GroupingComparator;
import lesson1.MaxSpendPartitioner;
import lesson1.SortComparator;
import lesson1.TextPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustWithMaxSpending {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "CustWithMaxSpending");	   
	    
	    job.setJarByClass(CustWithMaxSpending.class); 
	    job.setInputFormatClass(TextInputFormat.class);
	   // job.setMapperClass(MaxSpendMapper.class);
	    
	    MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,MaxSpendMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,MaxSpendMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class,MaxSpendMapper.class);
	   MultipleInputs.addInputPath(job,new Path(args[3]), TextInputFormat.class,MaxSpendMapper.class);
	    
	    job.setMapOutputKeyClass(TextPair.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    job.setNumReduceTasks(4);
	    job.setSortComparatorClass(SortComparator.class);
	    job.setGroupingComparatorClass(GroupingComparator.class);
	    	       
	    job.setPartitionerClass(MaxSpendPartitioner.class);
	    job.setReducerClass(MaxSpendReducer.class);  	    
	   
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    // File Output Format
	    FileOutputFormat.setOutputPath(job, new Path(args[4]));    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
    
  class MaxSpendMapper extends Mapper<LongWritable, Text, TextPair, LongWritable>
  {
	  @Override  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
      String[] line = value.toString().split(";");        
      String date1 = line[0].substring(0,10);     
      String custID = line[1].substring(0,8);
      long amt = Long.parseLong(line[8]);
    
      context.write(new TextPair(new Text(date1),new Text(custID)),new LongWritable(amt));           
    }    
 } 
 
   class MaxSpendReducer extends Reducer<TextPair, LongWritable,LongWritable,Text> 
    {   	          
    private TreeMap<Long,String> countMap = new TreeMap<Long,String>();
        @Override            
    public void reduce(TextPair key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException 
    {              
       long sum = 0;    
       String custID =" ", date1= " ";
       
       for (LongWritable val : values)
        {  
    	       sum+=val.get();   
        }
          date1 = key.getFirst().toString().substring(0, 7);    			   
	      custID = key.getSecond().toString();
	      String custIDDate = custID+"\t"+date1;	      
          countMap.put(sum, custIDDate);      
         // context.write(new LongWritable(sum), new Text(custIDDate)); 
     }
         @Override
     public void cleanup( Context context) throws IOException, InterruptedException
    {         
         
         long MaxAmount = countMap.pollLastEntry().getKey();         
       	 String custIdDt = countMap.pollLastEntry().getValue().toString();          	 
       	 context.write(new LongWritable(MaxAmount),new Text(custIdDt));
       	
    } 
   } 
    	 
    	 
    		 
    	 
    	 
    	 
    	 
    	 
    		 
    	 
    
    
    	