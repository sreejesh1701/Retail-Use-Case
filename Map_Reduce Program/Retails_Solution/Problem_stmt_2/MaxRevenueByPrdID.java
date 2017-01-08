package lesson1;

import java.io.IOException;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxRevenueByPrdID {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "MaxRevenueByPrdID");	   
	    
	    job.setJarByClass(MaxRevenueByPrdID.class); 
	    job.setInputFormatClass(TextInputFormat.class);
	   // job.setMapperClass(MaxSpendMapper.class);
	    
	    MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,MaxRevPrdMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,MaxRevPrdMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class,MaxRevPrdMapper.class);
	    MultipleInputs.addInputPath(job,new Path(args[3]), TextInputFormat.class,MaxRevPrdMapper.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    
	    //job.setNumReduceTasks(4);
	   
	    job.setReducerClass(MaxRevPrdReducer.class);  	    
	   
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    // File Output Format
	    FileOutputFormat.setOutputPath(job, new Path(args[4]));    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}

class MaxRevPrdMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
    String[] line = value.toString().split(";");        
    String prd_id = line[5].trim();      
    Long amt = Long.parseLong(line[8]);
  
    context.write(new Text(prd_id),new LongWritable(amt));           
  }    
} 
class MaxRevPrdReducer extends Reducer<Text, LongWritable,Text, LongWritable> 
{   	
          
    long max1 = 0;
    String custID =" ", date1= " ";
    
    private TreeMap<Long,String> countMap = new TreeMap<Long,String>();
    @Override            
public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException 
{              
   long sum = 0;      
   
   for (LongWritable val : values)
    {  
	       sum+=val.get();    	  		      	   	   
    }       			   
      
      countMap.put(sum,key.toString());             
 }
    @Override
public void cleanup( Context context) throws IOException, InterruptedException
{
    	NavigableMap<Long, String> nMap = countMap.descendingMap();
    
    			int count = 0;
    	for(Map.Entry<Long, String> entry : nMap.entrySet()) 
    	{
    		count ++;
    		if (count < 11)
    		{
    		  long prd_sum = entry.getKey();
    		  String prd_id = entry.getValue();
              context.write(new Text(prd_id),new LongWritable(prd_sum));
    		}
             else break;
                 		   
    	}   	
    }
}
