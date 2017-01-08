
	package lesson1;

	import java.io.IOException;

	import java.util.HashMap;
	import java.util.Map;
	import java.util.TreeMap;
	import org.apache.hadoop.conf.Configuration;
	import lesson1.PrdSalesGroupingComparator;
	import lesson1.PrdSalesPartitioner;
	import lesson1.PrdSalesSortComparator;
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

	public class PrdSales2ByAgeGroup {
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "PrdSales2ByAgeGroup");	
		    
		    
		    
		    job.setJarByClass(PrdSales2ByAgeGroup.class); 
		    job.setInputFormatClass(TextInputFormat.class);
		     
		    
		    MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,PrdSales2Mapper.class);
		    MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,PrdSales2Mapper.class);
		    MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class,PrdSales2Mapper.class);
		    MultipleInputs.addInputPath(job,new Path(args[3]), TextInputFormat.class,PrdSales2Mapper.class);
		    
		    job.setMapOutputKeyClass(TextPair.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    
		    job.setNumReduceTasks(10);
		    job.setSortComparatorClass(PrdSalesSortComparator.class);
		    job.setGroupingComparatorClass(PrdSalesGroupingComparator.class);
		    	       
		    job.setPartitionerClass(PrdSalesPartitioner.class);
		    job.setReducerClass(PrdSales2Reducer.class);  	    
		   
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    
		    // File Output Format
		    FileOutputFormat.setOutputPath(job, new Path(args[4]));    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	
		}
	}
	    
	  class PrdSales2Mapper extends Mapper<LongWritable, Text, TextPair, LongWritable>
	  {
		  @Override  
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {
	      String[] line = value.toString().split(";");        
	      String Age_group = line[2].trim();
	      String Prd_subclass = line[4].trim();
	      //String Prd_id = line[5].trim();
	      long amt= Long.parseLong(line[8]);    		  
	    
	      context.write(new TextPair(new Text(Age_group),new Text(Prd_subclass)),new LongWritable(amt));           
	    }    
	 } 
	 
	   class PrdSales2Reducer extends Reducer<TextPair, LongWritable,Text, LongWritable> 
	    {   	           
	              
	       private TreeMap<Long,String> countMap = new TreeMap<Long,String>();
	       private Map<String,String> map = new HashMap<String,String>();    
	       private int i = 0;
	                      
	        
	            
	   public void reduce(TextPair key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException 
	    {              
	       long sum = 0; 
	              
	       String Prd_Subclass = "",Age_Group = " ", Age_Grp = " ";
	        
	       
	        map.put("A","Age Group < 25");
		    map.put("B","Age Group 25-29");
		    map.put("C","Age Group 30-34");
		    map.put("D","Age Group 35-39");
		    map.put("E","Age Group 40-44");
		    map.put("F","Age Group 45-49");
		    map.put("G","Age Group 50-54");
		    map.put("H","Age Group 55-59");
		    map.put("I","Age Group 60-64");
		    map.put("J","Age Group > 65");
	        
	       System.out.println("GROUPING"+ ++i);
	       for (LongWritable val : values)
	        {  
	    	   sum+=val.get();
	    	       
	    	   Prd_Subclass = key.getSecond().toString();
	               
	               System.out.print(key.getFirst().toString()+key.getSecond().toString()+"\t");
	               System.out.println(val.get());
	        }
	          Age_Group = key.getFirst().toString();
	          
	        
	          
	          if(map.containsKey(Age_Group))
	             {
	               Age_Grp = map.get(Age_Group);
	             }
	          else
	             {
	                 Age_Grp = "KEY NOT FOUND";
	              }
	                
	          String Value = Age_Grp+"\t"+"\t"+Prd_Subclass;
	          countMap.put(sum, Value);
		  }
	        @Override
	    public void cleanup(Context context) throws IOException, InterruptedException, NullPointerException
	    {  	 
	          	
	         long MaxAmount = countMap.pollLastEntry().getKey();        	
	    	 String Age_SubPrdClass_PrdID = countMap.pollLastEntry().getValue().toString();    	     	 
	    	 context.write(new Text(Age_SubPrdClass_PrdID),new LongWritable(MaxAmount)); 	   	  
	    	 
	      }
	    }  
	    	 
	    		 
	    	 
	    	 
	    	 
	    	 
	    	 
	    		 
	    	 
	    
	    
	    	

