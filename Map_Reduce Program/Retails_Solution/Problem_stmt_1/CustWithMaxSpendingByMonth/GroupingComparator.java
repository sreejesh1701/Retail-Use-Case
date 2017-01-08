package lesson1;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator 
{
	    protected GroupingComparator() 
	    {
	        super(TextPair.class, true);
	    }   
	    @SuppressWarnings("rawtypes")
	    @Override
	    
	    public int compare(WritableComparable w1, WritableComparable w2) 
	    {
	    	
	        TextPair k1 = (TextPair)w1;
	        TextPair k2 = (TextPair)w2;
	        
	        SimpleDateFormat formatter1 = new SimpleDateFormat("YYYY-MM");
	        
	        Date date1 = null , date2 = null; 
	        try
	        {
	        date1 = formatter1.parse(k1.getFirst().toString().substring(0,7));
	        date2 = formatter1.parse(k2.getFirst().toString().substring(0,7));
	    
	        }
	        catch(ParseException e)
	        {
	        e.printStackTrace();
	        }
	        
	        int result = date1.compareTo(date2);
	        if (result == 0)
	        {
	                  result = k1.getSecond().toString().compareTo(k2.getSecond().toString());
	        }
	        return result;	             
	        }	        
	    }
	


