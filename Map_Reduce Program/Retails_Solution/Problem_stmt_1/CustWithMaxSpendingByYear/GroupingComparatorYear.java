package lesson1;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorYear extends WritableComparator
{
	    protected GroupingComparatorYear() 
	    {
	        super(TextPair.class, true);
	    }   
	    @SuppressWarnings("rawtypes")
	    @Override
	    
	    public int compare(WritableComparable w1, WritableComparable w2) 
	    {
	        TextPair k1 = (TextPair)w1;
	        TextPair k2 = (TextPair)w2;
	        
	        SimpleDateFormat format1 = new SimpleDateFormat("YYYY");
	        Date year1 = null, year2 = null;
	       try
	        {
	        year1 = format1.parse(k1.getFirst().toString().substring(0,4));
	        year2 = format1.parse(k2.getFirst().toString().substring(0,4));	       
	        }
	        catch(ParseException e)
	        {
	        e.printStackTrace();
	        }
	        
	        int result = year1.compareTo(year2);
	        if (result == 0)
	        {
	        	result = k1.getSecond().toString().compareTo(k2.getSecond().toString());
            }
	        return result;	        
	    }
}

	


