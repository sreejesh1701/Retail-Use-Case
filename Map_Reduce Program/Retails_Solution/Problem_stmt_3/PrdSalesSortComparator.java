package lesson1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PrdSalesSortComparator extends WritableComparator
{
	protected PrdSalesSortComparator() {
        super(TextPair.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TextPair k1 = (TextPair)w1;
        TextPair k2 = (TextPair)w2;

         
        int result = k1.getFirst().toString().compareTo(k2.getFirst().toString());
        if (result == 0)
          {
             result = k1.getSecond().toString().compareTo(k2.getSecond().toString());
    
          }
        
        return result;
    }	
}
