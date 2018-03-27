package com.udf.my_udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MYUDF extends EvalFunc<DataBag>
{

	

	@Override
	public DataBag exec(Tuple input) throws IOException {
		
		
		if (input == null || input.size() == 0) {
			return null;

		}else {
			try {
				
				DataBag output = BagFactory.getInstance().newDefaultBag();
				
				String strinput = (String)input.get(0);
				String[] splits = strinput.split("\\|");
				
				for (String split : splits){
					Tuple tempt = TupleFactory.getInstance().newTuple(split);
					
					output.add(tempt);
				}
				return output;
				
			}catch(Exception e) {
				throw new IOException("Caught exception processing input row ", e);
			}
		}
		
		
		
	}
	
    public static void main( String[] args ) throws IOException
    {
//    		DefaultTuple tuple = new DefaultTuple();
//    		tuple.append("horror|action"); // should be {(horror),(action)}
//    		
//        DataBag db = new MYUDF().exec(tuple);
//        System.out.println(db);  // {(horror),(action)}
        
    }


	
}
