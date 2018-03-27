package com.udf.my_udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class MYUDFTest  
{
	@Test
	public void classExtendsEvalFunc() {
	     assertTrue(new MYUDF() instanceof EvalFunc);
	}
	
	@Test
	public void testWithTwoValues() throws IOException {
	    String inputText = "horror|action";
	    
	    DataBag expectedOutput = BagFactory.getInstance().newDefaultBag();
	    expectedOutput.add(TupleFactory.getInstance().newTuple("horror") );
	    expectedOutput.add(TupleFactory.getInstance().newTuple("action") );
	   
	 
	    DefaultTuple input = new DefaultTuple();
	    input.append(inputText);
	    
	    
	    assertEquals(expectedOutput, new MYUDF().exec(input));
	}
	
	@Test
	public void testWithManyValues() throws IOException {
	    String inputText = "horror|action|advanture|suspense";
	    
	    DataBag expectedOutput = BagFactory.getInstance().newDefaultBag();
	    expectedOutput.add(TupleFactory.getInstance().newTuple("horror") );
	    expectedOutput.add(TupleFactory.getInstance().newTuple("action") );
	    expectedOutput.add(TupleFactory.getInstance().newTuple("advanture") );
	    expectedOutput.add(TupleFactory.getInstance().newTuple("suspense") );

	 
	    DefaultTuple input = new DefaultTuple();
	    input.append(inputText);
	    
	    
	    assertEquals(expectedOutput, new MYUDF().exec(input));
	}

   
}
