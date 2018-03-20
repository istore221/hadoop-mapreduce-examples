package com.home.quickstart.mr_helloworld;



import static org.junit.Assert.assertEquals;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.home.quickstart.mr_helloworld.MovieRatingCount.MapKlass;
import com.home.quickstart.mr_helloworld.MovieRatingCount.ReduceKlass;





public class MovieRatingCountTest
{
    MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;
    ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, IntWritable, IntWritable, IntWritable, IntWritable> mapReduceDriver;
    
    
    @Before
    public void setUp() {
        MapKlass mapper = new MapKlass();
        ReduceKlass reducer = new ReduceKlass();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
     }
    
    @Test
     public void testMapper() throws IOException {
         
        // simulate u.data file
        mapDriver
                .withInput(
                    new LongWritable(0L),
                    new Text( "0     50 5 881250949"))
                .withInput(
                        new LongWritable(1L),
                        new Text( "0     50 1 881250949"))
                .withInput(
                        new LongWritable(2L),
                        new Text( "0     50 3 881250949"));
         
         // expected output
         mapDriver.withOutput(
                    new IntWritable(5),
                    new IntWritable(1)
                 )
                 .withOutput(
                        new IntWritable(1),
                        new IntWritable(1)
                  )
                 .withOutput(
                        new IntWritable(3),
                        new IntWritable(1) 
                 );
         
         mapDriver.runTest();
                             
                
     }
    
    
    @Test
    public void testMapperWithAssertions() throws IOException {
         
        // simulate u.data file
        mapDriver
                .withInput(
                            new LongWritable(0L),
                            new Text( "0     50 5 881250949"))
                .withInput(
                        new LongWritable(1L),
                        new Text( "0     50 1 881250949"))
                .withInput(
                        new LongWritable(2L),
                        new Text( "0     50 3 881250949"));
         
 
        List<Pair<IntWritable, IntWritable>> expected = new ArrayList<Pair<IntWritable, IntWritable>>();
        expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(5),new IntWritable(1)));
        expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(1),new IntWritable(1)));
        expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(3),new IntWritable(1)));
        
        List<Pair<IntWritable, IntWritable>> result = mapDriver.run();
        
        assertEquals(expected,result);
                    
     }

    
    @Test
     public void testReducer() throws IOException {
        
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));

        
        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
    
        
        reduceDriver
                .withInput(new IntWritable(3), values1)
                .withInput(new IntWritable(5), values2);
                
        
                
                
            reduceDriver
                .withOutput(new IntWritable(3), new IntWritable(4))
                .withOutput(new IntWritable(5), new IntWritable(2));
         
            
        reduceDriver.runTest();

     }
    
    
    @Test
     public void testReducerWithAssertions() throws IOException {
        
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));

        
        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
    
        
        reduceDriver
                .withInput(new IntWritable(3), values1)
                .withInput(new IntWritable(5), values2);
                
        
            
        List<Pair<IntWritable, IntWritable>> expected = new ArrayList<Pair<IntWritable, IntWritable>>();
        expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(3),new IntWritable(4)));
        expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(5),new IntWritable(2)));
                
        List<Pair<IntWritable, IntWritable>> result = reduceDriver.run();
        
        assertEquals(expected, result);
         
            

     }
    
    
    @Test
    public void testMapReduce() throws IOException {
        
        mapReduceDriver
                .withInput(
                            new LongWritable(0L),
                            new Text( "0     50 5 881250949"))
                .withInput(
                        new LongWritable(1L),
                        new Text( "0     50 1 881250949"))
                .withInput(
                        new LongWritable(2L),
                        new Text( "0     50 3 881250949"))
                .withInput(
                        new LongWritable(0L),
                        new Text( "0     50 5 881250949"));
        
        mapReduceDriver
            .withOutput(new IntWritable(1), new IntWritable(1))
            .withOutput(new IntWritable(3), new IntWritable(1))
            .withOutput(new IntWritable(5), new IntWritable(2));
        
        mapReduceDriver.runTest();

        
                
    }
    
    
    @Test
    public void testMapReduceWithAssertions() throws IOException {
        
        mapReduceDriver
                .withInput(
                            new LongWritable(0L),
                            new Text( "0     50 5 881250949"))
                .withInput(
                        new LongWritable(1L),
                        new Text( "0     50 1 881250949"))
                .withInput(
                        new LongWritable(2L),
                        new Text( "0     50 3 881250949"))
                .withInput(
                        new LongWritable(0L),
                        new Text( "0     50 5 881250949"));
        
                
         List<Pair<IntWritable, IntWritable>> result = mapReduceDriver.run();
         
         List<Pair<IntWritable, IntWritable>> expected = new ArrayList<Pair<IntWritable, IntWritable>>();
         expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(1),new IntWritable(1)));
         expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(3),new IntWritable(1)));
         expected.add(new Pair<IntWritable, IntWritable>(new IntWritable(5),new IntWritable(2)));
            
         assertEquals(expected, result);

        
                
    }

    


}

