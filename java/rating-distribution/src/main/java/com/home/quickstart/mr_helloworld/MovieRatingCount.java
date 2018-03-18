package com.home.quickstart.mr_helloworld;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;


//https://www.edureka.co/blog/mapreduce-tutorial/
public class MovieRatingCount{
	
	// Mapper Phase Code
	/*
	 * Input:	  
		The key is nothing but the offset of each line in the text file: LongWritable
		The value is each individual line : Text
		<offset,line_text>
		Example:
			input.data
				0	50	5	881250949   # userid | movieid | rating | timestamp
				196	242	3	881250949   # userid | movieid | rating | timestamp
				
		offset means
		  byte_offset	0	50	5	881250949
		  byte_offset	196	242	3	881250949
		  
		   public void map(LongWritable key, Text value, Context context) ==   extends Mapper<LongWritable, Text, xxx, xxx>
		  
		  TextInputFormat is the default InputFormat . 
		  Each record is a line of input. The key is a LongWritable , is the byte offset within the file of the beginning of the line. 
		  The value is the contents of the line, excluding any line terminators (e.g., newline or carriage return), and is packaged as a Text object. So a file containing the following text:
	 * 
	 * Output:
			The key is the rate value which can be in range of 1-5 : IntWritable
			We have the hardcoded value in our case which is 1: IntWritable
			Example – 1 1, 5 1, 3 1 
	 * 
	 * 
	 */
	public static class MapKlass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		 private IntWritable rate = new IntWritable();
		 private static IntWritable one = new IntWritable(1);
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String line = value.toString();
             String[] splits = line.split("\\s+");
             rate.set(Integer.parseInt(splits[2]));
             context.write(rate, one);
		 }
		 

	}
	
	// Reducer Phase Code
	/*
	 * Input:
			The key nothing but those unique rate value [1-5] which have been generated after the sorting and shuffling phase: IntWritable
			The value is a list of integers corresponding to each key: IntWritable
			Example – 1(rank), [1, 1](value list of int), etc.
			
		Output:
			The key is all the unique rank present in the input text file: IntWritable
			The value is the number of occurrences of each of the unique words: IntWritable
			Example – 1 100 , 2 50 , 3 14 , 4 5 , 5 80 
			
		In general, a single reducer is created for each of the unique rate value, but, you can specify the number of reducer in mapred-site.xml.

	 */
	public static class ReduceKlass extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			
			int sum=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key, new IntWritable(sum));
	
		}
		
	}

	
	// Driver Code
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
		    	Configuration conf= new Configuration();
		    	Job job = new Job(conf,"MovieRatingCount MR Job");
		    	job.setJarByClass(MovieRatingCount.class);
		    	job.setMapperClass(MapKlass.class);
		    	job.setReducerClass(ReduceKlass.class);
		    	
		    	// the data type of input/output of the mapper
		    	job.setOutputKeyClass(IntWritable.class);
		    	job.setOutputValueClass(IntWritable.class);
		    	
		    	/*
		    	 * The method setInputFormatClass () is used for specifying that how a Mapper will read the input data or what will be the unit of work.
		    	 *  Here, we have chosen TextInputFormat so that single line is read by the mapper at a time from the input text file.
		    	 */
		    	job.setInputFormatClass(TextInputFormat.class);
		    	job.setOutputFormatClass(TextOutputFormat.class);
		   
 	 
		    	//The path of the input and output folder as cmd line arg
		    	FileInputFormat.addInputPath(job, new Path(args[0]));
		    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	
		    	boolean result = job.waitForCompletion(true);
    	        System.exit(result ? 0 : 1);
    	        
    	        /*
    	         * runing the job on hadoop
    	         * 
    	         * maven package -> to build the jar file out of this 
    	         * 
    	         * #copy the jar file where the hadoop is running 
    	         * 
    	         * #upload input file to hdfs 
    	         * hadoop fs -put u.data  u.data
    	         * 
    	         * #verify
    	         * hadoop fs -cat /user/hadoop/u.data
    	         * 
    	         * #run the MR job
    	         * 
    	         * hadoop jar mr-helloworld-0.0.1-SNAPSHOT.jar com.home.quickstart.mr_helloworld.MovieRatingCount u.data mrjobresult
    	         * 
    	         * hadoop jar <Job jar file> <classname> <input_file hdfs path> <output dir hdfs path>					
    	         * 
    	         * u.data resloved to /user/hadoop<job runner username>/u.data
    	         * mrjobresult resloved to /user/hadoop<job runner username>/mrjobresult
    	         * 
    	         */
    	        
    	        // 
    	        
    	        // to see final result pwd is current user's home so dont need to add cat /user/hadoop/mrjobresult/part-r-00000
    	        // hadoop fs -cat mrjobresult/part-r-00000

    	        
        
    }
}
