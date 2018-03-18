package com.home.quickstart.populer_movies;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class PopulerMovies 
{
	
	public static class MapperKlass1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		
		 private IntWritable movieId = new IntWritable();
		 private static IntWritable one = new IntWritable(1);
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\\s+");
            movieId.set(Integer.parseInt(splits[1]));
            context.write(movieId, one);
		 }
		 

	}
	
	
	public static class ReduceKlass1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
			
			int sum=0;
			for(IntWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key,new IntWritable(sum));
	
		}
	}
	
	
	// sorting phase  
	// job2.setInputFormatClass(KeyValueTextInputFormat.class); 
	public static class MapperKlass2 extends Mapper<Text, Text, IntWritable, IntWritable> {
		
		 IntWritable popularity = new IntWritable();
		 IntWritable movieId = new IntWritable();

		
		 public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			 
			 movieId.set(Integer.parseInt(key.toString()));
			 popularity.set(Integer.parseInt(value.toString()));
			
			 
           context.write(popularity, movieId);
		 }
		 

	}
	
	public static class ReduceKlass2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		 IntWritable movieId = new IntWritable();
		  
		  public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException {
				
			  for (IntWritable value : values) {
				  	movieId.set(value.get());
			        context.write(movieId,key);
			    }

		
		  }


	
	}

	
			
	
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	
    		JobControl jobControl = new JobControl("PopulerMovies jobChain");
    		
    		
    		/*
    		 * job 1
    		 */
    		  

	    Configuration job1Config = new Configuration();
	    	Job job1 = new Job(job1Config,"PopulerMovies");
	    	
	    	job1.setJarByClass(PopulerMovies.class);
	    	job1.setMapperClass(MapperKlass1.class);
	    	job1.setReducerClass(ReduceKlass1.class);
	    	
	    	
	    	job1.setOutputKeyClass(IntWritable.class);
	    	job1.setOutputValueClass(IntWritable.class);
	    	
	   
	    	job1.setInputFormatClass(TextInputFormat.class);
	    	job1.setOutputFormatClass(TextOutputFormat.class);
	   
	    	
	    	FileInputFormat.addInputPath(job1, new Path(args[0]));
	    	FileOutputFormat.setOutputPath(job1, new Path("intermediate_output")); 
	    	
	    		
	    	ControlledJob controlledJob1 = new ControlledJob(job1Config);
	    controlledJob1.setJob(job1);
	    
	    jobControl.addJob(controlledJob1);
	    
	    /*
		 * job 2
		 */

	    Configuration job2Config = new Configuration();
	 	Job job2 = new Job(job2Config,"PopulerMovies Sorting By Popularity");
	 	job2.setJarByClass(PopulerMovies.class);
	 	job2.setMapperClass(MapperKlass2.class);
	 	job2.setReducerClass(ReduceKlass2.class);
	 	
	 	job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    job2.setInputFormatClass(KeyValueTextInputFormat.class); 
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job2, new Path("intermediate_output")); // output from first job
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

	    
	   
	    ControlledJob controlledJob2 = new ControlledJob(job2Config);
	    controlledJob2.setJob(job2);
	 	
	   // make job2 dependent on job1
	    controlledJob2.addDependingJob(controlledJob1); 
	    
	   // add the job to the job control
	    jobControl.addJob(controlledJob2);
	    
	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();
	    
	    
	    while (!jobControl.allFinished()) {
	        System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
	        System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
	        System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
	        System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
	        System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
	        
	        Thread.sleep(5000);
	        
	    }
	    
	  
	    System.exit(job2.waitForCompletion(true)?0:1);
	    
	  
        
    }
}
