package com.mapreduce.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class MultipleInputJoinMapReduce 
{
	
	static class Mapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		 private Text emptyText = new Text();
		 private IntWritable movieid = new IntWritable();
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\\s+");
            movieid.set(Integer.parseInt(splits[0]));
            emptyText.set("");
            context.write(movieid,emptyText);
		 }

	}
	
	static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		 private Text moviename = new Text();
		 private IntWritable movieid = new IntWritable();
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           String[] splits = line.split("\\|");
           movieid.set(Integer.parseInt(splits[0]));
           moviename.set(splits[1]);
           context.write(movieid, moviename);
		 }
		 

	}
	
	static class MultipleReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		
		
		Text moviename = new Text();
		IntWritable movieid = new IntWritable();
		
				
		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
			
			
			String mname = "";
			
			for(Text value:values)
			{
				if(value.toString() != "") {
					mname = value.toString();
				}
				
			}
			
			moviename.set(mname);
			context.write(key, moviename);

		}
		
	}

	
	
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	
     	Configuration conf= new Configuration();
	    	Job job = new Job(conf,"MovieName Join MR Job");
	    	job.setJarByClass(MultipleInputJoinMapReduce.class);
	    	job.setReducerClass(MultipleReducer.class);

	    	
	    job.setOutputKeyClass(IntWritable.class); 
	    	job.setOutputValueClass(Text.class);
	  
	    	MultipleInputs.addInputPath(job, new Path("u.data"), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job,new Path("u.item"), TextInputFormat.class, Mapper2.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		
		boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
        
		
    }
}
