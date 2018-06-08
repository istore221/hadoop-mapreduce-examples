package com.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.TopologyHistoryInfo;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

class HelloStrom 
{
    public static void main( String[] args ) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException
    {
    	
    		/*
    		 * 
    		 * one spouts for reading file 
    		 * 
    		 * two instance of word-splitter , this bolt will get tuples from spouts in random way and emit word by word to 
    		 * next down streaming bolts 
    		 * 
    		 * two instance of word-counter this listen to the tuples emit by word-splitter bolt and since it has two instances
    		 * it gets the data using field hash way : eg: same world will always endup in same bolt instance much like partitoning
    		 * in map reduce 
    		 * 
    		 *  
    		 *  we can do the same thing using our custom code using something like python ot nodejs or java using message queues
    		 *  but storm will do it automatically 
    		 *  
    		 *  without storm
    		 *  
    		 *  one nodejs app to read textfile and put in into message queue A
    		 *  
    		 *  two python scripts listen to mesage queue A and it will process data and put in to message queue B
    		 *  
    		 *  another two python scripts listen to message queute B and read word and update the count 
    		 *  
    		 *  
    		 * 
    		 */
        
    		Config config = new Config();
    		config.put("inputFile", args[0]);
    		config.setDebug(true);
    		config.setMaxSpoutPending(5); // This value puts a limit on how many tuples can be in flight
    		config.setNumWorkers(3);
    		
    		
    		
    		
    		TopologyBuilder builder = new TopologyBuilder();
    		builder.setSpout("line-reader-spout", new LineReaderSpout(),1); // only need 1 instance of spouts
    		
    		builder.setBolt("word-spitter", new WordSpitterBolt(),2)
    		.shuffleGrouping("line-reader-spout"); // bolt (2 instances) need data from word-splitter spout in random way
    		
    		
    		builder.setBolt("word-counter", new WordCounterBolt(),2)
    		.fieldsGrouping("word-spitter", new Fields("word"));
    		
    		
    		

    		
    		
    	    LocalCluster cluster = new LocalCluster();
   	    cluster.submitTopology("wordCounter", config, builder.createTopology());
   	    //StormSubmitter.submitTopology("wordCounter", config, builder.createTopology()); // use this on production
   	    
   	    
   	    
    	    
    	    Thread.sleep(5000);
    	    cluster.killTopology("wordCounter");
    	    cluster.shutdown();
    	
    }
}
