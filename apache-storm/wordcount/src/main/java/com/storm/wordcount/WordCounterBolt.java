package com.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCounterBolt implements IRichBolt {
	
	Integer id;
	String name;
	Map<String, Integer> counters; // in real case this should be something cetralized like redis or casendra hbase 
	private OutputCollector collector;


	
	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	public void cleanup() {
		System.out.println(" -- Word Counter Bolt [ Component Id = "+ name + " - Task Id = "+id +"]");
		for(Map.Entry<String, Integer> entry:counters.entrySet()){
			System.out.println(entry.getKey()+" : " + entry.getValue());
		}

	}
	
	
	/**
	 * If the word dosn't exist in the map we will create
	 * this, if not We will add 1 
	 */
	public void execute(Tuple input) {
		String str = input.getString(0);
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) +1;
			counters.put(str, c);
		}
		collector.ack(input);
	
		

	}

	/**
	 * On create 
	 */ 
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		


	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
