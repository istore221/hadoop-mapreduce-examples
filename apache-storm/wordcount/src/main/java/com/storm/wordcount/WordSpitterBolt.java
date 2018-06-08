package com.storm.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordSpitterBolt implements IRichBolt {
	
	private OutputCollector collector;

	public void cleanup() {
		// TODO Auto-generated method stub

	}
	
	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split("\\s+");
		for(String word: words){
			word = word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		collector.ack(input);


	}

	/**
	 * On create 
	 */
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.collector = collector;

	}
	
	/**
	 * The bolt will only emit the field "word" 
	 */

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
