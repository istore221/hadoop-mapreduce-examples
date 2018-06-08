package com.storm.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class LineReaderSpout implements IRichSpout {


	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;

	
	
	
	
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		try {
			fileReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	public void nextTuple() {
		
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str), str);
			
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading typle", e);
		} finally {
			completed = true;
		}
		
	}
	
	/**
	 * We will create the file and get the collector object
	 */

	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file "
					+ conf.get("inputFile"));
		}
		this.collector = collector;
		
	}

	/**
	 * Declare the output field "line"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	public boolean isDistributed() {
		return false;
	}

	

}
