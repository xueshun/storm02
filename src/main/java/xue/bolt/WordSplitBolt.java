package xue.bolt;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 对字符进行拆分
 * @author Administrator
 *
 */
public class WordSplitBolt implements IRichBolt{

	private static final String ERROR_STR = "don't have a cow man";
	
	private OutputCollector collector;
	
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		 this.collector = collector;
	}
	

	@Override
	public void execute(Tuple tuple) {
	        String sentence = tuple.getStringByField("sentence");
	        String[] words = sentence.split(" ");
	        for(String word : words){
	            this.collector.emit(new Values(word));
	            //this.collector.ack(tuple);
	        }			
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	@Override
	public void cleanup() {
		
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}


}
