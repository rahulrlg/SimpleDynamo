package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Map;

public class MessageResponse extends MessageWrapper  {
	String status;
	Map<String,String> mapData;
	Map<String,Integer> mapVersion;
	
	public MessageResponse(String status,Map<String,String> mapData, Map<String,Integer> mapVersion){
		this.status=status;
		this.mapData=mapData;
		this.mapVersion=mapVersion;
	}
	
}
