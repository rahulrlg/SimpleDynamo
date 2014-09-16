package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Map;

public class MessageRequest extends MessageWrapper {
	/**
	 * 
	 */
	String sender;
	String value;
	String key;
	Map<String,String> mapRecoverNodes;
	int version;
	
	public MessageRequest(String sender,String receiver,String key,String value,int version, Map<String,String> mapData,String messageType){
		
		this.sender=sender;
		super.receiver=receiver;
		this.key=key;
		this.value=value;
		this.version=version;
		this.mapRecoverNodes=mapData;
		super.messageType=messageType;
	}
}
