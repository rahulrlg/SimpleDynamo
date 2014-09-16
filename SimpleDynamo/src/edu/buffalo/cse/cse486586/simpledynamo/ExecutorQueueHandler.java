package edu.buffalo.cse.cse486586.simpledynamo;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ExecutorQueueHandler {
	static ExecutorService executor=Executors.newSingleThreadExecutor();
	
	public static ExecutorService  getExecutor(){
		return executor;
	}
}
