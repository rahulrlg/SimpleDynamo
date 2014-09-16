package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG= SimpleDynamoProvider.class.getName();
	private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    Context context=null;
	String[] columnNames={KEY_FIELD,VALUE_FIELD};
	String[] connString={"5554","5556","5558","5560","5562"};
	static final int Server_Port=10000;
	static String strSelfAvd=null;
	static boolean isSync=false;
	static SimpleDynamoProvider mThis=null;
	static final int READ_QUORUM=3;
	static final int Write_QUORUM=3;
	static List<String> lstPreferenceTable=new ArrayList<String>();
	static Map<String, String> hshMessages=new HashMap<String, String>();
	static Map<String,Integer> mapVersion=new HashMap<String, Integer>();
	public enum Action{
		PUT,
		DELETE,
		QUERY,
		RECOVER
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		//while(isSync){}
		boolean flag=false;
		MessageWrapper deleteRequest=null;
		Log.d("DELETE","Incoming request for delete for ="+selection);
    	if(selection.equals("@"))
    	{
    		
    		String[] fileNames=context.fileList();
    		flag=deleteLDump(fileNames);
    		Log.d(TAG,"Length after file delete:"+context.fileList().length);
    	}
    	else if(selection.equals("*"))
    	{
    		String receiver="";
    		for(int i=0;i<lstPreferenceTable.size();i++){
    			receiver=lstPreferenceTable.get(i);
    			Log.d("Delete","Send delete reuqest to "+ receiver);
    			deleteRequest=new MessageRequest(strSelfAvd, receiver, selection, null, 0, null, "DELETE");
    			Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(deleteRequest));
    		}
    	}
    	else
    	{
    		String key=selection;
    		String receiver=getCoordinatorNode(key);
    		int receiverIndex=lstPreferenceTable.indexOf(receiver);
    		for(int i=0;i<Write_QUORUM;i++){
    			Log.d("Delete","Iteration="+i+" preference table "+lstPreferenceTable);
    			receiverIndex=(receiverIndex+i)%5;
    			Log.d("Delete",selection+" Delete request sent to "+receiver);
    			deleteRequest=new MessageRequest(strSelfAvd, receiver, selection, null, 0, null, "DELETE");
    			Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(deleteRequest));
    			receiver=getSucc(receiver);
    		}
    	}
        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	 private boolean insertMessages(String key,String value)
    {
    	
    	while(isSync){
    		//Log.d("Waiting","waiting for lock to release at "+ strSelfAvd+". icomcing request for insert for key "+key);
    	}
    	
    	boolean flag=false;
    	FileOutputStream oStream=null;
    	int version=0;
    	try{
			if(hshMessages.containsKey(key)){
				if(hshMessages.get(key).equals(value)){
					return true;
				}
				context.deleteFile(key);
				version=mapVersion.get(key);
			}
    		oStream=context.openFileOutput(key, context.MODE_PRIVATE);
			oStream.write(value.getBytes());
			hshMessages.put(key, value);
			mapVersion.put(key, version+1);
			flag=true;
			Log.d("Insert","key inserted at "+strSelfAvd+". key="+key);
		}
		catch(FileNotFoundException ex)
		{
			Log.e(TAG, "Message:Error opening output stream."+ex.getMessage());
			flag=false;
		}
		catch(IOException ex)
		{
			Log.e(TAG, "Message: Error in write()."+ex.getMessage());
			flag=false;
		}
    	
		finally
		{
			try{
				if(oStream!=null){
					oStream.close();
				}
				
				return flag;
			}
			catch(IOException ex)
    		{
    			Log.e(TAG, "Message: unable to close output stream."+ex.getMessage());
    		}
		}
    	return flag;
    }
	 @Override
		public synchronized Cursor query(Uri uri, String[] projection, String selection,
		String[] selectionArgs, String sortOrder) {
			// TODO Auto-generated method stub
		// Log.d("Lock","Locked in query for key "+selection);
		 //while(isSync){}
		 //Log.d("Lock","Lock released in query for key "+selection);
		// isSync=true;
		 //Log.d("Lock","Lock set in query for key "+selection);
		 MatrixCursor cursor=new MatrixCursor(columnNames);
			MessageResponse objQueryResponse=null;
			Map<String,String> mapQueryResponse=new HashMap<String, String>();
			MessageWrapper queryRequest=null;
			Log.d("Query","Incoming query request for key="+selection);
			if(selection.equals("@")){
				//while(isSync){
				//	Log.d("Waiting","waiting for lock to release at "+ strSelfAvd+". icoming request for query for key "+selection);
				//}	
				String key="";
				Iterator<String> it=hshMessages.keySet().iterator();
				while(it.hasNext()){
					String[] columnValues=new String[2];
					key=it.next();
					columnValues[0]=key;
					columnValues[1]=hshMessages.get(key);
					cursor.addRow(columnValues);
				}
	    		return cursor;
			}else if(selection.equals("*")){
				Log.d("QUERY","Incoming Query * request at port :"+strSelfAvd);
				String receiver="";
	    		for(int i=0;i<lstPreferenceTable.size();i++){
	    			receiver=lstPreferenceTable.get(i);
	    			Log.d("QUERY","query * request send to "+receiver);
	    			queryRequest=new MessageRequest(strSelfAvd, receiver, selection, null, 0, null, "QUERY");
	    			Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(queryRequest));
	    			try {
						objQueryResponse=future.get();
	    				if(objQueryResponse==null){
							Log.d("QUERY",receiver+" node is down");
						}
						else{
							if(null!=objQueryResponse && null!=objQueryResponse.mapData && !objQueryResponse.mapData.isEmpty()){
								Log.d("QUERY","repsonse received from "+ receiver+" Size of data:"+objQueryResponse.mapData.size());
								mapQueryResponse.putAll(objQueryResponse.mapData);
								
							}
							
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	    		String key="";
				Iterator<String> it=mapQueryResponse.keySet().iterator();
				while(it.hasNext()){
					String[] columnValues=new String[2];
					key=it.next();
					columnValues[0]=key;
					columnValues[1]=mapQueryResponse.get(key);
					cursor.addRow(columnValues);
				}
				Log.d("Query","Befor returning cursor. Cursor rows:"+cursor.getCount());
	    		return cursor;
				
			}else{
				String key=selection;
				String value=null;
				int version=0;
	    		String receiver=getCoordinatorNode(key);
	    		String[] columnValues=new String[2];
	    		columnValues[0]=selection;
	    		int receiverIndex=lstPreferenceTable.indexOf(receiver);
	    		while(value==null){
	    		for(int i=0;i<READ_QUORUM;i++){
	    			Log.d("Query","request send to "+receiver);
	    			queryRequest=new MessageRequest(strSelfAvd, receiver, selection, value, version, null, "QUERY");
	    			Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(queryRequest));
	    			try {
						objQueryResponse=future.get();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    			if(objQueryResponse==null){
	    				Log.d("QUERY",receiver+" node is down");
	    				receiver=getSucc(receiver);
	    				continue;
	    			}else{
	    				if(null!=objQueryResponse.mapData && !objQueryResponse.mapData.isEmpty()){
	    					value=objQueryResponse.mapData.get("value");
		    				version=objQueryResponse.mapVersion.get("version");
		    				Log.d("Query","Iteration "+i+". response received from "+receiver);
		    				if(value!=null)
		    				break;
	    				}
	    				
	    			}
	    			receiver=getSucc(receiver);
	    		}
		    		try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	    		columnValues[1]=value;
	    		cursor.addRow(columnValues);
	    		Log.d("Query","before cursor is returned. Cursor count "+cursor.getCount() );
	    	//	isSync=false;
	    		Log.d("Lock","Lock unset in query for key "+selection);
				return cursor;
			}
			
		}
	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		//Log.d("Lock","Locked in client insert for key "+values.getAsString(KEY_FIELD));
		//while(isSync){}
		//Log.d("Lock","Lock released in client insert for key "+values.getAsString(KEY_FIELD));
		
		//Log.d("Lock","Lock set in client insert for key "+values.getAsString(KEY_FIELD));
		//isSync=true;
		String key=(String)values.get(KEY_FIELD);
    	String value=(String)values.get(VALUE_FIELD);
    	MessageWrapper messageWrapper=null;
    	String coordinatorNode=getCoordinatorNode(key);
    	int coordinatorIndex=lstPreferenceTable.indexOf(coordinatorNode);
    	String receiver="";
    	int receiverIndex=0;
    	Log.d("INSERT","incoming insert request at "+ strSelfAvd +"for key "+ key);
    	for(int i=0;i<Write_QUORUM;i++){
    		receiverIndex=(coordinatorIndex+i)%5;
    		receiver=lstPreferenceTable.get(receiverIndex);
    		Log.d("Insert","request sent to "+ receiver);
    		messageWrapper=new MessageRequest(strSelfAvd, receiver, key,value, 0, null, "PUT");
    		Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(messageWrapper));
    		try {
				if(null==future.get()){
					continue;
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	//isSync=false;
    	//Log.d("Lock","Lock unset in client insert for key "+values.getAsString(KEY_FIELD));
		return null;
	}
	private boolean deleteKey(String key){
		//while(isSync){}
		boolean flag=false;
		if(hshMessages.containsKey(key)){
			Log.d("Delete","Key deleted. "+ key);
			hshMessages.remove(key);
			mapVersion.remove(key);
			flag=context.deleteFile(key);
		}
		return flag;
	}
	 private boolean deleteLDump(String[] fileList){
		 //while(isSync){}	
		 boolean flag=false;
	    	for(int i=0;i<fileList.length;i++)
			{
				flag=context.deleteFile(fileList[i]);
				if(!flag)
				{
					Log.e(TAG,"file could not be deleted");
				}
			}
	    	hshMessages.clear();
	    	mapVersion.clear();
	    	return flag;
	    }
	 private String getAvd()
    {
    	TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return portStr;
        //return String.valueOf((Integer.parseInt(portStr) * 2));
    }
    private void runServer()
    {
    	
    	ServerSocket serverSocket=null;
    	try{
    		serverSocket=new ServerSocket(Server_Port);
    	}
    	catch(IOException ex){
    		Log.e(TAG, "Messag:Error in runServer."+ex.getMessage());
    	}
		new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
    }
	    
	private void setPreferenceList(){
		lstPreferenceTable.clear();
		for(int i=0;i<connString.length;i++){
			lstPreferenceTable.add(connString[i]);
		}
		Collections.sort(lstPreferenceTable, new Comparator<String>() {

			@Override
			public int compare(String one, String two) {
				// TODO Auto-generated method stub
				return genHash(one).compareTo(genHash(two));
			}
			
		});
		Log.d("Create","Preference table "+lstPreferenceTable);
	}
	private String getCoordinatorNode(String key){
		
		Iterator<String> it=lstPreferenceTable.iterator();
		String strNode="";
		int index=0;
		while(it.hasNext()){
			strNode=it.next();
			if(genHash(key).compareTo(genHash(strNode))<=0){
				break;
			}
			index=(index+1)%5;
		}
		return lstPreferenceTable.get(index);
	}
	private String getSucc(String incomingNode){
		if(incomingNode==null || incomingNode.equals("")){
			Log.d(TAG,"incoming parameter is null in getSucc");
		}
		int index=lstPreferenceTable.indexOf(incomingNode);
		return lstPreferenceTable.get((index+1)%5);
	}
	private String getPred(String incomingNode){
		if(incomingNode==null || incomingNode.equals("")){
			Log.d(TAG,"incoming parameter is null in getSucc");
		}
		int index=lstPreferenceTable.indexOf(incomingNode);
		return lstPreferenceTable.get((5+(index-1))%5);
	}
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Log.d("Create","Appliction Starts");
		mThis=this;
		context=getContext();
    	deleteLDump(context.fileList());
    	strSelfAvd=getAvd();
    	setPreferenceList();
    	runServer();
    	getRecovery();
		return true;
	}
	private void getRecovery(){
		try{
			//Avd ready to recover
			Log.d("Lock","Locked in client recovery for node "+strSelfAvd);
			//while(isSync){}
			Log.d("Lock","Lock release in client recovery for node "+strSelfAvd);
			isSync=true;
			Log.d("Lock","Lock set in client recovery for node "+strSelfAvd);
			//Delete intially stored files to be sure
			String[] fileList=context.fileList();
			deleteLDump(fileList);
			
			String pred=getPred(strSelfAvd);
			String secondPred=getPred(pred);
			String succ=getSucc(strSelfAvd);
			String secondSucc=getSucc(succ);
			MessageResponse objRecoveredResponse=null;
			Map<String,Integer> mapVersionResponse=null;
			
			//Send request to Secondary pred for recovery.
			Map<String,String> mapNodesToRecover=new HashMap<String, String>();
			mapNodesToRecover.put(secondPred, secondPred);
			MessageWrapper objRecoveryRequest=null;
			objRecoveryRequest=new MessageRequest(strSelfAvd, secondPred, null, null, 0, mapNodesToRecover, "RECOVER");
			Log.d("Recover","ready to send request to second pred "+ secondPred);
			Future<MessageResponse> future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(objRecoveryRequest));
			try {
				objRecoveredResponse=future.get();
				if(null!=objRecoveredResponse){
					Log.d("Recover","Recovery response received");
					mapNodesToRecover.clear();			// secondPred has been recovered.So clear the nodetoRecover.
					mapVersion.putAll(objRecoveredResponse.mapVersion);
					hshMessages.putAll(objRecoveredResponse.mapData);
					Log.d("Recover","Recovered Count from  node="+secondPred+":"+objRecoveredResponse.mapData.size());
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//send request to immediate pred for recovery.
			Log.d("Recover","ready to send request to pred "+ pred);
			mapNodesToRecover.put(pred, pred);
			recoveryRequest(pred, mapNodesToRecover, secondPred);
			Log.d("Recover","Recovery completed from "+pred);
			Log.d("Recover","Recovered Count:"+hshMessages.size());
			//Ready to recover from succ.
			Log.d("Recover","ready to recover from "+succ);
			mapNodesToRecover.put(strSelfAvd, strSelfAvd);
			recoveryRequest(succ, mapNodesToRecover, pred);
			Log.d("Recover","Recovery completed from "+succ);
			Log.d("Recover","Recovered Count:"+hshMessages.size());
			//ready to recover from Second Succ
			if(!mapNodesToRecover.isEmpty()){
				recoveryRequest(strSelfAvd,mapNodesToRecover,strSelfAvd);	
			}
			Log.d("Recover","Recovery completed");
			
			Iterator<String> it=hshMessages.keySet().iterator();
			Log.d("Recover","Recovered Count:"+hshMessages.size());
			String key;
			String value;
			FileOutputStream oStream=null;
			while(it.hasNext()){
				key=it.next();
				value=hshMessages.get(key);
				Log.d("Recover","Key Recovered "+key);
				oStream=context.openFileOutput(key, context.MODE_PRIVATE);
				oStream.write(value.getBytes());
			}
			
			
		}
		catch(Exception ex){
			Log.d("Recovery","Error in getRecovery() "+ex);
		}
		isSync=false;
		Log.d("Lock","Lock unset in client recovery for node "+strSelfAvd);
	}
	private void recoveryRequest(String toRecover,Map<String,String> nodesToRecover,String recovered){
		MessageResponse objRecoveredResponse=null;
		MessageWrapper objRecoveryRequest=null;
		Future<MessageResponse> future=null;
		objRecoveryRequest=new MessageRequest(strSelfAvd, toRecover, null, null, 0, nodesToRecover, "RECOVER");
		future=ExecutorQueueHandler.getExecutor().submit(new ClientTask(objRecoveryRequest));
		try {
			objRecoveredResponse=future.get();
			if(null!=objRecoveredResponse){
				nodesToRecover.clear();			// secondPred has been recovered.So clear the nodetoRecover.
				filterUpdatedVersionRecord(objRecoveredResponse);
				
			}
			else{
				nodesToRecover.remove(recovered);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	private void filterUpdatedVersionRecord(MessageResponse objRecoveredResponse){
		Map<String,Integer> mapVersionResponse=objRecoveredResponse.mapVersion;
		int incomingVersion;
		String incomingKey;
		Iterator<String> it=mapVersionResponse.keySet().iterator();
		while(it.hasNext()){
			incomingKey=it.next();
			incomingVersion=mapVersionResponse.get(incomingKey);
			if(mapVersion.containsKey(incomingKey)){
				if(incomingVersion>mapVersion.get(incomingKey)){
					mapVersion.put(incomingKey, incomingVersion);
					hshMessages.put(incomingKey,objRecoveredResponse.mapData.get(incomingKey));
				}
			}else{
				mapVersion.put(incomingKey, incomingVersion);
				hshMessages.put(incomingKey,objRecoveredResponse.mapData.get(incomingKey));
			}
		}
		
	}
	
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) {
        MessageDigest sha1=null;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    private boolean handleInsert(MessageWrapper incomingInsertRequest){
    	//while(isSync){}
    	//isSync=true;
    	boolean flag=false;
    	//Log.d("Lock","Lock set in server insert for node "+strSelfAvd);
    	MessageRequest request=(MessageRequest)incomingInsertRequest;
    	flag=insertMessages(request.key, request.value);
    	//isSync=false;
    	//Log.d("Lock","Lock unset in server insert for node "+strSelfAvd);
    	return flag;
    }
    private void handleDelete(MessageWrapper incomingDeleteRequest){
    	//while(isSync){}
    	MessageRequest deleteRequest=(MessageRequest)incomingDeleteRequest;
    	if(deleteRequest.key.equals("*")){
    		String[] fileList=context.fileList();
    		deleteLDump(fileList);
    	}else{
    		deleteKey(deleteRequest.key);
    	}
    }
    private MessageResponse keyLookUp(MessageWrapper incomingKeyQueryRequest){
    	MessageRequest objQueryRequest=(MessageRequest)incomingKeyQueryRequest;
    	MessageResponse objQueryResponse=null;
    	Map<String,String> mapDataResponse=new HashMap<String, String>();
    	Map<String,Integer> mapVersionResponse=new HashMap<String, Integer>();
    	
    	
    	int updatedVersion=objQueryRequest.version;
    	String updatedValue=objQueryRequest.value;
    	String key=objQueryRequest.key;
    	String localValue="";
    	int localVersion=0;
    	
    	if(hshMessages.containsKey(key)){
    		localVersion=mapVersion.get(objQueryRequest.key);
    		localValue=hshMessages.get(objQueryRequest.key);
    		if(null!=objQueryRequest.value){
    			if(localVersion>updatedVersion){
    				updatedVersion=localVersion;
    				updatedValue=localValue;
    			}
    		}else{
    			updatedValue=localValue;
    			updatedVersion=localVersion;
    		}
    	}
    	mapDataResponse.put("value", updatedValue);
    	mapVersionResponse.put("version", updatedVersion);
    	objQueryResponse=new MessageResponse("success", mapDataResponse, mapVersionResponse);
    	return objQueryResponse;
    }
    private MessageResponse handleQuery(MessageWrapper incomingQueryRequest){
    	//Log.d("Lock","server Query locked at "+ incomingQueryRequest.receiver);
    	//while(isSync){
    	//	Log.d("Waiting","waiting for lock to release at "+ strSelfAvd+". icomcing request for server query for key "+((MessageRequest)incomingQueryRequest).key);
    	//}
    	//Log.d("Lock","server Query lock released at "+ incomingQueryRequest.receiver);
    	
    	//Log.d("Lock","lock set at server query at "+ incomingQueryRequest.receiver);
    	//isSync=true;
    	//Log.d("Lock","Query unlocked at "+ incomingQueryRequest.receiver);
    	MessageRequest objQueryRequest=(MessageRequest)incomingQueryRequest;
    	MessageResponse objQueryResponse=null;
    	
    	if(objQueryRequest.key.equals("*")){
    		objQueryResponse=new MessageResponse("success", hshMessages, mapVersion);
    	}else{
    		objQueryResponse=keyLookUp(incomingQueryRequest);
    	}
    	//isSync=false;
    	//Log.d("Lock","lock unset at server query at "+ incomingQueryRequest.receiver);
    	return objQueryResponse;
    }
    private boolean isKeyLocation(String key,String node){
    	boolean flag=false;
    	Log.d("Recover","check recover request for "+ key);
    	int nodeIndex=lstPreferenceTable.indexOf(node);
    	//int predIndex= (5+(nodeIndex-1))%5;
    	String predNode=getPred(node);
    	String largeNode=lstPreferenceTable.get(lstPreferenceTable.size()-1);
    	Log.d("Recover","Index of node to recover"+nodeIndex+" and pred node "+predNode);
    	if(nodeIndex==0){
    		Log.d("Recover","Checking boundary condition for "+largeNode);
    		if(genHash(key).compareTo(genHash(node))<=0 || genHash(key).compareTo(genHash(largeNode))>0){
    			flag=true;
    		}
    	}
    	else{
    		Log.d("Recover","Checking normal condition for "+node);
    		if(genHash(key).compareTo(genHash(node))<=0 && genHash(key).compareTo(genHash(predNode))>0){
    			flag=true;
    		}
    	}
    	return flag;
    }
    
    private MessageResponse handleRecoveryRequest(MessageWrapper incomingRecoveryRequest){
    	
    	MessageRequest objRecoveryRequest=(MessageRequest)incomingRecoveryRequest;
    	Log.d("Recover","Incoming recovery request from "+objRecoveryRequest.sender);
    	Log.d("Recover","Data exists:"+hshMessages.size());
    	MessageResponse objRecoveryResponse=null;
    	Set<String> setNodesToRecover=objRecoveryRequest.mapRecoverNodes.keySet();
    	Map<String,String> recoveredData=new HashMap<String, String>();
    	Map<String,Integer> recoveredVersion=new HashMap<String, Integer>();
    	String key="";
    	Iterator<String> it= hshMessages.keySet().iterator();
    	while(it.hasNext()){
    		key=it.next();
    		for(String strNode:setNodesToRecover){
    			if(isKeyLocation(key, strNode)){
    				Log.d("Recover","Key is to be recovered");
    				recoveredData.put(key, hshMessages.get(key));
    				recoveredVersion.put(key, mapVersion.get(key));
    			}
    		}
    	}
    	Log.d("Recover","Data recovered:"+recoveredData.size());
    	objRecoveryResponse=new MessageResponse("success", recoveredData, recoveredVersion);
    	return objRecoveryResponse;
    }
    private class ServerTask extends AsyncTask<ServerSocket, Void, Void>
    {
    	ServerSocket socket=null;
    	Socket serviceSocket=null;
    	ObjectInputStream oisInput=null;
    	ObjectOutputStream out=null;
    	MessageWrapper objMessageWrapper=null;
    	@Override
    	protected Void doInBackground(ServerSocket... params) {
    		// TODO Auto-generated method stub
    		socket=params[0];
    		while(mThis!=null){
    			try{
    				serviceSocket=socket.accept();
    				oisInput=new ObjectInputStream(serviceSocket.getInputStream());
    				out=new ObjectOutputStream(serviceSocket.getOutputStream());
    				
    				
    				objMessageWrapper=(MessageWrapper)oisInput.readObject();
    				//Log.d(TAG,objMessageWrapper.strAction);
    				Future<MessageResponse> future=ServerExecutorQueue.getExecutor().submit(new ServerAction(objMessageWrapper));
    				MessageResponse objresponse=future.get();
    				if(objresponse!=null){
    					out.writeObject(objresponse);
    				}
    				
    			}
    			catch(IOException ex)
    			{
    				Log.e(TAG,""+ex);
    			}
    			catch(ClassNotFoundException ex)
    			{
    				Log.e(TAG,""+ex);
    			} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		return null;
    	}
    	
    }
    private class ServerAction implements Callable<MessageResponse>{

		private MessageWrapper objMessageWraper;
    	public ServerAction(MessageWrapper messageWrapper){
    		this.objMessageWraper=messageWrapper;
    	}
    	private MessageResponse actionhandler(){
    		Action action=Action.valueOf(objMessageWraper.messageType.toUpperCase());
    		MessageResponse msgResponse=null;
			switch(action)
			{
			case RECOVER:
				//Log.d("Lock","waiting for lock at server recovery at "+ strSelfAvd);
				//if(!(((MessageRequest)objMessageWrapper).sender.equals(((MessageRequest)objMessageWrapper)))){
				//	while(isSync){}
				//}
				
				//Log.d("Lock","Lock released at server recovery at "+ strSelfAvd);
				//Log.d("Recover","recovery lock set in "+strSelfAvd);
				isSync=true;
				msgResponse=handleRecoveryRequest(objMessageWraper);
				//if(!(((MessageRequest)objMessageWrapper).sender.equals(((MessageRequest)objMessageWrapper)))){
				isSync=false;
				//}
				
				//Log.d("Recover","recovery lock unset in "+strSelfAvd);
				//Log.d("Recover","Recovery repsonse from"+strSelfAvd+" and couint is "+objRecoveryResponse.mapData.size());
				
				//Log.d("Recover","sERVER RECOVERY DONE " +strSelfAvd);
				Log.d("Recover","sERVER RECOVERY DONE from " +strSelfAvd);
				break;
				
			case PUT:
				if(handleInsert(objMessageWraper)){
					msgResponse= new MessageResponse("success", null, null);
				}
				break;
			case DELETE:
				handleDelete(objMessageWraper);
				msgResponse= new MessageResponse("success", null, null);
				break;
			case QUERY:
				msgResponse= handleQuery(objMessageWraper);
				break;
			default:
				break;
			}
			return msgResponse;
    	}
    	@Override
		public MessageResponse call() throws Exception {
			// TODO Auto-generated method stub
			return actionhandler();
		}
    	
    }
}
