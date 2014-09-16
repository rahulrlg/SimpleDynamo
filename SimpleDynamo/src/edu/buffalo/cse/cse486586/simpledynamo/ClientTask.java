package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

import android.util.Log;

public class ClientTask implements Callable<MessageResponse> {
	MessageWrapper requestToSend;
	
	public ClientTask(MessageWrapper request){
		this.requestToSend=request;
	}
	
	private MessageResponse requestServer()
	 {
		ObjectOutputStream out=null;
		ObjectInputStream in=null;
		Socket socket=null;
		MessageResponse response=null;
		try
		{
			//Log.d(TAG, objMessageWrapper.strAction+"in client Task");
			//Log.d(TAG, objMessageWrapper.strSucc_or_key);
			socket=new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}), Integer.parseInt(requestToSend.receiver)*2); 
			//Log.d(TAG, strSenderPort+" check");
			out=new ObjectOutputStream(socket.getOutputStream());
			in=new ObjectInputStream(socket.getInputStream());
			socket.setSoTimeout(3000);
			//Log.d(TAG, objMessageWrapper.toString());
			out.writeObject(requestToSend);
			response=(MessageResponse)in.readObject();
		}
		catch(StreamCorruptedException ex)
		{
			Log.e("ClientTask","Stream Corrupted.Message:"+ex);
		}
		catch(SocketTimeoutException ex)
		{
			Log.e("ClientTask","Socket Time Out.Message:"+ex);
		}
		catch(IOException ex)
		{
			Log.e("ClientTask","Unable to create socket.Message:"+ex);
		}
		catch(Exception ex)
		{
			
			//Log.e(TAG,"Error in requestServer().:"+ex);
		}
		finally
		{
			try
        	{
        			if(null!=out){
        				out.close();
        			}
   			 	socket.close();
   			 	return response;
        	}
        	catch(IOException e)
    		{
    			Log.e("Client Task","Unable to close the stream:"+e);
    		}
		}
		return response;
	 }
	@Override
	public MessageResponse call() throws Exception {
		// TODO Auto-generated method stub
		
		return requestServer();
	}

}
