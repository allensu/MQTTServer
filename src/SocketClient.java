import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;

import MQTTLib.ConnAckMessage;
import MQTTLib.ConnectMessage;
import MQTTLib.DisconnectMessage;
import MQTTLib.Message;
import MQTTLib.MessageInputStream;
import MQTTLib.MessageOutputStream;
import MQTTLib.PublishMessage;
import MQTTLib.QoS;
import MQTTLib.SubscribeMessage;


public class SocketClient {

	private MessageInputStream in;
	private Socket socket;
	private MessageOutputStream out;
	private MqttReader reader;
	private Semaphore connectionAckLock;
	private final String id;

	public SocketClient(String id) {
		this.id = id;
	}

	public void connect(String host, int port)
			throws UnknownHostException, IOException, InterruptedException {
		socket = new Socket(host, port);
		InputStream is = socket.getInputStream();
		in = new MessageInputStream(is);
		OutputStream os = socket.getOutputStream();
		out = new MessageOutputStream(os);
		reader = new MqttReader();
		reader.start();
		ConnectMessage msg = new ConnectMessage(id, false, 60);
		connectionAckLock = new Semaphore(0);
		out.writeMessage(msg);
		connectionAckLock.acquire();
	}

	public void publish(String topic, String message) throws IOException {
		PublishMessage msg = new PublishMessage(topic, message);
		out.writeMessage(msg);
	}

	public void subscribe(String topic) throws IOException {
		SubscribeMessage msg = new SubscribeMessage(topic, QoS.AT_MOST_ONCE);
		out.writeMessage(msg);
	}

	public void disconnect() throws IOException {
		DisconnectMessage msg = new DisconnectMessage();
		out.writeMessage(msg);
		socket.close();
	}

	private void handleMessage(Message msg) {
		if (msg == null) {
			return;
		}
		switch (msg.getType()) {
		case CONNACK:
			handleMessage((ConnAckMessage) msg);
			break;
		case PUBLISH:
			handleMessage((PublishMessage) msg);
			break;
		default:
			break;
		}
	}

	private void handleMessage(ConnAckMessage msg) {
		connectionAckLock.release();
	}

	private void handleMessage(PublishMessage msg) {
				
		String str=msg.getDataAsString();
		
		mongodb db = new mongodb(); 
		
		String[] command=str.split(":");
		
		//System.out.printf("1->"+command[1]);
		//System.out.printf(" 2->"+command[2]);
		
		if(command[1].equals("del")){
			System.out.println("User has recieved message id: "+command[2]+" , so the message has been deleted.");
			db.delRecv(command[2]);
		}
		
//		else if(command[1].equals("online")){
//			System.out.println("User : "+command[2]+" is now online!");
//			db.resentNotRecv(command[2]);
//		}
		
	}
	
	


	private class MqttReader extends Thread {

		@Override
		public void run() {
			Message msg;
			try {
				while (true) {
					msg = in.readMessage();
					handleMessage(msg);
					
				}
			} catch (IOException e) {
			}
		}
	}

}
