import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import MQTTLib.Message;
import tw.com.gopartyon.mqtt.MessageData;
import tw.com.gopartyon.mqtt.MessageMongoDB;





public class Main {
	
	static SocketClient client;
	static SocketClient senderClient;
	static String brokerHostIp = "localhost";
	static int brokerPort = 1883;
	static String url = "localhost:27017";
	static String messageDB = "gopartyon_message";
	//static int reSendTime = 10000;
	static int awakeTime = 20000;
	static MessageMongoDB messageMDB = new MessageMongoDB();
	
	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
		
		if(args == null || args.length != 6)
		{
			System.out.println("Sample : java -jar MQTTServer.jar localhost 1883 localhost 27017 gopartyon_message 20000");
			return;
		} else {
			brokerHostIp = args[0];
			brokerPort = Integer.parseInt(args[1]);
			url = args[2] + ":" + args[3];
			messageDB = args[4];
			awakeTime = Integer.parseInt(args[5]);
			
			System.out.println("[Parameter]");
			System.out.println("Broker Host Ip : " + brokerHostIp);
			System.out.println("Broker Port : " + brokerPort);
			System.out.println("MongoDB Url : " + url);
			System.out.println("MessageDB Name : " + messageDB);
			System.out.println("Awake Time : " + awakeTime);
		}
		
		System.out.println("Start MQTTServer Version 0.1");
		messageMDB.start(url, messageDB);
		
		client = new SocketClient("FUNCUBE_MQTT_SERVER");
		client.connect(brokerHostIp, brokerPort, messageMDB);
		client.subscribe("$sys/");
		
		System.out.println("::.. MQTT Server ..::");
		System.out.println("===Waiting for Connection===");
		
		
		
		senderClient = new SocketClient("FUNCUBE_MQTT_SENDER");
		senderClient.connect(brokerHostIp, brokerPort, messageMDB);
		System.out.println("::.. MQTT Sender ..::");
		System.out.println("===Waiting for Connection===");
		
		
		
		//MqttSend mqttSend = new MqttSend();
		//mqttSend.start();
		
		while(true){
			System.out.println("I am awake!");
			
			client.publish("$hello/awake","FUNCUBE_MQTT_SERVER awake!");
			senderClient.publish("$hello/awake","FUNCUBE_MQTT_SENDER awake!");
			
			try{
				
				System.out.println(" Send Loop ...");
				
				// Get 閮
		 		List<MessageData> messageDateList = messageMDB.findAll(); 

				// �潮�閮
				for(MessageData currData : messageDateList)
				{
					try {
						
						
						senderClient.publish(currData.getTarget(), "0:" + "give:" + currData.getSerial() + ":" + currData.getMessage());

						System.out.println("Send : " + currData.toString());
						
					} catch(Exception ex)
					{
						System.out.println("Send Exception : " + ex.getMessage());
					}
					
					try{Thread.sleep((long) 0.001);}catch(Exception e){}
				}
			}
			catch(Exception e)
			{
				System.out.println("Awake Exception : " + e.getMessage());
			}
			
			Thread.sleep(awakeTime);
		}
	}
	
	public static void Send(String broker_ip_addr, int broker_port) throws IOException, InterruptedException{

		
	}
}
