import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.BSONObject;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.ServerAddress;

public class mongodb {

	static DB db;

	/**
	 * �蝙��Mongo DB
	 * @param url
	 * @param messageDB
	 * @throws Exception
	 */
	public void start(String url, String messageDB) throws Exception {

		ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
		for (String s : url.split(",")) {
			addr.add(new ServerAddress(s));
		}
		Mongo m = new Mongo(addr);

		db = m.getDB(messageDB);
	}

	/**
	 * ������芰��鞈�
	 * 
	 * @return
	 */
	public List<MessageData> findAll() {
		List<MessageData> result = new ArrayList<MessageData>();

		try {

			DBCollection coll = db.getCollection("messageData");

			BasicDBObject query = new BasicDBObject();

			DBCursor cur = coll.find(query);

			while (cur.hasNext()) {
				DBObject dbObj = cur.next();
				MessageData messageData = new MessageData();
				//messageData.setId(String.valueOf(dbObj.get("serial")));
				messageData.setSerial(Integer.parseInt( dbObj.get("serial").toString()));
				messageData.setTarget((String) dbObj.get("target"));
				messageData.setMessage((String) dbObj.get("message"));

				result.add(messageData);
			}

		} catch (Exception ex) {
			System.out.println("find_all Exception : " + ex.getMessage());
		}

		return result;
	}

	/**
	 * ���啁�瘚偌��
	 * 
	 * @return
	 */
	public int getNewSerial() {

		DBCollection coll = db.getCollection("messageData");

		DBObject orderBy = new BasicDBObject("serial", -1);

		DBCursor cur = coll.find().sort(orderBy).limit(1);

		int serial = 0;

		if (cur.hasNext()) {
			DBObject dbObj = cur.next();
			serial = Integer.parseInt((String)dbObj.get("serial"));
		}

		serial++;

		return serial;
	}

	/**
	 * �啣��潮�����
	 * 
	 * @param target
	 * @param message
	 * @param serial
	 */
	public void insert(String target, String message) {

		DBCollection coll = db.getCollection("messageData");
		BasicDBObject doc = new BasicDBObject();
		
		int newSerial = getNewSerial();
		
		//doc.put("id", String.valueOf(newSerial));
		doc.put("serial", newSerial);
		doc.put("target", target);
		doc.put("message", message);
		coll.insert(doc);

	}

	/**
	 * �芷Client�嗅����
	 * 
	 * @param serial
	 */
	public void delRecv(int serial) {

		DBCollection coll = db.getCollection("messageData");

		BasicDBObject query = new BasicDBObject();
		
		query.put("serial", serial);

		coll.remove(query);
		
		System.out.println("Deleting message with index [" + serial + "] ");
	}
}
