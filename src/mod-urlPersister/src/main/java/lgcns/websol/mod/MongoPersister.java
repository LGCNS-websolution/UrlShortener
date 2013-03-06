package lgcns.websol.mod;

import java.net.UnknownHostException;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoPersister extends BusModBase implements Handler<Message<JsonObject>>{

	private Mongo mongo;
	private DB db;
	
	@Override
	public void start() {
		super.start();
		
		String host = getOptionalStringConfig("host", "XXXX");
		
		try {
			mongo = new Mongo("localhost", 27017);
			db = mongo.getDB("urlshortener");
		} catch (UnknownHostException e) {
			
			logger.error("ERROR : "+e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		eb.registerHandler("mod-mongoPersister", this);
	}
	
	@Override
	public void handle(Message<JsonObject> message) {
		
		String action = message.body.getString("action");
		
		switch (action) {
		case "create":
			createShortUrl(message);
			break;

		case "get":
			getShortUrl(message);
			break;

		default:
			break;
		}
		
		
	}

	private void createShortUrl(Message<JsonObject> message) {
		
		DBCollection seq = db.getCollection("seq");
		
		DBObject query = new BasicDBObject();
		query.put("_id", "urlShortener");
		
		DBObject change = new BasicDBObject("seq", 1);
		DBObject update = new BasicDBObject("$inc", change);
		
		DBObject res = seq.findAndModify(
				query,  new BasicDBObject(), new BasicDBObject(), false, update, true, true);
		
		int num = Integer.parseInt(res.get("seq").toString());
		
		String key = BijectiveUtils.encode(num);

		DBCollection urls = db.getCollection("urls");
		
		DBObject urlData = new BasicDBObject("_id", key);
		urlData.put("url", message.body.getString("url"));
		
		urls.insert(urlData);
		
		JsonObject reply = new JsonObject();
		reply.putString("key", key);
		reply.putString("url", message.body.getString("url"));
		
		sendOK(message, reply);
		
	}

	private void getShortUrl(Message<JsonObject> message) {
		
		DBCollection urls = db.getCollection("urls");
		
		DBObject query = new BasicDBObject("_id", message.body.getString("key"));
		
		DBObject res = urls.findOne(query);
		
		if(res != null){
			sendOK(message, new JsonObject(res.toString()));
		}else{
			sendError(message, "Not existed!!!");
		}
		
	}
	
}
