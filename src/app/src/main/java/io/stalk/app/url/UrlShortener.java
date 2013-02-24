package io.stalk.app.url;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.deploy.Verticle;

public class UrlShortener extends Verticle  {

	private EventBus eb;
	
	@Override
	public void start() throws Exception {

		eb = vertx.eventBus();
		
		JsonObject config = new JsonObject();
		config.putString("host", "localhost");
		config.putNumber("port", 27017);
		
		container.deployModule("websol.mod-urlPersister-v1.2", config);
		
		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {
			public void handle(final HttpServerRequest req) {
				
				if(req.path.startsWith("/url/")){
					String url = req.uri.substring(5);
					
					JsonObject data = new JsonObject();
					data.putString("action", "create");
					data.putString("url", url);
					
					eb.send("mod-urlPersister", data, new Handler<Message<JsonObject>>() {

						@Override
						public void handle(Message<JsonObject> message) {

							req.response.headers().put("Content-Type", "application/json; charset=UTF-8");
							req.response.end(message.body.encode());
							
						}
						
					});
					

				}else{
					
					String key = req.path.substring(1);

					JsonObject data = new JsonObject();
					data.putString("action", "get");
					data.putString("key", key);
					
					eb.send("mod-urlPersister", data, new Handler<Message<JsonObject>>() {

						@Override
						public void handle(Message<JsonObject> message) {
							
							if("error".equals(message.body.getString("status"))){
								req.response.statusCode = 404;
								req.response.end();
							}else{
								req.response.statusCode = 301;
								req.response.headers().put("Location", message.body.getString("url"));
								req.response.end();
							}
							
						}
						
					});

					
				}

			}
			
		}).listen(8080);

	}


}
