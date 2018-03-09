package scenario1;

import org.fusesource.mqtt.client.BlockingConnection;

import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.kaaproject.kaa.client.DesktopKaaPlatformContext;
import org.kaaproject.kaa.client.Kaa;
import org.kaaproject.kaa.client.KaaClient;
import org.kaaproject.kaa.client.SimpleKaaClientStateListener;
import org.kaaproject.kaa.client.configuration.base.ConfigurationListener;
import org.kaaproject.kaa.client.configuration.base.SimpleConfigurationStorage;
import org.kaaproject.kaa.client.logging.strategies.RecordCountLogUploadStrategy;
//import org.kaaproject.kaa.schema.sample.ConfigurationCTL;
import org.kaaproject.kaa.schema.sample.SensorsCollectionV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
//import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import org.json.*;

/**
 * Class implement functionality for First Kaa application. Application send temperature data
 * from the Kaa endpoint with required configured sampling period
 */
public class Scenario1 {
 
    private static final long DEFAULT_START_DELAY = 1000L;
 
    private static final Logger LOG = LoggerFactory.getLogger(Scenario1.class);
 
    private static KaaClient kaaClient;
 
    private static ScheduledFuture<?> scheduledFuture;
    private static ScheduledExecutorService scheduledExecutorService;
    private static final String BROKER_URL="tcp://127.0.0.1:1883";
    private static final String INPUT_MOVE="zone_2/box_1/motion/id_1";
    private static final String INPUT_SENSOR="zone_2/box_1/light/id_1";
    private static final String OUTPUT_ACTION="light/status";
    static String moveStatus="";
    
    public static boolean ActionMessage(String moveStatus,int light,int temperature,int humidity) throws Exception {
    		System.out.println("handle message");
    		MQTT mqtt = new MQTT();
    		JSONObject outcomeJSON = new JSONObject();
	    mqtt.setHost(BROKER_URL);
	    BlockingConnection connection = mqtt.blockingConnection();
	    connection.connect();
	    System.out.println(moveStatus);
	    if(moveStatus.equals("true")) {
			if(light > 1000) {
				outcomeJSON.append("greenLed", "ON");
	    			outcomeJSON.append("yellowLed", "OFF");
	    			outcomeJSON.append("redLed", "OFF");
			}else if(light >500) {
				outcomeJSON.append("greenLed", "OFF");
	    			outcomeJSON.append("yellowLed", "ON");
	    			outcomeJSON.append("redLed", "OFF");
			}else {
				outcomeJSON.append("greenLed", "OFF");
	    			outcomeJSON.append("yellowLed", "ON");
	    			outcomeJSON.append("redLed", "OFF");
			}
			
			final String outcomeMessage = outcomeJSON.toString();
			connection.publish(OUTPUT_ACTION,outcomeMessage.getBytes(), QoS.AT_LEAST_ONCE, false);
			return true;
		}
			return false;
    }
    
    public static int reciveMessage() throws Exception {
// 	   System.out.println("Connecting to Broker1 using MQTT");
 	      MQTT mqtt = new MQTT();
 	      mqtt.setHost(BROKER_URL);
 	      BlockingConnection connection1 = mqtt.blockingConnection();
 	      BlockingConnection connection2 = mqtt.blockingConnection();
 	      connection1.connect();
 	      connection2.connect();
// 	      System.out.println("Connected to Artemis");

 	      // Subscribe to  fidelityAds topic
 	      Topic[] topics = {new Topic(INPUT_MOVE, QoS.AT_LEAST_ONCE),new Topic(INPUT_SENSOR, QoS.AT_LEAST_ONCE)};
 	      connection1.subscribe(topics);
// 	      connection2.subscribe(topics[2]);

 	      // Get Ads Messages

 		    	  Message message = connection1.receive(5, TimeUnit.SECONDS);
 		    	  if(message!=null){
 		    		  JSONObject incomeMessage = new JSONObject(new String(message.getPayload()));
// 		    		  System.out.println(incomeMessage.getInt("temperature"));
// 		    		  String moveStatus=incomeMessage.getStrong("moveStatus");
 		    		  moveStatus=incomeMessage.getString("moveStatus");
		    		  int light = incomeMessage.getInt("light");
		    		  int temperature = incomeMessage.getInt("temperature");
		    		  int humidity = incomeMessage.getInt("humidity");
		    		  ActionMessage(moveStatus,light,temperature,humidity);
 		    		  return light;
 		    	  }
 				return 0;
 		      
 	      
 	   
    }    
    public static void main(String[] args) {
    	
//    		MongoClient mongoClient = new MongoClient( "192.168.1.142" , 27017 );
//    		MongoDatabase database = mongoClient.getDatabase("kaa");
//    		MongoCollection<Document> collection = database.getCollection("logs_09682702090093578744");
//    		for (String name : database.listCollectionNames()) {
////    		    System.out.println(name);
//    		}
//    		Document myDoc = collection.find().first();
//    		System.out.println(myDoc.toJson());

        LOG.info(Scenario1.class.getSimpleName() + " app starting!");
 
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
   
        //Create the Kaa desktop context for the application.
        DesktopKaaPlatformContext desktopKaaPlatformContext = new DesktopKaaPlatformContext();
 
        /*
         * Create a Kaa client and add a listener which displays the Kaa client
         * configuration as soon as the Kaa client is started.
         */
        kaaClient = Kaa.newClient(desktopKaaPlatformContext, new FirstKaaClientStateListener(), true);
 
        /*
         *  Used by log collector on each adding of the new log record in order to check whether to send logs to server.
         *  Start log upload when there is at least one record in storage.
         */
        RecordCountLogUploadStrategy strategy = new RecordCountLogUploadStrategy(1);
        strategy.setMaxParallelUploads(1);
        kaaClient.setLogUploadStrategy(strategy);
 
        /*
         * Persist configuration in a local storage to avoid downloading it each
         * time the Kaa client is started.
         */
        kaaClient.setConfigurationStorage(new SimpleConfigurationStorage(desktopKaaPlatformContext, "saved_config.cfg"));
 
//        kaaClient.addConfigurationListener(new ConfigurationListener() {
//            @Override
//            public void onConfigurationUpdate(Configuration configuration) {
//                LOG.info("Received configuration data. New sample period: {}", configuration.getSamplePeriod());
//                onChangedConfiguration(TimeUnit.SECONDS.toMillis(configuration.getSamplePeriod()));
//            }
//        });
  
        //Start the Kaa client and connect it to the Kaa server.
        kaaClient.start();
 
        LOG.info("--= Press any key to exit =--");
        try {
            System.in.read();
        } catch (IOException e) {
            LOG.error("IOException has occurred: {}", e.getMessage());
        }
        LOG.info("Stopping...");
        scheduledExecutorService.shutdown();
        kaaClient.stop();
    }
 
    /*
     * Method, that emulate getting temperature from real sensor.
     * Retrieves random temperature.
     */
//    private static int getTemperatureRand() throws Exception {
//    		System.out.print(mqttClient.getMessage());
//        return mqttClient.getMessage();
//    }
 
    private static void onKaaStarted(long time) {
        if (time <= 0) {
            LOG.error("Wrong time is used. Please, check your configuration!");
            kaaClient.stop();
            System.exit(0);
        }
 
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        int light;
						try {
							light = reciveMessage();
							if(light!=0) {
								kaaClient.addLogRecord(new SensorsCollectionV2(light,light+1,light,true));
								System.out.println("Temperature: "+light);
		                         LOG.info("Sampled Temperature: {}", light);
							}
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        
                    }
                }, 0, time, TimeUnit.MILLISECONDS);
    }
 
    private static void onChangedConfiguration(long time) {
        if (time == 0) {
            time = DEFAULT_START_DELAY;
        }
        scheduledFuture.cancel(false);
 
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        int light;
						try {
							light = reciveMessage();
							if(light!=0) {
								kaaClient.addLogRecord(new SensorsCollectionV2(light,light+1,light,true));
		                        	LOG.info("Sampled Temperature: {}", light);
							}
							
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                        
                    }
                }, 0, time, TimeUnit.MILLISECONDS);
    }
 
    private static class FirstKaaClientStateListener extends SimpleKaaClientStateListener {
 
        @Override
        public void onStarted() {
            super.onStarted();
            LOG.info("Kaa client started");
//            Configuration configuration = kaaClient.getConfiguration();
//            LOG.info("Default sample period: {}", configuration.getSamplePeriod());
//            onKaaStarted(TimeUnit.SECONDS.toMillis(configuration.getSamplePeriod()));
        }
 
        @Override
        public void onStopped() {
            super.onStopped();
            LOG.info("Kaa client stopped");
        }
    }
    
    
}
