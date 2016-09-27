package Kafka.Twitter;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;

public class SearchTweets {
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Requires atleast one argument [hashtag] to search ");
            System.exit(-1);
        }
    	
    	Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
    	
    	ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("URf08M8BLaDY6V8UaJgkfeJgm");
        cb.setOAuthConsumerSecret("aZumHYvaorZCqCMcnqcezE4mwBRF96B2G43xJVlZnTE2br6PPF");
        cb.setOAuthAccessToken("2606826589-JjXdu4wyDGjUmGIAFm7j7TDIUKje5TPu8Dp60qy");
        cb.setOAuthAccessTokenSecret("xhMm3Sq6XO2xhfgIMFad4WdNWApGk2ieLz6q9D72ONhY6");
    	
        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        try {
            Query query = new Query(args[0]).lang("en");
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    System.out.println("\n\n\n@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                    String user = tweet.getUser().getScreenName();
                    String content = tweet.getText();
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(args[0], user, content);
                    producer.send(data);
                }
            } while ((query = result.nextQuery()) != null);
            producer.close();
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }
}