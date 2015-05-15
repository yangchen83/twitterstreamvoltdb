/**
 * Gets streaming data from tiwtter
 * Parse the information
 * Insert instances into VoltDB
 */


package e63final;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.*;
import org.scribe.builder.*;
import org.scribe.builder.api.*;
import org.scribe.model.*;
import org.scribe.oauth.*;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class TwitterConsumer extends Thread {

    private static final String STREAM_URI = "https://stream.twitter.com/1.1/statuses/filter.json";
    // add your own keys and tokens from twitter
    private static final String API_KEY = "";
    private static final String API_SECRET = "";
    private static final String ACCESS_TOKEN = "";
    private static final String ACCESS_TOKEN_SECRET = "";
    

    public void run() {
        try {
            // create connection to database
            org.voltdb.client.Client myApp;
            myApp = ClientFactory.createClient();
            myApp.createConnection("localhost");
 
            // get stream from twitter
            System.out.println("Starting Twitter public stream consumer thread.");
            // Enter your consumer key and secret below
            OAuthService service = new ServiceBuilder()
                    .provider(TwitterApi.class)
                    .apiKey(API_KEY)
                    .apiSecret(API_SECRET)
                    .build();

            // Set your access token
            Token accessToken = new Token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET);

            // Let's generate the request
            System.out.println("Connecting to Twitter Public Stream");
            OAuthRequest request = new OAuthRequest(Verb.POST, STREAM_URI);
            request.addHeader("version", "HTTP/1.1");
            request.addHeader("host", "stream.twitter.com");
            request.setConnectionKeepAlive(true);
            request.addHeader("user-agent", "Twitter Stream Reader");
            request.addBodyParameter("track", "i");
            service.signRequest(accessToken, request);
            Response response = request.send();

            // Create a reader to read Twitter's stream
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

            // initiate fields need to get from the stream
            String line = null, tweetId = null, userId = null, userName = null, lang = null, hashtag = null;
            JSONArray hashtags = null;

            while ((line = reader.readLine()) != null) {
                JSONObject obj = new JSONObject(line);

                try {
                    tweetId = obj.getString("id_str");
                    userId = obj.getJSONObject("user").getString("id_str");
                    userName = obj.getJSONObject("user").getString("screen_name");
                    hashtags = obj.getJSONObject("entities").getJSONArray("hashtags");
                    lang = obj.getString("lang");
                } catch (Exception e) {
                    // if any required fields are missing, ignore the tweet 
                }
                
                // store only tweets that are in English
                if (lang.equals("en")) {
                    myApp.callProcedure("tweetinsert", tweetId, userId, userName);   
                    if (hashtags.length() != 0) {
                        for (int i = 0; i < hashtags.length(); i++) {
                            hashtag = hashtags.getJSONObject(i).getString("text");
                            myApp.callProcedure("hashtaginsert", tweetId, hashtag);
                        }
                    }
                }
            }
        } catch (IOException | ProcCallException ioe) {
            ioe.printStackTrace();
        }
    }
}
