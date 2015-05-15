/**
 * Make query on the voltdb which stores twitter stream data 
 **/

package e63final;

import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class TweetQuery {

    public static void main(String[] args) {
        
        try {
            // create connection with the database 
            org.voltdb.client.Client myApp;
            myApp = ClientFactory.createClient();
            myApp.createConnection("localhost");
            
            while (true) {
                System.out.println("=============================================");
                
                // get tweet count
                final ClientResponse tweetResponse = myApp.callProcedure("tweetcount");
                if (tweetResponse.getStatus() != ClientResponse.SUCCESS){
                    System.err.println(tweetResponse.getStatusString());
                    System.exit(-1);
                }

                final VoltTable tweetResults[] = tweetResponse.getResults();
                VoltTable tweetResultTable = tweetResults[0]; 
                System.out.println("Total tweet numbers:");
                System.out.println(tweetResultTable.fetchRow(0).getLong("c1"));
                System.out.println("");
                
                // get most used hashtags
                final ClientResponse response = myApp.callProcedure("hashtagcount");  
                if (response.getStatus() != ClientResponse.SUCCESS){
                    System.err.println(response.getStatusString());
                    System.exit(-1);
                }
                final VoltTable results[] = response.getResults();
                if (results.length == 0) {
                    System.out.println("No hashtag results available yet");
                    System.exit(-1);
                } 
                VoltTable resultTable = results[0]; 
                int rowCount = resultTable.getRowCount();
                System.out.println("Most Popular hashtags: ");
                for (int i =0; i< rowCount; i++){
                    System.out.println(resultTable.fetchRow(i).getString("hashtag"));
                } 
                System.out.println("");
                
                // get most tweeted user list
                final ClientResponse userResponse = myApp.callProcedure("mosttweetuser");
                if (userResponse.getStatus() != ClientResponse.SUCCESS){
                    System.err.println(userResponse.getStatusString());
                    System.exit(-1);
                }
                final VoltTable userResults[] = userResponse.getResults();
                if (results.length == 0) {
                    System.out.println("No user results available yet");
                    System.exit(-1);
                }
                VoltTable userResultTable = userResults[0]; 
                int userRowCount = userResultTable.getRowCount();
                System.out.println("Most tweeted users: ");
                for (int i =0; i< userRowCount; i++){
                    VoltTableRow row = userResultTable.fetchRow(i); 
                    System.out.println(row.getString("username") + " tweeted " + row.getLong("number") + " tweets");
                }
                System.out.println("=============================================");

                // wait 10 s to the the queries again
                Thread.sleep(10000);
            }            
        } catch (IOException | ProcCallException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
