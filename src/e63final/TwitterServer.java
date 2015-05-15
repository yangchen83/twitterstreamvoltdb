/**
 * Starts twitter consumer
 */

package e63final;

public class TwitterServer {

    public static void main(String[] args) {
        TwitterConsumer stream = new TwitterConsumer();
        stream.run();
    }

}
