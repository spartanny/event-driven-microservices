package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceExceptions;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-microservice.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[] {
            "irure",
            "eu",
            "consequat",
            "reprehenderit",
            "velit",
            "ex",
            "sint",
            "reprehenderit",
            "occaecat",
            "labore",
            "cupidatat",
            "ipsum",
            "eiusmod",
            "ex",
            "dolore"
    };
    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    private static final String tweetAsRawJson = "{\"created_at\": \"{0}\",\"id\": \"{1}\",\"text\": \"{2}\",\"user\": {\"id\": \"{3}\"}}";


    public MockKafkaStreamRunner(TwitterKafkaStatusListener statusListener, TwitterToKafkaServiceConfigData configData){
        this.twitterKafkaStatusListener = statusListener;
        this.twitterToKafkaServiceConfigData = configData;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();

        LOG.info("Starting mock filtering stream with keywords {}", Arrays.toString(Arrays.stream(keywords).toArray()));

        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs){
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while(true){
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                LOG.error("Error creating twitter status !", e);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try{
            Thread.sleep(sleepTimeMs);
        }catch (InterruptedException e){
            throw new TwitterToKafkaServiceExceptions("Error while sleeping, for waiting new status to create!");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };
        return formatTweetAsJsonWithParameters(params);
    }

    private String formatTweetAsJsonWithParameters(String[] params){
        String tweet = tweetAsRawJson;
        for (int i=0; i<4; i++){
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        Integer tweetLength =  RANDOM.nextInt(maxTweetLength - minTweetLength + 1)  + minTweetLength;
        StringBuilder randomTweet = new StringBuilder();
        for (int i = 0; i < tweetLength; i++) {
            randomTweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
        }
        return randomTweet.toString().trim();
    }
}
