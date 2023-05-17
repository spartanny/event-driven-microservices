package com.microservices.demo.twitter.to.kafka.service.exception;

public class TwitterToKafkaServiceExceptions extends RuntimeException{

    public TwitterToKafkaServiceExceptions(){
        super();
    }

    public TwitterToKafkaServiceExceptions(String message){
        super(message);
    }

    public TwitterToKafkaServiceExceptions(String message, Throwable cause){
        super(message, cause);
    }
}
