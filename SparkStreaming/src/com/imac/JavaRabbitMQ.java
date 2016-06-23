package com.imac;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.stratio.receiver.RabbitMQUtils;

public class JavaRabbitMQ {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JavaRabbitMQ");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(1));
		
		Map<String,String> params = new HashMap<String, String>();
		params.put("host", "10.26.1.113");
		params.put("queueName", "stat");
		params.put("username", "docker");
		params.put("password", "docker");
		
		
		JavaReceiverInputDStream<String> receiverStream = RabbitMQUtils.createJavaStream(jssc, params);
		receiverStream.print();
		
		jssc.start(); 
		jssc.awaitTermination(); 
	}

}
