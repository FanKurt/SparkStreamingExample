package com.imac;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class JavaMQTTStreamSuite {

	public static void main(String[] args) {
		String brokerUrl = args[0];
		String topic = args[1];
		Duration batchInterval = new Duration(1000);
		SparkConf sparkConf = new SparkConf().setAppName("JavaMQTTStreamSuite");
		sparkConf.set("spark.driver.allowMultipleContexts", "true");
		
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				batchInterval);
		JavaReceiverInputDStream<String> inputDStream = MQTTUtils.createStream(ssc,
				brokerUrl, topic, StorageLevels.MEMORY_AND_DISK_SER);

		inputDStream.print();
		ssc.start();
		ssc.awaitTermination();
	}

}
