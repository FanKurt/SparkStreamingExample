package com.imac;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * 
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <zkQuorum>
 * is a list of one or more zookeeper servers that make quorum <group> is the
 * name of kafka consumer group <topics> is a list of one or more kafka topics
 * to consume from <numThreads> is the number of threads the kafka consumer
 * should use
 * 
 * To run this example: `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \ zoo03
 * my-consumer-group topic1,topic2 1`
 */

public final class JavaKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	private JavaKafkaWordCount() {
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}

		StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,new Duration(2000));
		int numThreads = Integer.parseInt(args[3]);
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap);
//		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//					public String call(Tuple2<String, String> tuple2) {
//						return tuple2._2();
//					}
//				});
//
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//					public Iterable<String> call(String x) {
//						return Lists.newArrayList(SPACE.split(x));
//					}
//				});
//
//		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
//					public Tuple2<String, Integer> call(String s) {
//						return new Tuple2<String, Integer>(s, 1);
//					}
//				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
//			public Integer call(Integer i1, Integer i2) {
//				return i1 + i2;
//			}
//		});

		messages.print();
		jssc.start();
		jssc.awaitTermination();
	}
}