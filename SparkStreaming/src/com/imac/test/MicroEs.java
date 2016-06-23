package com.imac.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;

import scala.Tuple2;

public class MicroEs {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err
					.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 2 seconds batch interval
		SparkConf conf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.9:9200");
		conf.set("es.resource", "micro/status");
		conf.set("es.input.json", "true");
		JavaStreamingContext ssc = new JavaStreamingContext(conf,
				Durations.seconds(1));

		HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics
				.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topicsSet);

		JavaDStream<String> lines = messages
				.map(new Function<Tuple2<String, String>, String>() {
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});
		
		
		JavaDStream<String> esRDD = lines.map(new Function<String, String>() {
			public String call(String line) throws Exception {
				String[] parts = line.split(",");
				String[] token = parts[1].split(" ");
				Double humidity = Double.parseDouble(token[2].trim());
				Double temp = Double.parseDouble(token[1].trim());
				String[] features = parts[2].split(" ");
				Double cpu = Double.parseDouble(features[1].trim().split(":")[1]);
				Double memory = Double.parseDouble(features[2].trim().split(":")[1]);
				Double disk = Double.parseDouble(features[3].trim().split(":")[1]);
				JSONObject json = new JSONObject();
				json.put("humidity", humidity);
				json.put("temp", temp);
				json.put("cpu", cpu);
				json.put("memory", memory);
				json.put("disk", disk);
				String [] slice = parts[0].split(" ");
				String hour = slice[1].substring(0,slice[1].indexOf(":"));
				int hour_update;
				hour_update = (Integer.parseInt(hour)-8<0)?24+(Integer.parseInt(hour)-8):Integer.parseInt(hour)-8;
				String new_time = hour_update+":"+slice[1].substring(slice[1].indexOf(":")+1,slice[1].length());
				String day = slice[0].substring(slice[0].lastIndexOf("-")+1,slice[0].length());
				int day_update;
				day_update = ((Integer.parseInt(hour)-8<0)?(Integer.parseInt(day)-1):Integer.parseInt(day));
				String new_day =slice[0].substring(0,slice[0].lastIndexOf("-"))+"-"+day_update;
//				System.out.println(new_day);
				json.put("time",new_day+"T"+new_time+"Z");
				return json.toString();
			}
		});
		esRDD.print();

		esRDD.foreach(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> arg0) throws Exception {
				JavaEsSpark.saveToEs(arg0, "micro/status");
				return null;
			}
		});

		
		ssc.start();
		ssc.awaitTermination();
	}
}
