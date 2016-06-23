package com.imac;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public final class JavaFlumeEventCount {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: JavaFlumeEventCount <host> <port>");
			System.exit(1);
		}

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		Duration batchInterval = new Duration(10000);
		SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount");
		sparkConf.set("spark.serializer", KryoSerializer.class.getName());
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", "10.26.1.9:9200");
		sparkConf.set("es.resource", "elevator/records");
		sparkConf.set("es.input.json", "true");

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				batchInterval);
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils
				.createStream(ssc, host, port);

		// flumeStream.dstream()
		// flumeStream.count();

//		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		// flumeStream.map(new Function<Long, String>() {
		// @Override
		// public String call(Long in) {
		// return "Received " + in + " flume events.";
		// }
		// }).print();
		JavaDStream<String> flume_data = flumeStream
				.map(new Function<SparkFlumeEvent, String>() {
					public String call(SparkFlumeEvent arg0) throws Exception {
						ByteBuffer bytePayload = arg0.event().getBody();
						return new String(bytePayload.array());
					}
				});
//		flume_data.print();
//		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		flume_data.foreach(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> arg0) throws Exception {
				if(!arg0.isEmpty()){
					JavaRDD<String> update_time =arg0.map(new Function<String, String>() {
						public String call(String arg0) throws Exception {
							String str = arg0.substring(0,arg0.lastIndexOf(",")+1)+"\"createTime\":\""+getNowTime()+"\"}";
							return str;
						}
					});
					update_time.foreach(new VoidFunction<String>() {
						public void call(String arg0) throws Exception {
							System.out.println("Data : "+arg0);
							
						}
					});
					JavaEsSpark.saveToEs(update_time, "elevator/records");
				}
				return null;
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

	private static String getNowTime() {
		Date now = new Date();
		String[] token = now.toString().split(" ");
		String time = token[token.length - 1] + "-" + tranferMonth(token[1])
				+ "-" + token[2] + "T" + token[3] + "Z";
		return time;
	}

	private static String tranferMonth(String month) {
		String[] arrStrings = { "Jan", "Feb", "Mar", "Apr", "Mar", "Jun",
				"Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
		for (int i = 0; i < arrStrings.length; i++) {
			if (month.equals(arrStrings[i])) {
				return (i < 9) ? "0" + (i + 1) : "" + (i + 1);
			}
		}
		return "";
	}
}