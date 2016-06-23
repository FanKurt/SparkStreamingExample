package com.imac;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class JavaNetworkWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(1));

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(
				args[0], Integer.parseInt(args[1]));
		lines.countByValue();
		
		JavaReceiverInputDStream<String> lines1 = jssc.socketTextStream(
				args[0], Integer.parseInt(args[1]));
		
		lines.union(lines1);
		
		
//		JavaDStream<String> b = lines.reduceByWindow(new Function2() {
//			public Object call(Object arg0, Object arg1) throws Exception {
////				System.out.println("1  : "+arg0);
////				System.out.println("2  : "+arg1);
//				return arg0+","+arg1;
//			}
//		}, Durations.seconds(30), Durations.seconds(5));
		
		
//		JavaDStream<String> words = lines
//				.flatMap(new FlatMapFunction<String, String>() {
//					public Iterable<String> call(String x) {
//						return Arrays.asList(x.split(" "));
//					}
//				});
//		JavaPairDStream<String, Integer> pairs = words
//				.mapToPair(new PairFunction<String, String, Integer>() {
//					public Tuple2<String, Integer> call(String s) {
//						return new Tuple2<String, Integer>(s, 1);
//					}
//				});
//		JavaPairDStream<String, Integer> wordCounts = pairs
//				.reduceByKey(new Function2<Integer, Integer, Integer>() {
//					
//					public Integer call(Integer i1, Integer i2) {
//						return i1 + i2;
//					}
//				});
//		wordCounts.print();
//		wordCounts.saveAsNewAPIHadoopFiles("/user/hadoop/streaming/imac", "" ,String.class , Integer.class , (Class<? extends OutputFormat<String, Integer>>) TextOutputFormat.class);
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	

	}

	private void SS(String aa , int a){
		
	}
}