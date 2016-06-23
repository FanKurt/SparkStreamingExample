package com.imac.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class MicroLinearRegression {

	public static void main(String[] args) {
		if (args.length < 2) {
		      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
		          "  <brokers> is a list of one or more Kafka brokers\n" +
		          "  <topics> is a list of one or more kafka topics to consume from\n\n");
		      System.exit(1);
		    }

		    String brokers = args[0];
		    String topics = args[1];

		    // Create context with a 2 seconds batch interval
		    SparkConf conf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

		    HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		    HashMap<String, String> kafkaParams = new HashMap<>();
		    kafkaParams.put("metadata.broker.list", brokers);
		    
		    

		    // Create direct kafka stream with brokers and topics
		    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
		        ssc,
		        String.class,
		        String.class,
		        StringDecoder.class,
		        StringDecoder.class,
		        kafkaParams,
		        topicsSet
		    );

		    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
		    

		    JavaDStream<LabeledPoint> trainingData = lines.map(new Function<String, LabeledPoint>() {
				public LabeledPoint call(String line) throws Exception {
					String[] parts = line.split(",");
					String[] token = parts[1].split(" ");
					Double humidity = Double.parseDouble(token[2].trim());
					
					String [] features =parts[2].split(" ");
					double[] v = new double[2];
					v[0] =Double.parseDouble(features[2].trim().split(":")[1]) ;
					v[1] = Double.parseDouble(features[1].trim().split(":")[1]);
					
					System.out.println(token[1]+"		"+v[0]+" , "+v[1]);
					
//					for(int i=0 ;i<features.length ;i++ ){
//						if(i==0){
//							v[i] =humidity ;
//						}else{
//							v[i] = Double.parseDouble(features[i].trim().split(":")[1]);
//						}
//					}
					return new LabeledPoint(Double.parseDouble(token[1].trim()), Vectors.dense(v));
				}
			}).cache();
		    
		    
		    int numFeatures = 2;
		    StreamingLinearRegressionWithSGD model = new StreamingLinearRegressionWithSGD()
		      .setInitialWeights(Vectors.zeros(numFeatures));

		   
		   model.trainOn(trainingData);
		   
		    
		   JavaPairDStream<Double, Double> aa = model.predictOnValues(trainingData.mapToPair(new PairFunction<LabeledPoint, Double, Vector>() {
				public Tuple2<Double, Vector> call(LabeledPoint arg0)
						throws Exception {
					return new Tuple2<Double, Vector>(arg0.label(), arg0.features());
				}
			}));
		   
			aa.print();
			
			aa.foreach(new Function<JavaPairRDD<Double,Double>, Void>() {
				public Void call(JavaPairRDD<Double, Double> arg0) throws Exception {
					if(!arg0.isEmpty()){
						Double MSE = new JavaDoubleRDD(arg0.map(new Function<Tuple2<Double,Double>, Object>() {
							public Object call(Tuple2<Double, Double> pair)
									throws Exception {
								return Math.pow(pair._1() - pair._2(), 2.0);
							}
						}).rdd()).mean();
						
						System.out.println("training Mean Squared Error = " + MSE);
					}
					
					return null;
				}
			});
		    
		    ssc.start();
		    ssc.awaitTermination();
	}
}
