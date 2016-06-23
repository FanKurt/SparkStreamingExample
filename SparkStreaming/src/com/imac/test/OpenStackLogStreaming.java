package com.imac.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.GraphLoader;
import org.apache.spark.mllib.clustering.StreamingKMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.json.JSONException;
import org.json.JSONObject;

import scala.Tuple2;

public class OpenStackLogStreaming {
	
	public static final Pattern nova_compute_log = Pattern
			.compile("([\\d*-]*) ([\\d*:]*.\\d*) (\\d*) (\\w*) ([\\w*\\.]*) \\[([\\w*-]*).*\\] (.*)(\\s*)");
//	private static SimpleDateFormat dateFormatUtc = new SimpleDateFormat("EE MMM dd HH:mm:ss zzz yyyy", Locale.US);
	private static final int FEATURE_COUNT = 9;
	private static DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
	public static void main(String[] args) {

//		if (args.length != 3) {
//			System.err.println("Usage: JavaFlumeEventCount <host> <port> <durations_min>");
//			System.exit(1);
//		}
//		final String host = args[0];
//		final int port = Integer.parseInt(args[1]);
		String directory = args[0];
		int sec = Integer.parseInt(args[2]);
		final SparkConf conf = new SparkConf();
		conf.setAppName("OpenStackStreamingExample");
		conf.set("spark.mesos.coarse", "true");
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf,Durations.seconds(sec));
//		JavaDStream<String> inputStream = ssc.socketTextStream(host, port);
		JavaDStream<String> inputStream = ssc.textFileStream(directory);
//		JavaDStream<String> inputStream = ssc.textFileStream(directory);
		
		
//		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, host, port);
//		
//		// 擷取 Flume 中的資料
//		JavaDStream<String> inputData = flumeStream.map(new Function<SparkFlumeEvent, String>() {
//			public String call(SparkFlumeEvent arg0) throws Exception {
//				return new String(arg0.event().getBody().array());
//			}
//		}).filter(new Function<String, Boolean>() {
//			public Boolean call(String arg0) throws Exception {
//				JSONObject jsonObject = new JSONObject(arg0);
//				return jsonObject.get("type").toString().equals("ceph")
//						&& !arg0.contains("tags");
//			}
//		});
//		
//		
		JavaDStream<Vector> trainData = inputStream.map(new Function<String, Vector>() {
			public Vector call(String arg0) throws Exception {
				try{
					JSONObject jsonObject = new JSONObject(arg0);
					double[] doubleArray = getDoubleArray(jsonObject);
					return Vectors.dense(doubleArray);
				}catch(Exception e){
					System.out.println("testData Error : "+e);
					return null ;
				}
			}
		}).filter(new Function<Vector, Boolean>() {
			public Boolean call(Vector arg0) throws Exception {
				return arg0!=null;
			}
		});
		
		JavaPairDStream<String, Vector> testData = inputStream.mapToPair(new PairFunction<String, String, Vector>() {
			public Tuple2<String, Vector> call(String arg0) throws Exception {
				try{
					JSONObject jsonObject = new JSONObject(arg0);
					double[] doubleArray = getDoubleArray(jsonObject);
					return new Tuple2<String, Vector>(Arrays.toString(doubleArray), Vectors.dense(doubleArray));
				}catch(Exception e){
					System.out.println("testData Error : "+e);
					return null ;
				}
			}
		}).filter(new Function<Tuple2<String,Vector>, Boolean>() {
			public Boolean call(Tuple2<String,Vector> arg0) throws Exception {
				return arg0!=null;
			}
		});
		
		StreamingKMeans model = new StreamingKMeans();
		
		model.setK(3);
		model.setDecayFactor(1.0);
		model.setRandomCenters(FEATURE_COUNT, 0.0, 0L);
		model.trainOn(trainData);

		JavaPairDStream<String, Integer> result = model.predictOnValues(testData);
		result.print();
		
		
		ssc.start();
		ssc.awaitTermination();

		ssc.stop();
	}
	
	/**
	 * 取得數值特徵
	 * @throws ParseException 
	 */
	private static double[] getDoubleArray(JSONObject jsonObject)
			throws NumberFormatException, JSONException, ParseException {
			double[] mDouble = new double[FEATURE_COUNT];
		mDouble[0] = Double.parseDouble(jsonObject.get("pgmap").toString());
		mDouble[1] = Double.parseDouble(jsonObject.get("pgs").toString());
		mDouble[2] = Double.parseDouble(jsonObject.get("data").toString());
		mDouble[3] = Double.parseDouble(jsonObject.get("used").toString());
		mDouble[4] = Double.parseDouble(jsonObject.get("unused").toString());
		mDouble[5] = Double.parseDouble(jsonObject.get("total").toString());
		mDouble[6] = Double.parseDouble(jsonObject.get("rd").toString());
		mDouble[7] = Double.parseDouble(jsonObject.get("wr").toString());
		mDouble[8] = Double.parseDouble(jsonObject.get("ops").toString());
//		mDouble[9] = Double.parseDouble(getSecond(jsonObject.get("time").toString()));
		return mDouble;
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return 分鐘
	 * @throws ParseException
	 */
	private static String getMinute(String dataDate) throws ParseException{
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.MINUTE)+"";
	}
	
	/**
	 * 
	 * @param dataDate
	 * @return 秒鐘
	 * @throws ParseException
	 */
	private static String getSecond(String dataDate) throws ParseException{
		Calendar calendar =  Calendar.getInstance();
		calendar.setTime(dateFormat.parse(dataDate));
		return calendar.get(Calendar.SECOND)+"";
	}
	
}
