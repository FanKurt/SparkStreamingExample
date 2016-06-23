package com.imac;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;

public final class JavaFileStream {
	public static final Pattern log = Pattern
			.compile("((\\d*-)*\\d*) ((\\d*:)*\\d*)(,\\d*) ([A-Z]*) (([a-z]*.)*)");

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Format Error : [File Path]");
		}
		final String filePath = args[0];
		final String checkpointDirectory = "/flume_recover";
		// final String outputPath = "/flume_recover";

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			public JavaStreamingContext create() {
				return createContext(filePath, checkpointDirectory);
			}
		};
		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
				checkpointDirectory, factory);
		
		jssc.start();
		jssc.awaitTermination();
	}

	private static JavaStreamingContext createContext(String input,
			String checkpointDirectory) {
		System.out.println("Creating new context");
		// final File outputFile = new File("/flume_recover");
		// if (outputFile.exists()) {
		// outputFile.delete();
		// }

		SparkConf conf = new SparkConf().setMaster("local[2]")
				.setAppName("Stream File")
				.set("spark.driver.allowMultipleContexts", "true");
		conf.set("spark.serializer", KryoSerializer.class.getName());
		conf.set("es.index.auto.create", "true");
		conf.set("es.nodes", "10.26.1.134:9200");
		conf.set("es.resource", "flume/test");
		conf.set("es.input.json", "true");

		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				new Duration(3000));
		jssc.checkpoint(checkpointDirectory);
		

		JavaDStream<String> textFile = jssc.textFileStream(input);
		JavaDStream<String> jsonStr = textFile
				.map(new Function<String, String>() {
					public String call(String arg0) throws Exception {
						Matcher m = log.matcher(arg0);
						if (m.find()) {
							return transferJson(m);
						}
						return "";
					}
				});
		jsonStr.print();
		
		
		jsonStr.foreach(new Function<JavaRDD<String>, Void>() {
			public Void call(JavaRDD<String> arg0) throws Exception {
				if (!arg0.isEmpty() && arg0 != null) {
					JavaEsSpark.saveToEs(arg0, "flume/test");
				}
				return null;
			}
		});

		return jssc;
	}

	private static String transferJson(Matcher m) {
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("message", m.group(0));
		map.put("date", m.group(1));
		map.put("time", m.group(3));
		map.put("logtype", m.group(6));
		String str = m.group(7);
		map.put("source", str.substring(0, str.indexOf(":")));
		JSONObject json = new JSONObject();
		json.putAll(map);
		return json.toString();
	}

	private static String getTime() {
		Date date = new Date();
		String[] token = date.toString().split(" ");
		String current_Time = token[token.length - 1] + "-"
				+ tranferMonth(token[1]) + "-" + token[2] + "T" + token[3]
				+ "Z";
		return current_Time;
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