package MySparkStream152.MySparkStream152;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class Test1 {
	
	public static void main(String[] args) {
		// 创建一个本地的streamcontext，包含2个执行线程和间隔为1秒的批次
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWorldCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		//创建一个DStream，将会连接hostname:port，接受每一行为文本的流。
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("60.24.64.141", 9999);
		
		//把每一行按照空格切分，flatmap是一个DStream操作，通过从包含一条记录的源DStream产生
		//一个包含多条记录的DStream。
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
			public Iterable<String> call(String x) throws Exception {
				//把每一行按照空格切分，然后转换为列表
				return Arrays.asList(x.split(" "));
			}
		});
		
		//	数出每个批次中的单词
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) throws Exception {
				return new Tuple2<String, Integer>(x, 1);
			}
		});
		JavaPairDStream<String, Integer> wordsCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a1, Integer a2) throws Exception {
				return a1 + a2;
			}
		});
		
		//打印这个DStream流中产生的RDD中的前10个元素
		wordsCounts.print();
		
		//当开始计算的时候才进行计算
		jssc.start();
		jssc.awaitTermination();
	}
}
