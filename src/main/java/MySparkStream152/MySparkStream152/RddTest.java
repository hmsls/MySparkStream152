package MySparkStream152.MySparkStream152;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class RddTest {
	public static void main(String[] args) {
		//初始化
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("RddTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//创建一个并行化的数据集RDD（创建RDD方式一）
		List<Integer> data = Arrays.asList(1,2,3,4,5);
		//后面的参数10是把数据集切成10片，即10个分区，每个分区将运行一个task
		JavaRDD<Integer> distData = sc.parallelize(data,10);
		
		//创建一个外部数据集RDD（创建RDD方式二）
		//第二个参数为分区数目，默认是一个块分为一个分区。
		JavaRDD<String> distFile = sc.textFile("data.txt",10);
		
		//RDD的transform和action
		JavaRDD<String> lines = sc.textFile("data.txt",10);
		JavaRDD<Integer> lineLength = lines.map(s -> s.length());
		Integer totalLength = lineLength.reduce((x,y)->x+y);
		//持久化结果
		lineLength.persist(StorageLevel.MEMORY_ONLY());
		System.out.println(totalLength);
		
	}
}
