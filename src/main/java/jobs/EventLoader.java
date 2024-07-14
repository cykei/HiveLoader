package jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class EventLoader {
    public static void main(String[] args) throws Exception {
        // 파일 경로 및 기타 변수 설정
        String inputPath = "hdfs://namenode:9000/user/hive/warehouse/data/*.csv";
        String outputPath = "hdfs://namenode:9000/user/hive/warehouse/output";
        String tableName = "event";
        String zoneId = "Asia/Seoul";

        SparkSession spark = SparkSession
                .builder()
                .appName("eventLoader")
                .enableHiveSupport()
                .getOrCreate();

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("event_time", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("event_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("product_id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("category_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("category_code", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("brand", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("user_id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("user_session", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

        // 데이터 로드 및 처리
        Dataset<Row> df = spark.read()
                .schema(schema)
                .option("header", "true")
                .option("sep", ",")
                .csv(inputPath);

        Dataset<Row> formattedDF = df
                .withColumn("event_time_utc",
                        date_format(to_timestamp(regexp_replace(col("event_time"), " UTC$", "")), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("event_time_kst", from_utc_timestamp(col("event_time_utc"), zoneId))
                .withColumn("partition_key", date_format(col("event_time_kst"), "yyyy-MM-dd"));

        // 데이터를 Parquet 포맷으로 저장
        formattedDF.write()
                .mode("overwrite")
                .partitionBy("partition_key")
                .format("parquet")
                .save(outputPath + "/parquet");

        // Hive External Table 생성
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " ("
                + "event_time STRING, "
                + "event_type STRING, "
                + "product_id INT, "
                + "category_id STRING, "
                + "category_code STRING, "
                + "brand STRING, "
                + "price DOUBLE, "
                + "user_id INT, "
                + "user_session STRING, "
                + "event_time_kst TIMESTAMP "
                + ") "
                + "PARTITIONED BY (partition_key STRING) "
                + "STORED AS PARQUET "
                + "LOCATION '" + outputPath + "/parquet'");

        // 테이블 데이터 복구
        spark.sql("MSCK REPAIR TABLE " + tableName);
        spark.stop();
    }
}
