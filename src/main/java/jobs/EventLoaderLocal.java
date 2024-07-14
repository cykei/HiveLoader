package jobs;

import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class EventLoaderLocal {
    public static void main(String[] args) throws Exception {
        // 파일 경로 및 기타 변수 설정
        System.setProperty("hadoop.home.dir", "C:\\Users\\cykei\\hadoop2.7.3");
        String inputDir = "D:/dockerProjects/hadoop_base/hive/data";
        String outputPath = "D:/dockerProjects/hadoop_base/hive/data/output";
        String checkPointPath = "D:/dockerProjects/hadoop_base/hive/data/output/checkpoint";
        String tableName = "event";
        String zoneId = "Asia/Seoul";

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("eventLoader")
                .config("checkPointLocation", checkPointPath)
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setCheckpointDir(checkPointPath);

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
        File file = new File(inputDir);
        String[] fileNames = file.list((f, name) -> name.endsWith("csv"));

        File check = new File(checkPointPath);
        String[] checkedNames = check.list((f, name) -> name.endsWith("check"));

        if (fileNames == null) {
            throw new Exception("입력 데이터가 없습니다.");
        }

        List<String> targetFilenames = getDifference(fileNames, checkedNames);

        for (String filename : targetFilenames) {
            File checked = new File(checkPointPath, filename + ".check");
            if (!checked.createNewFile()) {
                throw new IOException("checkPointPath 가 존재하는지 확인해주세요.");
            }
            String inputPath = new File(inputDir, filename).getPath();
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
                    .mode("append")
                    .partitionBy("partition_key")
                    .format("parquet")
                    .save(outputPath + "/parquet");

        }

//        // Hive External Table 생성
//        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " ("
//                + "event_time STRING, "
//                + "event_type STRING, "
//                + "product_id INT, "
//                + "category_id STRING, "
//                + "category_code STRING, "
//                + "brand STRING, "
//                + "price DOUBLE, "
//                + "user_id INT, "
//                + "user_session STRING, "
//                + "event_time_kst TIMESTAMP "
//                + ") "
//                + "PARTITIONED BY (partition_key STRING) "
//                + "STORED AS PARQUET "
//                + "LOCATION '" + outputPath + "/parquet'");
//
//        // 테이블 데이터 복구
//        spark.sql("MSCK REPAIR TABLE " + tableName);

        spark.stop();
    }

    private static List<String> getDifference(String[] sources, String[] targets) {
        Set<String> all = new HashSet<>(Arrays.asList(sources));
        for (String filename : targets) {
            String filenameWithoutExtension = FilenameUtils.removeExtension(filename);
            all.remove(filenameWithoutExtension);
        }
        return new ArrayList<>(all);
    }
}
