package jobs;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class EventLoader {
    public static void main(String[] args) throws Exception {

        // 파일 경로 및 기타 변수 설정
        String inputDir = "hdfs://namenode:9000/user/hive/warehouse/data";
        String outputPath = "hdfs://namenode:9000/user/hive/warehouse/output";
        String checkPointPath = "hdfs://namenode:9000/user/hive/warehouse/output/checkpoint";
        String tableName = "event";
        String zoneId = "Asia/Seoul";

        SparkSession spark = SparkSession
                .builder()
                .appName("eventLoader")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setCheckpointDir(checkPointPath);

        Configuration hadoopConf = new Configuration();
        FileSystem fileSystem = FileSystem.get(hadoopConf);
        FileStatus[] inputFileStatuses = fileSystem.listStatus(new Path(inputDir));
        FileStatus[] checkFileStatuses = fileSystem.listStatus(new Path(checkPointPath));
        List<String> inputFileNames = getFileNames(inputFileStatuses);
        List<String> checkFileNames = getFileNames(checkFileStatuses);

        List<String> targetFilenames = getDifference(inputFileNames, checkFileNames);

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

        // 데이터 로드 및 처리
        for (String filename : targetFilenames) {
            Path checkFilePath = new Path(checkPointPath, filename + ".check");
            if (!fileSystem.createNewFile(checkFilePath)) {
                throw new IOException("checkDir 이 존재하는지 확인해주세요.");
            }

            String inputFilePath = String.join("/", inputDir, filename);
            System.out.println("log: " + inputFilePath);
            Dataset<Row> df = spark.read()
                    .schema(schema)
                    .option("header", "true")
                    .option("sep", ",")
                    .csv(inputFilePath);

            Dataset<Row> formattedDF = df
                    .withColumn("event_time_utc",
                            date_format(to_timestamp(regexp_replace(col("event_time"), " UTC$", "")), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("event_time_kst", from_utc_timestamp(col("event_time_utc"), zoneId))
                    .withColumn("partition_key", date_format(col("event_time_kst"), "yyyy-MM-dd"));

            // 데이터를 Parquet 포맷으로 저장
            formattedDF.write()
                    .mode(SaveMode.Append)
                    .partitionBy("partition_key")
                    .parquet(outputPath + "/parquet");

            List<Row> partitions = formattedDF.select("partition_key").distinct().collectAsList();
            for (Row row : partitions) {
                String partitionKey = row.getString(0);
                String partitionPath = outputPath + "/parquet/partition_key=" + partitionKey;
                String partitionQuery = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (partition_key='%s') LOCATION '%s'",
                        tableName, partitionKey, partitionPath);
                spark.sql(partitionQuery);
            }
        }

        spark.stop();
    }

    private static List<String> getFileNames(FileStatus[] inputFileStatuses) {
        List<String> fileNames = new ArrayList<>();
        for (FileStatus fileStatus : inputFileStatuses) {
            fileNames.add(fileStatus.getPath().getName());
        }
        return fileNames;
    }

    private static List<String> getDifference(List<String> sources, List<String> targets) {
        Set<String> all = new HashSet<>(sources);
        for (String filename : targets) {
            String filenameWithoutExtension = FilenameUtils.removeExtension(filename);
            all.remove(filenameWithoutExtension);
        }
        return new ArrayList<>(all);
    }
}
