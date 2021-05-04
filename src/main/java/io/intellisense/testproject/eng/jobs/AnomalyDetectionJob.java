package io.intellisense.testproject.eng.jobs;

import io.intellisense.testproject.eng.function.CalculateIqrWithinWindow;
import io.intellisense.testproject.eng.model.SensorsDataPoint;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSinkv2;
import lombok.extern.slf4j.Slf4j;
import io.intellisense.testproject.eng.datasource.CsvDatasource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Slf4j
public class AnomalyDetectionJob {

    public static final String CONFIG_FILE = "configFile";
    public static final String SENSOR_DATA = "sensorData";
    public static final String FLINK_PARALLELISM = "flinkParallelism";
    public static final int FLINK_PARALLELISM_DEFAULT_VALUE = 1;
    public static final String SENSOR_DATA_FILE_DEFAULT_VALUE = "sensor-data.csv";

    public static final String TUMBLING_WINDOW_BY_COUNT_VALUE = "tumbling.window.count";

    public static final String DATASOURCE_OPERATOR = "datasource-operator";
    public static final String ANOMALY_DETECTION_JOB_NAME = "Anomaly Detection Job";
    public static final String SINK_OPERATOR = "sink-operator";

    public static void main(String[] args) throws Exception {
        AnomalyDetectionJob anomalyDetectionJob = new AnomalyDetectionJob();
        anomalyDetectionJob.intellisenseCustomJob(args);
    }
    private static long timestampExtract(Row event) {
        final Optional<Object> timestampField = Optional.ofNullable(event.getField(0));
        return timestampField.map(o -> Instant.parse((String) o).toEpochMilli()).orElseGet(() -> Instant.now().toEpochMilli());
    }

    private void intellisenseCustomJob(String[] args) throws Exception {
        // Pass configFile location as a program argument:
        // --configFile config.local.yaml
        final ParameterTool programArgs = ParameterTool.fromArgs(args);
        final String configFile = programArgs.getRequired(CONFIG_FILE);
        final InputStream resourceStream = AnomalyDetectionJob.class.getClassLoader().getResourceAsStream(configFile);
        final ParameterTool configProperties = ParameterTool.fromPropertiesFile(resourceStream);

        // Stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(configProperties.getInt(FLINK_PARALLELISM, FLINK_PARALLELISM_DEFAULT_VALUE));
        env.getConfig().setGlobalJobParameters(configProperties);
        // Simple CSV-table datasource
        final String dataset = programArgs.get(SENSOR_DATA, SENSOR_DATA_FILE_DEFAULT_VALUE);
        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource();
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name(DATASOURCE_OPERATOR);
        System.out.println("Going to print DataStream - sourceStream:");
        //sourceStream.print();

        final SinkFunction<List<SensorsDataPoint>> influxDBSink = new InfluxDBSinkv2<>(configProperties);
        sourceStream.countWindowAll(Integer.parseInt(configProperties.getRequired(TUMBLING_WINDOW_BY_COUNT_VALUE)))
                .process(new CalculateIqrWithinWindow()).addSink(influxDBSink).name(SINK_OPERATOR);

        final JobExecutionResult jobResult = env.execute(ANOMALY_DETECTION_JOB_NAME);
        System.out.println("Execution got completed!!!");
        System.out.println(jobResult.toString());
    }
}