package io.intellisense.testproject.eng.sink.influxdb;

import java.time.Instant;
import java.util.List;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import io.intellisense.testproject.eng.model.SensorsDataPoint;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@RequiredArgsConstructor
public class InfluxDBSinkv2<T extends List<SensorsDataPoint>> extends RichSinkFunction<T> {

    private InfluxDBClient influxDBClient;

    final ParameterTool configProperties;

    // cluster metrics
    final Accumulator windowsIn = new IntCounter(0);
    final Accumulator windowsOut = new IntCounter(0);

    public static void main(final String[] args) {

        char[] token = "y8zPNnTzC29Y_OO8kp2ovz2BM0snVe36bHgvRW2tIRQr-KDIGTKml844OGhfY3-gMkludpiX7xi6VsyiU9xjyw==".toCharArray();
        String org = "intellisense";
        String bucket = "anomaly-detection";

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);

        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            SensorsDataPoint sensorsDataPoint = new SensorsDataPoint(
                    "Sensor-1", 215.3621546536,
                    1.0, Instant.now());
            System.out.println(sensorsDataPoint.sensor);
            System.out.println(sensorsDataPoint.value);
            System.out.println(sensorsDataPoint.anomalyScore);
            System.out.println(sensorsDataPoint.time);
            writeApi.writeMeasurement(WritePrecision.NS, sensorsDataPoint);
        }
        influxDBClient.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsIn", windowsIn);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsOut", windowsOut);
       /* influxDB = InfluxDBFactory.connect(
                configProperties.getRequired("influxdb.url"),
                configProperties.getRequired("influxdb.username"),
                configProperties.getRequired("influxdb.password"));*/
        char[] token = "y8zPNnTzC29Y_OO8kp2ovz2BM0snVe36bHgvRW2tIRQr-KDIGTKml844OGhfY3-gMkludpiX7xi6VsyiU9xjyw==".toCharArray();
        String org = "intellisense";
        String bucket = "anomaly-detection";
        this.influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucket);
    }


    @Override
    public void invoke(T sensorsDataPointList, Context context) throws Exception {
        windowsIn.add(1);
        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            for(SensorsDataPoint sensorsDataPoint : sensorsDataPointList) {
                System.out.println(sensorsDataPoint.sensor);
                System.out.println(sensorsDataPoint.value);
                System.out.println(sensorsDataPoint.anomalyScore);
                System.out.println(sensorsDataPoint.time);
                writeApi.writeMeasurement(WritePrecision.NS, sensorsDataPoint);
            }
            windowsOut.add(1);
            System.out.println("Added into Sink .. InfluxDb");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if(influxDBClient != null) {
           // influxDBClient.close();
        }
    }

}
