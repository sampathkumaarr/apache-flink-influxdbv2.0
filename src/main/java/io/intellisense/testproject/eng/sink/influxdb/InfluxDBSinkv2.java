package io.intellisense.testproject.eng.sink.influxdb;

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

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsIn", windowsIn);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsOut", windowsOut);
        String influxdbUrl = "http://" + configProperties.getRequired("influxdb.url");
        char[] authenticationToken = configProperties.getRequired("influxdb.authenticationToken").toCharArray();
        String organisationName = configProperties.getRequired("influxdb.organisationName");
        String bucket = configProperties.getRequired("influxdb.bucket");
        this.influxDBClient = InfluxDBClientFactory.create(influxdbUrl, authenticationToken, organisationName, bucket);
    }

    @Override
    public void invoke(T sensorsDataPointList, Context context) throws Exception {
        windowsIn.add(1);
        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            for(SensorsDataPoint sensorsDataPoint : sensorsDataPointList) {
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
           influxDBClient.close();
        }
    }

}
