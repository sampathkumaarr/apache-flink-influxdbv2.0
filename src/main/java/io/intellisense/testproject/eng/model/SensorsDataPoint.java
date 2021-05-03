package io.intellisense.testproject.eng.model;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.RequiredArgsConstructor;

import java.time.Instant;

@RequiredArgsConstructor
@Measurement(name = "sensor_data")
public class SensorsDataPoint {

    @Column(tag = true, name = "sensor")
    final public String sensor;

    @Column(name = "value")
    final public Double value;

    @Column(name = "anomalyScore")
    final public Double anomalyScore;

    @Column(timestamp = true, name = "time")
    final public Instant time;
}
