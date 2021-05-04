package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.model.SensorsDataPoint;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

public class CalculateIqrWithinWindow  extends ProcessAllWindowFunction<Row, List<SensorsDataPoint>, GlobalWindow> {

    public static final String SENSOR_HYPHEN = "Sensor-";

    /**
     * This method is to calculate IQR for the given records in the WINDOW (100)
     * and add the "Anomaly Score" per sensor based on the IQR value per sensor for the given WINDOW array.
     * */
    @Override
    public void process(Context context, Iterable<Row> iterable, Collector<List<SensorsDataPoint>> collector) throws Exception {
        if (iterable != null) {
            final int windowSize = (int) StreamSupport.stream(iterable.spliterator(), false).count();
            int howManySensors = 0;
            if (windowSize > 0) {
                System.out.println("Record:" + StreamSupport.stream(iterable.spliterator(), false).findFirst());
                if (StreamSupport.stream(iterable.spliterator(), false).findFirst().isPresent()) {
                    howManySensors = StreamSupport.stream(iterable.spliterator(), false).findFirst().get().getArity() - 1;
                }
            }
            Map<String, List<Double>> mapOfSensorsDataArrayList = new HashMap();
            Map<String, Double> mapOfSensorsIQR = new HashMap<>();

            System.out.println("windowSize:" + windowSize);
            System.out.println("howManySensors:" + howManySensors);

            for (int i = 1; i <= howManySensors; i++) {
                mapOfSensorsDataArrayList.put((SENSOR_HYPHEN + i), new ArrayList<>());
            }
            for (Row rowRecord : iterable) {
                for (int sensorsCounter = 1; sensorsCounter <= howManySensors; sensorsCounter++) {
                    Optional <String> tempValue = Optional.of(((String)rowRecord.getField(sensorsCounter)).trim()).filter(input -> !input.isEmpty());
                    Double parsedValue = tempValue.map((input) -> Double.parseDouble(input)).orElse(Double.NaN);
                    mapOfSensorsDataArrayList.get((SENSOR_HYPHEN + sensorsCounter))
                                .add(parsedValue);
                }
            }
            // Iterate through number of Sensors and the window records array
            for (int i = 1; i <= howManySensors; i++) {
                // Convert ArrayList of Sensors value to Double Array
                final double[] doubleArrayPerSensor = mapOfSensorsDataArrayList
                        .get((SENSOR_HYPHEN + i)).stream().mapToDouble(d -> d).toArray();
                // Calculate the IQR for each sensor.
                mapOfSensorsIQR.put((SENSOR_HYPHEN + i), InterQuartileRangeCalculator
                        .calculateInterQuartileRange(doubleArrayPerSensor));
            }
            System.out.println("IQR for this window:" + mapOfSensorsIQR);

            List<SensorsDataPoint> sensorsDataPointList = new ArrayList<>();
            for (Row row : iterable) {
                /*System.out.println(Instant.parse((String) row.getField(0)));
                System.out.println(row.getField(1));
                System.out.println(row.getField(2));
                System.out.println(row.getField(3));
                System.out.println(row.getField(4));
                System.out.println(row.getField(5));
                System.out.println(row.getField(6));
                System.out.println(row.getField(7));
                System.out.println(row.getField(8));
                System.out.println(row.getField(9));
                System.out.println(row.getField(10));*/

                for (int i = 1; i <= howManySensors; i++) {
                    Optional <String> tempValue = Optional.of(((String)row.getField(i)).trim()).filter(input -> !input.isEmpty());
                    Double parsedValue = tempValue.map((input) -> Double.parseDouble(input)).orElse(Double.NaN);
                    SensorsDataPoint sensorsDataPoint = new SensorsDataPoint(
                            (SENSOR_HYPHEN + i),
                            parsedValue,
                            calculateAnomalyScore(parsedValue,
                                    mapOfSensorsIQR.get((SENSOR_HYPHEN + i))),
                                    //Instead of writing actual timestamp, I am writing Instant.now() to easily view in InfluxDB
                                    //Instant.parse((String) row.getField(0))
                                    Instant.now());
                    sensorsDataPointList.add(sensorsDataPoint);
                }
            }
            System.out.println("sensorsDataPointList:" + sensorsDataPointList);
            collector.collect(sensorsDataPointList);
            //System.out.println("***************************");
            //Thread.sleep(2000);
        }
    }

    private double calculateAnomalyScore(double value, double iqr){
        if (value < (1.5 * iqr)) {
            return 0.0;
        } else if ((value >= (1.5 * iqr)) && (value < (3 * iqr))) {
            return 0.5;
        } else if (value >= (3 * iqr)) {
            return 1.0;
        } else {
            return Double.NaN;
        }
    };

    /*transient BiFunction<Double, Double, Double> calculateAnomalyScore = (value, iqr) -> {
        if (value < (1.5 * iqr)) {
            return 0.0;
        } else if ((value >= (1.5 * iqr)) && (value < (3 * iqr))) {
            return 0.5;
        } else if (value >= (3 * iqr)) {
            return 1.0;
        } else {
            return Double.NaN;
        }
    };*/
}
