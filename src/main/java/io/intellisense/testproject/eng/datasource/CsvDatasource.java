package io.intellisense.testproject.eng.datasource;

import lombok.RequiredArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.sources.CsvTableSource;

@RequiredArgsConstructor(staticName = "of")
public class CsvDatasource {

    final String dataset;

    /**
     * CsvTableSource only supports String and SQL dates. Mapping is needed later on.
     * @see org.apache.flink.table.sources.CsvTableSource
     */
    public CsvTableSource getCsvSource() {
        return CsvTableSource.builder()
                .path(CsvDatasource.class.getClassLoader().getResource(dataset).getPath())
                .ignoreFirstLine()
                .field("Date", DataTypes.STRING())
                .field("Sensor-1", DataTypes.STRING())
                .field("Sensor-2", DataTypes.STRING())
                .field("Sensor-3", DataTypes.STRING())
                .field("Sensor-4", DataTypes.STRING())
                .field("Sensor-5", DataTypes.STRING())
                .field("Sensor-6", DataTypes.STRING())
                .field("Sensor-7", DataTypes.STRING())
                .field("Sensor-8", DataTypes.STRING())
                .field("Sensor-9", DataTypes.STRING())
                .field("Sensor-10", DataTypes.STRING())
                .build();
    }
}
