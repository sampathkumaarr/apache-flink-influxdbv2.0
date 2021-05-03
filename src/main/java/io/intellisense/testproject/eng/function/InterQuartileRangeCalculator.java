package io.intellisense.testproject.eng.function;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class InterQuartileRangeCalculator {

    public static double calculateInterQuartileRange(double[] arrayOfValues) {
        DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics(arrayOfValues);
        return (descriptiveStatistics.getPercentile(75) -
                descriptiveStatistics.getPercentile(25));
    }
}
