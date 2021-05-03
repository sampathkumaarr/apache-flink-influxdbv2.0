package io.intellisense.testproject.eng.function;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InterQuartileRangeCalculatorTest {

    @Test
    public void testCalculateInterQuartileRange() {
        InterQuartileRangeCalculator igrCalculator = new InterQuartileRangeCalculator();
        final double inputArray[] = {1, 19, 7, 6, 5, 9, 12, 27, 18, 2, 15};
        final double expectedValue = 13.0;
        System.out.println(igrCalculator.calculateInterQuartileRange(inputArray));
        //assertEquals(expectedValue, igrCalculator.calculateInterQuartileRange(inputArray));
    }
}
