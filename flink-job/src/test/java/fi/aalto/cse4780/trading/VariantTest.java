package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.util.ParameterTool;
import org.junit.jupiter.api.Test;

class VariantTest {

    @Test
    void missingFlagsKeepTheBaseline() {
        assertEquals(Variant.BASELINE, Variant.from(ParameterTool.fromArgs(new String[] {"--bounded"})));
    }

    @Test
    void eachFlagSelectsItsVariant() {
        Variant variant = Variant.from(ParameterTool.fromArgs(new String[] {
            "--operator", "window", "--empty-windows", "carry-forward", "--state-backend", "rocksdb",
            "--ticks-encoding", "binary"}));
        assertEquals(new Variant(Variant.Operator.WINDOW, Variant.EmptyWindows.CARRY_FORWARD,
                Variant.StateBackend.ROCKSDB, Variant.TicksEncoding.BINARY), variant);
    }

    @Test
    void anUnknownValueNamesTheChoices() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> Variant.from(ParameterTool.fromArgs(new String[] {"--empty-windows", "fill"})));
        assertEquals("--empty-windows must be one of skip, carry-forward: fill", error.getMessage());
    }
}
