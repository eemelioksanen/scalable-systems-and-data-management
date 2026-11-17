package fi.aalto.cse4780.trading;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.util.ParameterTool;

/**
 * The design-comparison switches of D-42, one flag each. {@link #BASELINE} is the job as D-19,
 * D-6 and D-14 describe it, and a missing flag keeps the baseline's choice.
 */
record Variant(
        Operator operator,
        EmptyWindows emptyWindows,
        StateBackend stateBackend,
        TicksEncoding ticksEncoding) {

    static final Variant BASELINE = new Variant(
            Operator.PROCESS, EmptyWindows.SKIP, StateBackend.HASHMAP, TicksEncoding.CSV);

    /** {@code --operator}: how Query 1 is built. */
    enum Operator implements Choice {
        /** {@link EmaCalculator}, a keyed process function with its own close candidates (D-19). */
        PROCESS,
        /** Flink's tumbling event-time windows, with the EMAs in the window function's keyed state. */
        WINDOW
    }

    /** {@code --empty-windows}: what a window without a price event does. */
    enum EmptyWindows implements Choice {
        /** Nothing: no result, EMAs unchanged, not a crossover check's previous window (D-6). */
        SKIP,
        /** Repeats the symbol's previous close, filling the gap before its next non-empty window. */
        CARRY_FORWARD
    }

    /** {@code --state-backend}: Flink's {@code state.backend.type}. */
    enum StateBackend implements Choice {
        HASHMAP,
        ROCKSDB
    }

    /** {@code --ticks-encoding}: the value format of {@code ticks}, which must match the replayer's. */
    enum TicksEncoding implements Choice {
        /** The 7-field CSV projection (D-14). */
        CSV,
        /** Typed fields with a text fallback (docs/interfaces.md). */
        BINARY
    }

    /** A flag value: the constant's name in lower case, with dashes. */
    interface Choice {
        String name();

        default String flag() {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }
    }

    static Variant from(ParameterTool params) {
        return new Variant(
                choice(params, "operator", BASELINE.operator),
                choice(params, "empty-windows", BASELINE.emptyWindows),
                choice(params, "state-backend", BASELINE.stateBackend),
                choice(params, "ticks-encoding", BASELINE.ticksEncoding));
    }

    private static <E extends Enum<E> & Choice> E choice(ParameterTool params, String flag, E fallback) {
        if (!params.has(flag)) {
            return fallback;
        }
        String value = params.get(flag);
        E[] values = fallback.getDeclaringClass().getEnumConstants();
        return Arrays.stream(values)
                .filter(v -> v.flag().equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("--" + flag + " must be one of "
                        + Arrays.stream(values).map(Choice::flag).collect(Collectors.joining(", "))
                        + ": " + value));
    }
}
