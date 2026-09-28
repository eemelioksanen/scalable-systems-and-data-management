package fi.aalto.cse4780.trading;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Publishes a result only if some trader subscribes to its symbol (D-9, D-17). Keyed by symbol,
 * with the {@code subscriptions} topic as broadcast state.
 *
 * <p>EMAs and crossovers are computed for every symbol upstream. A symbol's first published result
 * after a subscription therefore already reflects its whole history, and so does its crossover
 * check.
 *
 * <p><b>Readiness.</b> Results are held back until the filter has read the topic up to its end
 * offsets at job submission, then released against that state. So every result is judged against
 * at least the subscriptions that existed at startup, whichever source starts first. Tests and
 * correctness runs preload the topic before submitting the job (D-27).
 *
 * <p><b>Cutover.</b> After that, an update applies to every result this operator processes after
 * the update. Results already published are not withdrawn and earlier windows are not replayed.
 */
public class SubscriptionFilter
        extends KeyedBroadcastProcessFunction<String, EmaResult, SubscriptionUpdate, EmaResult> {

    private static final long serialVersionUID = 1L;

    /** A symbol list entry that subscribes to every symbol. */
    public static final String ALL_SYMBOLS = "*";

    /** Each trader's current symbol list. */
    static final MapStateDescriptor<String, String[]> TRADERS = new MapStateDescriptor<>(
            "subscription-traders", Types.STRING, BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);

    /**
     * How often each symbol, including {@link #ALL_SYMBOLS}, appears across all traders' lists.
     * Absent means never.
     */
    static final MapStateDescriptor<String, Integer> SUBSCRIBED = new MapStateDescriptor<>(
            "subscribed-symbols", Types.STRING, Types.INT);

    /** The offset of the last record read, per partition of the topic. */
    static final MapStateDescriptor<Integer, Long> POSITION = new MapStateDescriptor<>(
            "subscriptions-position", Types.INT, Types.LONG);

    private static final ListStateDescriptor<EmaResult> HELD = new ListStateDescriptor<>(
            "held-results", TypeInformation.of(EmaResult.class));

    private final HashMap<Integer, Long> startupEndOffsets;

    private transient ListState<EmaResult> held;
    private transient boolean ready;
    private transient Counter publishedResults;
    private transient Counter publishedAdvisories;
    private transient Counter suppressedResults;

    /**
     * @param startupEndOffsets end offset of each partition of {@code subscriptions} that held
     *     records at submission. Empty if the topic was empty.
     */
    public SubscriptionFilter(Map<Integer, Long> startupEndOffsets) {
        this.startupEndOffsets = new HashMap<>(startupEndOffsets);
    }

    @Override
    public void open(OpenContext openContext) {
        held = getRuntimeContext().getListState(HELD);
        ready = startupEndOffsets.isEmpty();
        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        publishedResults = metrics.counter("publishedResults");
        publishedAdvisories = metrics.counter("publishedAdvisories");
        suppressedResults = metrics.counter("suppressedResults");
        metrics.gauge("subscriptionsReady", () -> ready ? 1 : 0);
    }

    @Override
    public void processElement(EmaResult result, ReadOnlyContext ctx, Collector<EmaResult> out)
            throws Exception {
        if (isReady(ctx.getBroadcastState(POSITION))) {
            publish(result, ctx.getBroadcastState(SUBSCRIBED), out);
        } else {
            held.add(result);
        }
    }

    @Override
    public void processBroadcastElement(
            SubscriptionUpdate update, Context ctx, Collector<EmaResult> out) throws Exception {
        BroadcastState<String, Integer> subscribed = ctx.getBroadcastState(SUBSCRIBED);
        if (update.trader != null) {
            BroadcastState<String, String[]> traders = ctx.getBroadcastState(TRADERS);
            String[] previous = traders.get(update.trader);
            if (previous != null) {
                for (String symbol : previous) {
                    int count = subscribed.get(symbol) - 1;
                    if (count == 0) {
                        subscribed.remove(symbol);
                    } else {
                        subscribed.put(symbol, count);
                    }
                }
            }
            if (update.symbols == null) {
                traders.remove(update.trader);
            } else {
                traders.put(update.trader, update.symbols);
                for (String symbol : update.symbols) {
                    Integer count = subscribed.get(symbol);
                    subscribed.put(symbol, count == null ? 1 : count + 1);
                }
            }
        }

        BroadcastState<Integer, Long> position = ctx.getBroadcastState(POSITION);
        Long previousOffset = position.get(update.partition);
        if (previousOffset == null || update.offset > previousOffset) {
            position.put(update.partition, update.offset);
        }

        if (!ready && isReady(position)) {
            ctx.applyToKeyedState(HELD, (symbol, results) -> {
                for (EmaResult result : results.get()) {
                    publish(result, subscribed, out);
                }
                results.clear();
            });
        }
    }

    /** Whether the topic has been read up to its end offsets at submission. */
    private boolean isReady(ReadOnlyBroadcastState<Integer, Long> position) throws Exception {
        if (!ready) {
            ready = true;
            for (Map.Entry<Integer, Long> end : startupEndOffsets.entrySet()) {
                Long offset = position.get(end.getKey());
                if (offset == null || offset < end.getValue() - 1) {
                    ready = false;
                    break;
                }
            }
        }
        return ready;
    }

    private void publish(
            EmaResult result,
            ReadOnlyBroadcastState<String, Integer> subscribed,
            Collector<EmaResult> out)
            throws Exception {
        if (subscribed.contains(ALL_SYMBOLS) || subscribed.contains(result.symbol)) {
            publishedResults.inc();
            if (result.crossover != null) {
                publishedAdvisories.inc();
            }
            out.collect(result);
        } else {
            suppressedResults.inc();
        }
    }
}
