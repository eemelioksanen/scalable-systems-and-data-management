package fi.aalto.cse4780.trading;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The binary {@code ticks} value against the CSV one, on every row the replayer sends from the
 * semantics fixtures ({@code fixtures/semantics/ticks-binary.tsv}, written by the replayer's
 * encoder): each must be classified alike and give the same tick.
 */
class BinaryTicksContractTest {

    static final Path CONTRACT = Path.of("..", "fixtures", "semantics", "ticks-binary.tsv");

    /** A row of the contract fixture. */
    record Row(int file, long line, String csv, byte[] binary) {}

    static List<Row> rows() throws IOException {
        return Files.readAllLines(CONTRACT).stream()
                .map(line -> line.split("\t", -1))
                .map(f -> new Row(Integer.parseInt(f[0]), Long.parseLong(f[1]), f[2], HexFormat.of().parseHex(f[3])))
                .toList();
    }

    @Test
    void everyRowIsClassifiedAsItsCsvValueIs() throws IOException {
        TickParser csvParser = new TickParser(ZoneOffset.UTC);
        TickParser binaryParser = new TickParser(ZoneOffset.UTC);
        Map<TickParser.Outcome, Integer> outcomes = new TreeMap<>();
        for (Row row : rows()) {
            Tick fromCsv = new Tick();
            Tick fromBinary = new Tick();
            TickParser.Outcome expected = csvParser.parse(row.csv(), fromCsv);
            String at = row.file() + ":" + row.line() + " " + row.csv();
            assertEquals(expected, binaryParser.parseBinary(row.binary(), fromBinary), at);
            assertEquals(fromCsv.toString(), fromBinary.toString(), at);
            outcomes.merge(expected, 1, Integer::sum);
        }
        // fixtures/semantics/derivation.md, "Counts": 71 accepted, one dated from Date.
        assertEquals(Map.of(
                TickParser.Outcome.PRICE, 70,
                TickParser.Outcome.PRICE_SYSTEM_DATE, 1,
                TickParser.Outcome.NON_PRICE, 2,
                TickParser.Outcome.NON_POSITIVE_PRICE, 2,
                TickParser.Outcome.MISSING_TIMESTAMP, 2,
                TickParser.Outcome.MALFORMED, 3), outcomes);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "02",
        "01",
        "0101000452",
        "01010001410000020000",
        "0100000000000000",
        "0101000141000000000000ff",
        "01010001410000000000000000",
        "010200000000000000000000",
    })
    void aValueOffTheLayoutIsMalformed(String hex) {
        assertEquals(TickParser.Outcome.MALFORMED,
                new TickParser().parseBinary(HexFormat.of().parseHex(hex), new Tick()));
    }
}
