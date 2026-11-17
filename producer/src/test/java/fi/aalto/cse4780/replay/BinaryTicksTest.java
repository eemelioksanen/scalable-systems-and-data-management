package fi.aalto.cse4780.replay;

import static fi.aalto.cse4780.replay.TestCsv.price;
import static fi.aalto.cse4780.replay.TestCsv.quote;
import static fi.aalto.cse4780.replay.TestCsv.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class BinaryTicksTest {

    private static final Path SEMANTICS = Path.of("..", "fixtures", "semantics");
    private static final Path CONTRACT = SEMANTICS.resolve("ticks-binary.tsv");

    private final BinaryTicks binary = new BinaryTicks();

    private String encode(String line) {
        CsvLine row = new CsvLine();
        assertEquals(CsvLine.Kind.DATA, row.parse(line));
        return HexFormat.of().formatHex(binary.encode(row));
    }

    /** Expected bytes follow docs/interfaces.md field by field. */
    @Test
    void encodesDatesTimesAndThePriceAsNumbers() {
        String rdsa = row("RDSA.NL", "08-11-2021", "09:00:01.123", "19.280000", "09:00:01.100", "");
        assertEquals("01" + "010007524453412e4e4c" + "01000145" + "02000049fb" + "0301ee66e3"
                + "04403347ae147ae148" + "0301ee66cc" + "00", encode(rdsa));
        assertEquals("01" + "010006414952" + "2e4652" + "01000145" + "02000049fb" + "0301ee6730" + "00" + "00" + "00",
                encode(quote("AIR.FR", "09:00:01.200")));
    }

    @Test
    void leavesFieldsTheJobWouldParseDifferentlyAsText() {
        String odd = row("MISS.FR", "08-11-2021", "12:00:01", "abc", "12:00:61.000", "31-02-2021");
        assertEquals("01" + "010007" + "4d4953532e4652" + "01000145" + "02000049fb" + "03029331e8"
                + "01000361" + "6263" + "01000c" + "31323a30303a36312e303030" + "01000a" + "33312d30322d32303231",
                encode(odd));
    }

    @Test
    void refusesAFieldLongerThanItsLengthPrefix() {
        CsvLine row = new CsvLine();
        row.parse(price("X".repeat(BinaryTicks.MAX_TEXT_BYTES + 1) + ".FR", "09:00:00.000", "1.0"));
        assertThrows(IllegalArgumentException.class, () -> binary.encode(row));
    }

    /**
     * {@code ticks-binary.tsv} lists every row the replayer sends from the semantics fixtures: file
     * index, line, the CSV value and the binary value in hex. The job's and the export's tests read
     * it, so the encoder and both decoders agree on the same rows. Regenerate it with
     * {@code -DregenerateFixtures=true}.
     */
    @Test
    void contractFixtureMatchesTheEncoder() throws IOException {
        List<String> lines = new ArrayList<>();
        List<Path> files;
        try (Stream<Path> csv = Files.list(SEMANTICS)) {
            files = csv.filter(p -> p.toString().endsWith(".csv")).sorted().toList();
        }
        CsvLine row = new CsvLine();
        for (int index = 0; index < files.size(); index++) {
            List<String> source = Files.readAllLines(files.get(index), StandardCharsets.UTF_8);
            for (int line = 1; line <= source.size(); line++) {
                if (row.parse(source.get(line - 1)) == CsvLine.Kind.DATA) {
                    lines.add(index + "\t" + line + "\t" + row.projection() + "\t"
                            + HexFormat.of().formatHex(binary.encode(row)));
                }
            }
        }
        String expected = String.join("\n", lines) + "\n";
        if (Boolean.getBoolean("regenerateFixtures")) {
            Files.writeString(CONTRACT, expected);
        }
        assertEquals(expected, Files.readString(CONTRACT));
    }
}
