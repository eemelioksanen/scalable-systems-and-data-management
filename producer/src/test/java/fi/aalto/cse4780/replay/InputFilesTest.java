package fi.aalto.cse4780.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InputFilesTest {

    @TempDir
    Path dir;

    private Path touch(String name) throws IOException {
        return Files.writeString(dir.resolve(name), "");
    }

    @Test
    void ordersADirectoryByTheDayInDebsFileNames() throws IOException {
        Path dec1 = touch("debs2022-gc-trading-day-01-12-21.csv");
        Path nov8 = touch("debs2022-gc-trading-day-08-11-21.csv");
        Path nov14 = touch("debs2022-gc-trading-day-14-11-21.csv");
        Path undated = touch("chunk_00.csv");
        touch("notes.txt");

        assertEquals(List.of(nov8, nov14, dec1, undated), InputFiles.resolve(List.of(dir)));
    }

    @Test
    void keepsTheOrderOfFilesNamedExplicitly() throws IOException {
        Path nov9 = touch("debs2022-gc-trading-day-09-11-21.csv");
        Path nov8 = touch("debs2022-gc-trading-day-08-11-21.csv");

        assertEquals(List.of(nov9, nov8), InputFiles.resolve(List.of(nov9, nov8)));
    }

    @Test
    void rejectsMissingAndRepeatedInputs() throws IOException {
        Path nov8 = touch("debs2022-gc-trading-day-08-11-21.csv");

        assertThrows(NoSuchFileException.class, () -> InputFiles.resolve(List.of(dir.resolve("missing.csv"))));
        assertThrows(IllegalArgumentException.class, () -> InputFiles.resolve(List.of(nov8, dir)));
        Files.delete(nov8);
        assertThrows(NoSuchFileException.class, () -> InputFiles.resolve(List.of(dir)));
    }
}
