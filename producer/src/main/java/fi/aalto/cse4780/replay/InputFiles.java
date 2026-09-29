package fi.aalto.cse4780.replay;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Resolves the replay inputs to an ordered list of CSV files.
 *
 * <p>Files named on the command line keep their order. A directory expands to its {@code *.csv}
 * files in day order, taken from DEBS names such as {@code debs2022-gc-trading-day-08-11-21.csv},
 * so a week replays Monday to Sunday (D-15). Files without a date in the name follow, by name.
 */
final class InputFiles {

    private static final Pattern DEBS_DATE = Pattern.compile("(\\d{2})-(\\d{2})-(\\d{2})\\.csv$");

    static final Comparator<Path> DAY_ORDER = Comparator
            .comparing((Path p) -> debsDate(p).orElse(LocalDate.MAX))
            .thenComparing(p -> p.getFileName().toString());

    private InputFiles() {}

    static List<Path> resolve(List<Path> inputs) throws IOException {
        List<Path> files = new ArrayList<>();
        for (Path input : inputs) {
            if (Files.isDirectory(input)) {
                try (Stream<Path> listing = Files.list(input)) {
                    List<Path> csv = listing
                            .filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".csv"))
                            .sorted(DAY_ORDER)
                            .toList();
                    if (csv.isEmpty()) {
                        throw new NoSuchFileException(input + " contains no .csv files");
                    }
                    files.addAll(csv);
                }
            } else if (Files.isRegularFile(input)) {
                files.add(input);
            } else {
                throw new NoSuchFileException(input.toString());
            }
        }
        if (new HashSet<>(files.stream().map(p -> p.toAbsolutePath().normalize()).toList()).size() != files.size()) {
            throw new IllegalArgumentException("an input file is listed more than once: " + files);
        }
        return files;
    }

    /** The trading day in a DEBS file name ({@code DD-MM-YY}). */
    static Optional<LocalDate> debsDate(Path file) {
        Matcher m = DEBS_DATE.matcher(file.getFileName().toString());
        if (!m.find()) {
            return Optional.empty();
        }
        try {
            return Optional.of(LocalDate.of(
                    2000 + Integer.parseInt(m.group(3)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(1))));
        } catch (DateTimeException e) {
            return Optional.empty();
        }
    }
}
