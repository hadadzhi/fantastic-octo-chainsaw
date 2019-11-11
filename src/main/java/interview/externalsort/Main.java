package interview.externalsort;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Hello!");

        try (final BufferedWriter output = Files.newBufferedWriter(Paths.get("output.txt"))) {
            final SorterOptions<String> options = SorterOptions.<String>builder()
                    .input(Files.lines(Paths.get("input.txt")).iterator())
                    .output(str -> {
                        try {
                            output.write(str);
                            output.newLine();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .comparator(Comparator.naturalOrder())
                    .threads(1)
                    .reader(is -> new BufferedReader(new InputStreamReader(is)).lines().iterator())
                    .writer((it, os) -> {
                        try (final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
                            it.forEachRemaining(str -> {
                                try {
                                    writer.write(str);
                                    writer.newLine();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .build();

            final double time = Experiments.time(() -> {
                try {
                    new SimpleSorter(10).sort(options);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            System.out.println("Time: " + time + " s");
        }
    }
}
