package interview.externalsort;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Experiments {
//    private static final List<String> data = Stream.generate(() -> UUID.randomUUID().toString())
//            .limit(10_000_000)
//            .collect(Collectors.toList());
//
//    @Benchmark
//    public void sequential(Blackhole bh) {
//        final String[] strings = data.toArray(new String[]{});
//        Arrays.sort(strings);
//        bh.consume(strings);
//    }
//
//    @Benchmark
//    public void parallel(Blackhole bh) {
//        final String[] strings = data.toArray(new String[]{});
//        Arrays.parallelSort(strings);
//        bh.consume(strings);
//    }

    public static void main(String[] args) {
//        final double time = time(() -> {
//            try (final BufferedWriter w = Files.newBufferedWriter(Paths.get("output1.txt"))) {
//                Files.lines(Paths.get("output.txt")).forEach(line -> {
//                    try {
//                        w.write(line);
//                        w.newLine();
//                        //w.flush();
//                    } catch (IOException e) {
//                        throw new UncheckedIOException(e);
//                    }
//                });
//            } catch (IOException e) {
//                throw new UncheckedIOException(e);
//            }
//        });
//        System.out.println("Copied in " + time + " s");

//        final Options opt = new OptionsBuilder()
//                .include(Main.class.getSimpleName())
//                .threads(1)
//                .warmupIterations(1)
//                .measurementIterations(10)
//                .forks(1)
//                .build();
//
//        new Runner(opt).run();
    }

    /**
     * @return time to run the runnable, in seconds
     */
    public static double time(Runnable r) {
        final long s = System.nanoTime();
        r.run();
        final long f = System.nanoTime();
        return (f - s) / 1e9d;
    }
}
