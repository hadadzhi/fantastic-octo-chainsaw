package interview.externalsort;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        try {
            final SorterOptions options = parseArgs(args);
            if (options == null) {
                printUsage();
                return;
            }

            final double time = time(() -> new ChunkedSorter(options).sort());

            System.out.println("Run time: " + time + " s");
        } catch (Exception e) {
            final StringWriter sw = new StringWriter();
            try (final PrintWriter pw = new PrintWriter(sw)) {
                e.printStackTrace(pw);
                System.err.println("sort: " + sw.toString());
            }
        }
    }

    private static SorterOptions parseArgs(String[] args) {
        if (args.length != 3) {
            return null;
        }
        final Path input = Paths.get(args[0]);
        final Path output = Paths.get(args[1]);
        final int nThreads;
        try {
            nThreads = Integer.parseInt(args[2]);
        } catch (NumberFormatException ignored) {
            return null;
        }
        if (nThreads <= 0) {
            return null;
        }
        return SorterOptions.builder()
                .input(input)
                .output(output)
                .threads(nThreads)
                .build();
    }

    private static void printUsage() {
        System.out.println("Usage: <java -jar ...> <in_file> <out_file> <n_threads>\n\n\tn_threads > 0\n");
    }

    /**
     * @return command's run time in seconds
     */
    private static double time(Runnable command) {
        final long s = System.nanoTime();
        command.run();
        final long f = System.nanoTime();
        return (f - s) / 1e9d;
    }
}
