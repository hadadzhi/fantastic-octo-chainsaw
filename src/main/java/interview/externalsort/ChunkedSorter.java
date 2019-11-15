package interview.externalsort;

import lombok.AllArgsConstructor;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * Sorts a large file by dividing it into separately sorted chunks and then merging the chunks.
 */
public class ChunkedSorter {
    private final int nThreads;
    private final SorterOptions options;
    private final Semaphore maxParallelSorts;

    public ChunkedSorter(final SorterOptions options) {
        this.options = options;
        this.nThreads = Math.min(options.getThreads(), Runtime.getRuntime().availableProcessors());
        this.maxParallelSorts = new Semaphore(this.nThreads);
    }

    /**
     * Submits the task to the common pool,
     * blocking if the maximum number of tasks have been submitted,
     * until at least one of the tasks finishes.
     * The maximum number of tasks is regulated with a semaphore.
     * Adapted from Java Concurrency In Practice.
     */
    private <V> Future<V> boundedRunAsync(final Callable<V> task) throws InterruptedException {
        maxParallelSorts.acquire();
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return task.call();
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    if (e instanceof IOException) {
                        throw new UncheckedIOException((IOException) e);
                    }
                    throw new RuntimeException(e);
                } finally {
                    maxParallelSorts.release();
                }
            });
        } catch (RejectedExecutionException e) {
            maxParallelSorts.release();
            throw e;
        }
    }

    /**
     * Sorts a chunk of lines and writes sorted lines to a temp file
     *
     * @return the path to the temp files with sorted lines
     */
    private static Path sortChunk(final String[] lines) {
        try {
            Arrays.sort(lines);

            final Path chunkFile = Files.createTempFile("sort", ".txt");
            Files.write(chunkFile, Arrays.asList(lines), Charset.defaultCharset()); // Arrays.asList() does not copy

            return chunkFile;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * @return memory available for future allocations, in bytes
     */
    private static long availableMemory() {
        final Runtime r = Runtime.getRuntime();
        return r.maxMemory() - (r.totalMemory() - r.freeMemory()); // max - used
    }

    public void sort() {
        try (final Stream<String> input = Files.lines(options.getInput(), Charset.defaultCharset());
             final BufferedWriter output = Files.newBufferedWriter(options.getOutput(), Charset.defaultCharset())) {

            // The idea is to approximate how much memory is available and try not to exceed that amount
            // We want to keep the workers busy, so at least (nThreads + 1) chunks must fit in memory
            // (nThreads chunks are being sorted and 1 is ready to be sorted)
            // References, IO buffers and other stuff also requires memory, so factor should be < 1.
            // Empirical data shows that lower factors increase performance. GC sucks :(
            final double factor = 0.1;
            final long szChunkPreferred = (long) (factor * availableMemory() / (nThreads + 1));

            long szChunk = 0;
            final List<Future<Path>> chunks = new ArrayList<>();
            final List<String> chunk = new ArrayList<>();
            final Iterator<String> inputIt = input.iterator();
            while (inputIt.hasNext()) {
                final String line = inputIt.next();
                chunk.add(line);
                szChunk += line.length() * 2; // Approximate string memory footprint
                if (szChunk >= szChunkPreferred) {
                    final String[] lines = new String[chunk.size()];
                    chunk.toArray(lines);
                    chunk.clear();
                    szChunk = 0;
                    chunks.add(boundedRunAsync(() -> sortChunk(lines)));
                }
            }
            if (!chunk.isEmpty()) { // Submit leftover lines
                final String[] lines = new String[chunk.size()];
                chunk.toArray(lines);
                chunk.clear();
                chunks.add(boundedRunAsync(() -> sortChunk(lines)));
            }

//            mergeAndDelete(chunks, output);

// Doesn't seem to scale
            if (chunks.size() < nThreads * 2) {
                mergeAndDelete(chunks, output);
            } else {
                System.err.println("Doing threaded merge");

                final List<List<Future<Path>>> parts = partition(chunks, nThreads);
                chunks.clear();

                for (final List<Future<Path>> m : parts) {
                    chunks.add(
                            boundedRunAsync(() -> {
                                final Path tmp = Files.createTempFile("merge-", ".txt");
                                try (final BufferedWriter w = Files.newBufferedWriter(tmp, Charset.defaultCharset())) {
                                    mergeAndDelete(m, w);
                                }
                                return tmp;
                            })
                    );
                }

                mergeAndDelete(chunks, output);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Merges multiple sorted files, then deletes them.
     */
    private void mergeAndDelete(final List<Future<Path>> chunks, final BufferedWriter output) throws IOException, ExecutionException, InterruptedException {
        @AllArgsConstructor
        class MergeHelper {
            String v;
            Iterator<String> it;
            AutoCloseable closeable;
            Path deletable;
        }

        final PriorityQueue<MergeHelper> q = new PriorityQueue<>(Comparator.comparing(a -> a.v)); // Least element is always the head, add() in O(log(size))
        for (final Future<Path> f : chunks) {
            final Path p = f.get();
            final Stream<String> lines = Files.lines(p, Charset.defaultCharset());
            final Iterator<String> it = lines.iterator();
            if (it.hasNext()) {
                q.add(new MergeHelper(it.next(), it, lines, p));
            } else {
                lines.close();
            }
        }

        final Iterator<String> mergedIt = new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return !q.isEmpty();
            }

            @Override
            public String next() {
                final MergeHelper h = q.poll();
                if (h == null) {
                    throw new NoSuchElementException();
                }
                if (h.it.hasNext()) {
                    q.add(new MergeHelper(h.it.next(), h.it, h.closeable, h.deletable));
                } else {
                    try {
                        h.closeable.close();
                        Files.delete(h.deletable);
                    } catch (Exception e) {
                        // ignore, should never happen
                    }
                }
                return h.v;
            }
        };

        while (mergedIt.hasNext()) {
            output.write(mergedIt.next());
            output.newLine();
        }
    }

    private static <E> List<List<E>> partition(final List<E> list, int nLists) {
        Objects.requireNonNull(list, "list");

        if (nLists <= 0 || list.size() < nLists) {
            throw new IllegalArgumentException("nLists");
        }

        final int szSubList = (int) Math.ceil((double) list.size() / nLists);
        final List<List<E>> partitions = new ArrayList<>(nLists);

        for (int i = 0; i < nLists; ++i) {
            partitions.add(new ArrayList<>(list.subList(
                    i * szSubList,
                    Math.min(i * szSubList + szSubList, list.size())
            )));
        }

        return partitions;
    }
}
