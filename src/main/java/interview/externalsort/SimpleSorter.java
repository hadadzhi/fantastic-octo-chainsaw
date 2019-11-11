package interview.externalsort;

import lombok.AllArgsConstructor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class SimpleSorter implements Sorter {
    private final int chunkSize;

    public SimpleSorter(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public <T> void sort(SorterOptions<T> options) {
        final ExecutorService executor = new ThreadPoolExecutor(
                options.getThreads() - 1,
                options.getThreads() - 1,
                0, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        // TODO nope, this will not work either, need to _block_ if there's `options.getThreads()` sorts already running; write own executor?

        final List<Future<Path>> chunks = sortInChunks(options, chunkSize, executor);

        // TODO merge in chunks for parallelism and reduced memory
        final Path result;
        try {
            result = submitMerge(
                    chunks.stream().map(f -> {
                        try {
                            return f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList()),
                    options,
                    executor
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        // TODO is this copy horrible enough to try to optimize?
        try (final InputStream is = Files.newInputStream(result)) {
            options.getReader().apply(is).forEachRemaining(options.getOutput());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <T> List<Future<Path>> sortInChunks(SorterOptions<T> options, int chunkSize, ExecutorService executor) {
        final List<Future<Path>> chunkFiles = new ArrayList<>();

        List<T> chunk = new ArrayList<>();
        while (options.getInput().hasNext()) {
            chunk.add(options.getInput().next());

            if (chunk.size() == chunkSize) {
                chunkFiles.add(submitSort(chunk, options, executor));
                chunk = new ArrayList<>();
            }
        }

        if (!chunk.isEmpty()) {
            chunkFiles.add(submitSort(chunk, options, executor));
        }

        return chunkFiles;
    }

    private <T> Future<Path> submitSort(final List<T> chunk, final SorterOptions<T> options, ExecutorService executor) {
        return executor.submit(() -> {
            // TODO: List.sort() copies itself to an array, then copies back, use arrays throughout (will have to write an Iterator over an array)
            chunk.sort(options.getComparator());

            final Path tmp = Files.createTempFile(SimpleSorter.class.getSimpleName(), null);

            try (final OutputStream os = Files.newOutputStream(tmp)) {
                options.getWriter().accept(chunk.iterator(), os);
            }

            return tmp;
        });
    }

    @AllArgsConstructor
    private static class MergeEntry<E> {
        E value;
        Iterator<E> source;
    }

    private <T> Future<Path> submitMerge(List<Path> chunks, SorterOptions<T> options, ExecutorService executor) {
        return executor.submit(() -> mergeChunks(chunks, options));
    }

    /**
     * Merges k sorted files into a single sorted file
     * and returns that file. <b>Deletes</b> the original files.
     * Given only one file, returns that file.
     */
    private <T> Path mergeChunks(List<Path> chunks, SorterOptions<T> options) throws IOException {
        if (chunks.size() == 0) {
            throw new IllegalArgumentException("chunks.size() == 0");
        }

        if (chunks.size() == 1) {
            return chunks.get(0);
        }

        final List<Closeable> streams = new ArrayList<>();
        final PriorityQueue<MergeEntry<T>> queue = new PriorityQueue<>((a, b) -> options.getComparator().compare(a.value, b.value));

        for (final Path path : chunks) {
            final InputStream stream = Files.newInputStream(path);
            final Iterator<T> it = options.getReader().apply(stream);
            if (it.hasNext()) {
                queue.add(new MergeEntry<>(it.next(), it));
            }
            streams.add(stream);
        }

        final Iterator<T> mergedIterator = new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !queue.isEmpty();
            }

            @Override
            public T next() {
                final MergeEntry<T> e = queue.poll();
                if (e == null) {
                    throw new NoSuchElementException();
                }
                if (e.source.hasNext()) {
                    queue.add(new MergeEntry<>(e.source.next(), e.source));
                }
                return e.value;
            }
        };

        final Path tmp = Files.createTempFile(SimpleSorter.class.getSimpleName(), null);
        try (final OutputStream os = Files.newOutputStream(tmp)) {
            options.getWriter().accept(mergedIterator, os);
        }

        for (final Closeable stream : streams) {
            stream.close();
        }
        for (final Path chunk : chunks) {
            Files.delete(chunk);
        }

        return tmp;
    }
}
