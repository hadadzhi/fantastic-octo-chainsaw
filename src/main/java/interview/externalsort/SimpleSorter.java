package interview.externalsort;

import lombok.AllArgsConstructor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SimpleSorter implements Sorter {
    private final int chunkSize;

    // TODO maybe use common pool and throttle with a semaphore?
    private static class SimpleExecutor {
        private final SynchronousQueue<Runnable> queue = new SynchronousQueue<>();
        private final List<Thread> workers = new ArrayList<>();
        private final AtomicBoolean running = new AtomicBoolean(true);

        public SimpleExecutor(int threads) {
            for (int i = 0; i < threads; ++i) {
                final Thread t = new Thread(() -> {
                    while (running.get()) {
                        try {
                            queue.take().run();
                        } catch (InterruptedException ignored) {
                            // continue
                        }
                    }
                });
                workers.add(t);
                t.start();
            }
        }

        /**
         * Blocks until a thread becomes available to process the task
         */
        public <V> Future<V> submit(Callable<V> callable) throws InterruptedException {
            final FutureTask<V> task = new FutureTask<>(callable);
            queue.put(task);
            return task;
        }

        /**
         * Signals
         */
        public void shutdown() throws InterruptedException {
            running.set(false);
            for (final Thread t : workers) {
                t.interrupt();
                t.join();
            }
        }
    }

    public SimpleSorter(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public <T> void sort(SorterOptions<T> options) {
        final SimpleExecutor executor = new SimpleExecutor(options.getThreads());
        try {
            final List<Future<Path>> chunks = sortInChunks(options, chunkSize, executor);

            // TODO: merge chunks as they become available, in groups of k (adjustable parameter), in parallel
            final List<Path> paths = new ArrayList<>();
            for (final Future<Path> f : chunks) {
                paths.add(f.get());
            }

            final Path result = submitMerge(paths, options, executor).get();

            // TODO is this copy horrible enough to try to optimize?
            try (final InputStream is = Files.newInputStream(result)) {
                options.getReader().apply(is).forEachRemaining(options.getOutput());
            }

            executor.shutdown();
        } catch (InterruptedException | ExecutionException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> List<Future<Path>> sortInChunks(SorterOptions<T> options, int chunkSize, SimpleExecutor executor) throws InterruptedException {
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

    private <T> Future<Path> submitSort(final List<T> chunk, final SorterOptions<T> options, SimpleExecutor executor) throws InterruptedException {
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

    private <T> Future<Path> submitMerge(List<Path> chunks, SorterOptions<T> options, SimpleExecutor executor) throws InterruptedException {
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

        @AllArgsConstructor
        class MergeEntry<E> {
            E value;
            Iterator<E> source;
        }

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
