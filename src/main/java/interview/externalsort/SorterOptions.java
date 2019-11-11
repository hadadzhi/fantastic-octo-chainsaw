package interview.externalsort;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Holds parameters for {@link Sorter#sort(SorterOptions)} 
 * @param <T> the type of the elements to be sorted
 */
@Builder
@Getter
public class SorterOptions<T> {
    /**
     * An iterator over the elements to be sorted
     */
    @NonNull
    private final Iterator<T> input;

    /**
     * A consumer of sorted elements
     */
    @NonNull
    private final Consumer<T> output;

    /**
     * How to sort
     */
    @NonNull
    private final Comparator<T> comparator;

    /**
     * Given a stream and an iterator, writes all elements returned by the iterator to the stream
     */
    @NonNull
    private final BiConsumer<Iterator<T>, OutputStream> writer;

    /**
     * Given a stream, returns an iterator over the elements read from that stream
     */
    @NonNull
    private final Function<InputStream, Iterator<T>> reader;

    /**
     * Maximum number of threads to use for sorting
     */
    @NonNull
    private final int threads;
}
