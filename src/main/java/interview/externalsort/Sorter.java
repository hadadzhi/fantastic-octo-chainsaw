package interview.externalsort;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Sorts a collection that may not fit in memory.
 */
interface Sorter {
    /**
     * Starts sorting, blocks until sorting is complete.
     */
    <T> void sort(SorterOptions<T> options);
}
