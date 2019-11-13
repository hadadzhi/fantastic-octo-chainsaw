package interview.externalsort;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.nio.file.Path;

@Builder
@Getter
@ToString
public class SorterOptions {
    @NonNull
    private final Path input;

    @NonNull
    private final Path output;

    @Builder.Default
    private final int threads;
}
