package spring.batch.six.bugs.job;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Component
public class WriterSink implements Consumer<String> {

    @Getter
    private final List<String> items = new ArrayList<>();

    @Override
    public void accept(String s) {
        items.add(s);
    }
}
