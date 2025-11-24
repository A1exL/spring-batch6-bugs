package spring.batch.six.bugs.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Slf4j
@Configuration
public class BaseConfig {

    @Bean
    public JdbcCursorItemReader<String> reader(DataSource dataSource) {
        String sql = "select some_column from some_table";
        return new JdbcCursorItemReader<>(dataSource, sql, (rs, i) -> rs.getString("some_column"));
    }

    @Bean
    public ItemWriter<String> writer(WriterSink writerSink) {
        return chunk -> {
            for (String item : chunk) {
                writerSink.accept(item);
                log.info("Writing item '{}'", item);
            }
        };
    }
}
