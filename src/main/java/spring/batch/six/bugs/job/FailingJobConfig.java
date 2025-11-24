package spring.batch.six.bugs.job;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class FailingJobConfig {

    public static final String JOB_NAME = "FailingJob";

    @Bean
    public Job failingJob(JobRepository jobRepository,
                                         Step failingStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(failingStep)
                .build();
    }

    @Bean
    Step failingStep(JobRepository jobRepository, JdbcCursorItemReader<String> reader,
                               ItemProcessor<String, String> failingProcessor,
                               ItemWriter<String> writer) {
        return new StepBuilder("failingStep", jobRepository)
                .<String, String>chunk(1000)
                .reader(reader)
                .processor(failingProcessor)
                .writer(writer)
                .build();
    }


    @Bean
    ItemProcessor<String, String> failingProcessor() {
        return new ItemProcessor<>() {
            @Override
            public @Nullable String process(String item) {
                if ("value_2".equals(item)) {
                    throw new IllegalStateException("Exception in failing job");
                }
                return item;
            }
        };
    }

}
