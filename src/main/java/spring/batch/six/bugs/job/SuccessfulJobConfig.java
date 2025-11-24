package spring.batch.six.bugs.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SuccessfulJobConfig {

    public static final String JOB_NAME = "SuccessfulJob";


    @Bean
    public Job successfulJob(JobRepository jobRepository,
                             Step successfulStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(successfulStep)
                .build();
    }

    @Bean
    Step successfulStep(JobRepository jobRepository,
                        JdbcCursorItemReader<String> reader,
                        ItemWriter<String> writer) {
        return new StepBuilder("successfulStep", jobRepository)
                .<String, String>chunk(1000)
                .reader(reader)
                .writer(writer)
                .build();
    }
}
