package spring.batch.six.bugs.job;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.item.ChunkOrientedStep;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import spring.batch.six.bugs.step.ReRunnableChunkOrientedStepWorkaround;

@Configuration
public class ReRunnableJobConfig {
    public static final String JOB_NAME = "ReRunnableJob";

    @Bean
    public Job reRunnableJob(JobRepository jobRepository,
                             Step reRunnableStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(reRunnableStep)
                .build();
    }

    @Bean
    Step reRunnableStep(JobRepository jobRepository,
                        JdbcCursorItemReader<String> reader,
                        ItemWriter<String> writer) {
        ChunkOrientedStep<String, String> step = new StepBuilder("reRunnableStep", jobRepository)
                .<String, String>chunk(1000)
                .reader(reader)
                .writer(writer)
                .build();
        return new ReRunnableChunkOrientedStepWorkaround(step);
    }


}
