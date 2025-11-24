package spring.batch.six.bugs;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.test.annotation.DirtiesContext;
import spring.batch.six.bugs.job.WriterSink;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class JobReRunTests {
    @Autowired
    private Job successfulJob;

    @Autowired
    private Job reRunnableJob;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcOperations jdbcOperations;

    @Autowired
    private WriterSink writerSink;

    @BeforeEach
    void createCustomTable() {
        String createSql = """
                create table if not exists some_table(
                    some_column VARCHAR(36) PRIMARY KEY
                )
                """;

        String insertSql = """
                INSERT INTO some_table (some_column)
                VALUES
                    ('value_1'),
                    ('value_2'),
                    ('value_3');
                """;
        jdbcOperations.execute(createSql);
        jdbcOperations.execute(insertSql);
        log.info("Tables created successfully");
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        writerSink.getItems().clear();
    }

    @AfterEach
    void cleanUp() {
        jdbcOperations.execute("delete from some_table");
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        writerSink.getItems().clear();
    }

    @Test
    void shouldReRunJobAndReadDataAgain() throws Exception {
        assertThat(writerSink.getItems()).isEmpty();
        JobOperatorTestUtils jobOperatorTestUtils = new JobOperatorTestUtils(jobOperator, jobRepository);
        jobOperatorTestUtils.setJob(successfulJob);
        JobExecution firstExecution = jobOperatorTestUtils.startJob();
        assertThat(firstExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(writerSink.getItems()).hasSize(3);

        JobExecution secondExecution = jobOperatorTestUtils.startJob();
        assertThat(secondExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(writerSink.getItems()).hasSize(6);
    }

    @Test
    void shouldReRunJobAndReadDataAgainWithWorkaround() throws Exception {
        assertThat(writerSink.getItems()).isEmpty();
        JobOperatorTestUtils jobOperatorTestUtils = new JobOperatorTestUtils(jobOperator, jobRepository);
        jobOperatorTestUtils.setJob(reRunnableJob);
        JobExecution firstExecution = jobOperatorTestUtils.startJob();
        assertThat(firstExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(writerSink.getItems()).hasSize(3);

        JobExecution secondExecution = jobOperatorTestUtils.startJob();
        assertThat(secondExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(writerSink.getItems()).hasSize(6);
    }
}
