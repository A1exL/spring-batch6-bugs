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

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class JobRepositoryTests {

    @Autowired
    private Job successfulJob;

    @Autowired
    private Job failingJob;

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
    void findRunningJobExecutions_shouldNotThrowExceptionForSuccessfullyCompletedJob() throws Exception {
        JobOperatorTestUtils jobOperatorTestUtils = new JobOperatorTestUtils(jobOperator, jobRepository);
        jobOperatorTestUtils.setJob(successfulJob);
        JobExecution jobExecution = jobOperatorTestUtils.startJob();

        List<String> jobExecutionStatuses = jdbcOperations.queryForList("select e.STATUS from BATCH_JOB_EXECUTION e", String.class);
        assertThat(jobExecutionStatuses).hasSize(1).containsExactly("COMPLETED");
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Set<JobExecution> runningJobExecutions = jobRepository.findRunningJobExecutions(successfulJob.getName());
        assertThat(runningJobExecutions).isEmpty();
    }

    @Test
    void findRunningJobExecutions_shouldNotThrowExceptionForFailedJob() throws Exception {
        JobOperatorTestUtils jobOperatorTestUtils = new JobOperatorTestUtils(jobOperator, jobRepository);
        jobOperatorTestUtils.setJob(failingJob);
        JobExecution jobExecution = jobOperatorTestUtils.startJob();

        List<String> jobExecutionStatuses = jdbcOperations.queryForList("select e.STATUS from BATCH_JOB_EXECUTION e", String.class);
        assertThat(jobExecutionStatuses).hasSize(1).containsExactly("FAILED");
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.FAILED);

        Set<JobExecution> runningJobExecutions = jobRepository.findRunningJobExecutions(failingJob.getName());
        assertThat(runningJobExecutions).isEmpty();
    }


}
