package spring.batch.six.bugs.step;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.jspecify.annotations.NullMarked;
import org.springframework.batch.core.job.JobInterruptedException;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.StoppableStep;
import org.springframework.batch.core.step.item.ChunkOrientedStep;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

@Slf4j
@NullMarked
@RequiredArgsConstructor
public class ReRunnableChunkOrientedStepWorkaround implements StoppableStep, InitializingBean, BeanNameAware {

    private final ChunkOrientedStep<?, ?> delegate;

    @Override
    public void setBeanName(String name) {
        delegate.setBeanName(name);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        delegate.afterPropertiesSet();
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public boolean isAllowStartIfComplete() {
        return delegate.isAllowStartIfComplete();
    }

    @Override
    public int getStartLimit() {
        return delegate.getStartLimit();
    }

    @Override
    public void execute(StepExecution stepExecution) throws JobInterruptedException {
        try {
            delegate.execute(stepExecution);
        } finally {
            resetChunkTracker();
        }
    }

    private void resetChunkTracker() {
        try {
            Object chunkTracker = FieldUtils.readField(delegate, "chunkTracker", true);
            FieldUtils.writeField(chunkTracker, "moreItems", true, true);
        } catch (IllegalAccessException e) {
            log.error("Cannot reset chunkTracker.moreItems field for {}", getName(), e);
        }
    }

    @Override
    public String toString() {
        return "%s(%s)".formatted(getClass().getSimpleName(), getName());
    }

}
