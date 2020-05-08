package com.larbotech.batch.reader;

import com.larbotech.batch.model.Employee;
import com.larbotech.batch.model.EndEmployeeMarker;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.core.io.Resource;

@Slf4j

public class EmployeeCompletionPolicyReader extends SimpleCompletionPolicy implements
    ResourceAwareItemReaderItemStream<Employee>, ItemReader<Employee> {

  @Setter
  private FlatFileItemReader<Employee> delegate;

  private boolean endFile = false;
  private Employee currentReadItem = null;



  @Override
  public Employee read() throws Exception {
    currentReadItem = delegate.read();
    if (currentReadItem == null && !endFile){
      endFile = true;
      return new EndEmployeeMarker();
    }
    return currentReadItem;
  }

  @Override
  public RepeatContext start(final RepeatContext context) {
    return new ComparisonPolicyTerminationContext(context);
  }

  @Override
  public void setResource(Resource resource) {
    delegate.setResource(resource);
    endFile = false;
  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
  delegate.open(executionContext);
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
delegate.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
delegate.close();
  }


  protected class ComparisonPolicyTerminationContext extends SimpleTerminationContext {

    public ComparisonPolicyTerminationContext(final RepeatContext context) {
      super(context);
    }

    @Override
    public boolean isComplete() {
      return currentReadItem == null || super.isComplete();
    }
  }


}
