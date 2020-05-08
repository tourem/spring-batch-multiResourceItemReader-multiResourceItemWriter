package com.larbotech.batch.writer;

import com.larbotech.batch.model.Employee;
import com.larbotech.batch.model.EndEmployeeMarker;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class CustomFlatFileItemWriter extends FlatFileItemWriter<Employee> {

  private String currentFileName = null;
  private ExecutionContext executionContext;


  public CustomFlatFileItemWriter (){
    super.setHeaderCallback(new FlatFileHeaderCallback() {

      public void writeHeader(Writer writer) throws IOException {
        writer.write("id,firstName,lastName");

      }
    });
  }

  @Override
  public void write(List<? extends Employee> items) throws Exception {
    List<Employee> employees = items.stream().filter(e -> !(e instanceof EndEmployeeMarker)).collect(Collectors.toList());
    if (currentFileName == null){
      Resource resource = new FileSystemResource(employees.get(0).getFileName().replace(".csv", "-out.csv"));
      setResource(resource);
      currentFileName = resource.getFile().getAbsolutePath();
      super.open(executionContext);
    }
    else if (!employees.get(0).getFileName().equals(currentFileName)){
      super.close();
      super.state = null;
      Resource resource = new FileSystemResource(employees.get(0).getFileName().replace(".csv", "-out.csv"));
      setResource(resource);
      setResource(resource);
      currentFileName = resource.getFile().getAbsolutePath();
      super.open(executionContext);
    }

    super.write(employees);

  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    this.executionContext = executionContext;
  }

  @Override
  public void update(ExecutionContext executionContext) {
  }
}
