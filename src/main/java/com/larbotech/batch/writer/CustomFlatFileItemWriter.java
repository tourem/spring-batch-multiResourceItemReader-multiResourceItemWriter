package com.larbotech.batch.writer;

import static org.springframework.util.CollectionUtils.isEmpty;

import com.larbotech.batch.model.Employee;
import com.larbotech.batch.model.EndEmployeeMarker;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

public class CustomFlatFileItemWriter extends FlatFileItemWriter<Employee> {

  private String currentFileName = null;
  private ExecutionContext executionContext;


  private File outputDir;

  public CustomFlatFileItemWriter(String outputDir) {
    this.outputDir = new File(outputDir);
    super.setHeaderCallback(writer -> writer.write("id,firstName,lastName"));
  }

  @Override
  public void write(List<? extends Employee> items) throws Exception {
    List<Employee> employees = items.stream().filter(e -> !(e instanceof EndEmployeeMarker))
        .collect(Collectors.toList());

    if (!isEmpty(employees)) {
      if (isNotInitialized()) {
        initialize(employees.get(0));
      } else if (fileHasChanged(employees.get(0))) {
        changeFile(employees.get(0));
      }

      super.write(employees);
    }

  }

  private void initialize(Employee employee) throws IOException {
    createNewResource(employee);
  }

  private void createNewResource(Employee employee) throws IOException {
    File output = File.createTempFile("output-", ".csv", outputDir);
    Resource resource = new FileSystemResource(output.toPath());
    setResource(resource);
    currentFileName = employee.getFileName();
    super.open(executionContext);
  }

  private void changeFile(Employee employee) throws IOException {
    super.close();
    super.state = null;
    createNewResource(employee);
  }

  private boolean isNotInitialized() {
    return currentFileName == null;
  }

  private boolean fileHasChanged(Employee record) {
    return record.getFileName() != null && !record.getFileName().equals(currentFileName);
  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    this.executionContext = executionContext;
  }

  @Override
  public void update(ExecutionContext executionContext) {
  }
}
