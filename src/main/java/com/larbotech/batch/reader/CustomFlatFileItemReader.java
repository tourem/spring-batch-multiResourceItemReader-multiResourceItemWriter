package com.larbotech.batch.reader;

import com.larbotech.batch.model.Employee;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;

public class CustomFlatFileItemReader extends FlatFileItemReader<Employee> {

  private Resource resource;


  @Override
  public void setResource(Resource resource) {
    super.setResource(resource);
    this.resource = resource;
  }

  @Override
  protected Employee doRead() throws Exception {
    Employee employee = super.doRead();
    if (employee != null){
      employee.setFileName(resource.getFile().getAbsolutePath());
    }

    return employee;
  }
}
