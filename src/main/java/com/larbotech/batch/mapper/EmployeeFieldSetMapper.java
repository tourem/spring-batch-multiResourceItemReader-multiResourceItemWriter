package com.larbotech.batch.mapper;

import com.larbotech.batch.model.Employee;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class EmployeeFieldSetMapper implements FieldSetMapper<Employee> {

  @Override
  public Employee mapFieldSet(FieldSet fieldSet) throws BindException {
    Employee employee = new Employee();
    employee.setId(fieldSet.readString("id"));
    employee.setFirstName(fieldSet.readString("firstName"));
    employee.setLastName(fieldSet.readString("lastName"));
    return employee;
  }
}