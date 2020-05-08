package com.larbotech.batch.config;

import com.larbotech.batch.mapper.EmployeeFieldSetMapper;
import com.larbotech.batch.model.Employee;
import com.larbotech.batch.reader.CustomFlatFileItemReader;
import com.larbotech.batch.reader.CustomMultiResourceItemReader;
import com.larbotech.batch.reader.EmployeeCompletionPolicyReader;
import com.larbotech.batch.writer.ConsoleItemWriter;
import com.larbotech.batch.writer.CustomFlatFileItemWriter;
import java.io.IOException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;


@Configuration
@EnableBatchProcessing
public class BatchConfig {

  private static final String DIR_INTPUT = "src/main/resources/input";
  private static final String DIR_OUTPUT = "output";

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Value("input/inputData*.csv")
  private Resource[] inputResources;

  @Bean
  public Job readCSVFilesJob() throws IOException {
    return jobBuilderFactory
        .get("readCSVFilesJob")
        .incrementer(new RunIdIncrementer())
        .start(step1())
        .build();
  }

  @Bean
  public Step step1() throws IOException {
    return stepBuilderFactory.get("step1").<Employee, Employee>chunk(customFlatFileItemReader())
        .reader(multiResourceItemReader())
        .writer(fileWriter())
        .build();
  }

  @Bean
  public CustomMultiResourceItemReader multiResourceItemReader() throws IOException {
    CustomMultiResourceItemReader resourceItemReader = new CustomMultiResourceItemReader(
        DIR_INTPUT);
    resourceItemReader.setDelegate(customFlatFileItemReader());
    return resourceItemReader;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Bean
  public FlatFileItemReader<Employee> reader() {
    //Create reader instance
    FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();

    //Set number of lines to skips. Use it if file has header rows.
    reader.setLinesToSkip(1);

    //Configure how each line will be parsed and mapped to different values
    reader.setLineMapper(createDefaultLineMapper());
    return reader;
  }


  @Bean
  public EmployeeCompletionPolicyReader customFlatFileItemReader() {
    CustomFlatFileItemReader customFlatFileItemReader = new CustomFlatFileItemReader();
    //Set number of lines to skips. Use it if file has header rows.
    customFlatFileItemReader.setLinesToSkip(1);

    //Configure how each line will be parsed and mapped to different values
    customFlatFileItemReader.setLineMapper(createDefaultLineMapper());

    EmployeeCompletionPolicyReader policyReader = new EmployeeCompletionPolicyReader();
    policyReader.setChunkSize(3);
    policyReader.setDelegate(customFlatFileItemReader);
    return policyReader;

  }

  private DefaultLineMapper createDefaultLineMapper() {
    DefaultLineMapper<Employee> defaultLineMapper = new DefaultLineMapper<>();
    DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
    lineTokenizer.setNames("id", "firstName", "lastName");
    defaultLineMapper.setLineTokenizer(lineTokenizer);
    defaultLineMapper.setFieldSetMapper(new EmployeeFieldSetMapper());
    return defaultLineMapper;
  }


  @Bean
  public ConsoleItemWriter<Employee> writer() {
    return new ConsoleItemWriter<>();
  }


  @Bean
  public CustomFlatFileItemWriter fileWriter() {
    //Create writer instance
    CustomFlatFileItemWriter writer = new CustomFlatFileItemWriter(DIR_OUTPUT);

    //All job repetitions should "append" to same output file
    writer.setAppendAllowed(true);

    //Name field values sequence based on object properties
    writer.setLineAggregator(createEmployeeLineAggregator());
    return writer;
  }


  private LineAggregator<Employee> createEmployeeLineAggregator() {
    DelimitedLineAggregator<Employee> lineAggregator = new DelimitedLineAggregator<>();
    lineAggregator.setDelimiter(",");

    FieldExtractor<Employee> fieldExtractor = createEmployeeFieldExtractor();
    lineAggregator.setFieldExtractor(fieldExtractor);

    return lineAggregator;
  }

  private FieldExtractor<Employee> createEmployeeFieldExtractor() {
    BeanWrapperFieldExtractor<Employee> extractor = new BeanWrapperFieldExtractor<>();
    extractor.setNames(new String[]{"id", "firstName", "lastName"});
    return extractor;
  }


}