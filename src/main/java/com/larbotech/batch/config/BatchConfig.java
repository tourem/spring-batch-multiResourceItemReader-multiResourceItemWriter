package com.larbotech.batch.config;

import com.larbotech.batch.mapper.EmployeeFieldSetMapper;
import com.larbotech.batch.model.Employee;
import com.larbotech.batch.partitioner.CustomMultiResourcePartitioner;
import com.larbotech.batch.reader.CustomFlatFileItemReader;
import com.larbotech.batch.reader.CustomMultiResourceItemReader;
import com.larbotech.batch.reader.EmployeeCompletionPolicyReader;
import com.larbotech.batch.tasklet.UnzipTasklet;
import com.larbotech.batch.writer.ConsoleItemWriter;
import com.larbotech.batch.writer.CustomFlatFileItemWriter;
import java.io.IOException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


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

  @Autowired
  UnzipTasklet unzipTasklet;


  @Bean
  public Job readCSVFilesJob() throws IOException {
    return jobBuilderFactory
        .get("readCSVFilesJob")
        .incrementer(new RunIdIncrementer())
        .start(masterStep())
        .build();
  }

  @Bean
  public Step step1() throws IOException {
    return stepBuilderFactory.get("step1").<Employee, Employee>chunk(customFlatFileItemReader())
        .reader(multiResourceItemReader(null, null))
        .writer(fileWriter(null, null))
        .build();
  }

  @Bean
  @StepScope
  public CustomMultiResourceItemReader multiResourceItemReader(
      @Value("#{stepExecutionContext[partitionId]}") String partitionId,
      @Value("#{stepExecution.jobExecution}") JobExecution jobExecution) throws IOException {
    CustomMultiResourceItemReader resourceItemReader = new CustomMultiResourceItemReader(
        partitionId, jobExecution);
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
  @StepScope
  public CustomFlatFileItemWriter fileWriter(@Value("#{stepExecutionContext[partitionId]}") String partitionId,
      @Value("#{stepExecution.jobExecution}") JobExecution jobExecution) {
    //Create writer instance
    CustomFlatFileItemWriter writer = new CustomFlatFileItemWriter(partitionId, jobExecution);

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

  @Bean
  public Step unzipTaskletStep() {
    return stepBuilderFactory.get("unzipTaskletStep")
        .tasklet(unzipTasklet)
        .build();
  }

  @Bean
  public Flow unzipAndProcessCsvFlow() throws IOException {
    return new FlowBuilder<SimpleFlow>("unzipAndProcessCsvFlow")
        .start(unzipTaskletStep())
        .next(step1())
        .build();

  }

  @Bean("partitioner")
  @StepScope
  public Partitioner partitioner() {

    CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
    partitioner.setZipDirectory("src/main/resources/inputs");
    partitioner.partition(10);
    return partitioner;
  }

  @Bean
  public Step unzipAndProcessCsvStepFlow() throws IOException {
    return stepBuilderFactory.get("twoStepFlow")
        .flow(unzipAndProcessCsvFlow())
        .build();

  }

  @Bean
  public Step masterStep() throws IOException {
    return stepBuilderFactory.get("masterStep")
        .partitioner(unzipAndProcessCsvStepFlow())
        .partitioner("slaveStep", partitioner())
        .gridSize(2)
        .taskExecutor(taskExecutor())
        .build();


  }

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(1);
    taskExecutor.setCorePoolSize(1);
    taskExecutor.afterPropertiesSet();
    taskExecutor.setWaitForTasksToCompleteOnShutdown(true);
    return taskExecutor;
  }

}