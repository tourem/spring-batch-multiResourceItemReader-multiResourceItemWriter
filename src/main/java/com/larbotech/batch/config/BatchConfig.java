package com.larbotech.batch.config;

import com.larbotech.batch.mapper.EmployeeFieldSetMapper;
import com.larbotech.batch.model.Employee;
import com.larbotech.batch.reader.CustomFlatFileItemReader;
import com.larbotech.batch.reader.EmployeeCompletionPolicyReader;
import com.larbotech.batch.writer.ConsoleItemWriter;
import com.larbotech.batch.writer.CustomFlatFileItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;


@Configuration
@EnableBatchProcessing
public class BatchConfig
{
  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Value("input/inputData*.csv")
  private Resource[] inputResources;

  @Bean
  public Job readCSVFilesJob() {
    return jobBuilderFactory
        .get("readCSVFilesJob")
        .incrementer(new RunIdIncrementer())
        .start(step1())
        .build();
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<Employee, Employee>chunk(customFlatFileItemReader())
        .reader(multiResourceItemReader())
        .writer(fileWriter())
        .build();
  }

  @Bean
  public MultiResourceItemReader<Employee> multiResourceItemReader()
  {
    MultiResourceItemReader<Employee> resourceItemReader = new MultiResourceItemReader<>();
    resourceItemReader.setResources(inputResources);
    resourceItemReader.setDelegate(customFlatFileItemReader());
    return resourceItemReader;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Bean
  public FlatFileItemReader<Employee> reader()
  {
    //Create reader instance
    FlatFileItemReader<Employee> reader = new FlatFileItemReader<>();

    //Set number of lines to skips. Use it if file has header rows.
    reader.setLinesToSkip(1);

    //Configure how each line will be parsed and mapped to different values
    reader.setLineMapper(new DefaultLineMapper() {
      {
        //3 columns in each row
        setLineTokenizer(new DelimitedLineTokenizer() {
          {
            setNames(new String[] { "id", "firstName", "lastName" });
          }
        });
        //Set values in Employee class
        setFieldSetMapper(new EmployeeFieldSetMapper());
      }
    });
    return reader;
  }


  @Bean
  public EmployeeCompletionPolicyReader customFlatFileItemReader()
  {
    CustomFlatFileItemReader customFlatFileItemReader = new CustomFlatFileItemReader();
    //Set number of lines to skips. Use it if file has header rows.
    customFlatFileItemReader.setLinesToSkip(1);

    //Configure how each line will be parsed and mapped to different values
    customFlatFileItemReader.setLineMapper(new DefaultLineMapper() {
      {
        //3 columns in each row
        setLineTokenizer(new DelimitedLineTokenizer() {
          {
            setNames(new String[] { "id", "firstName", "lastName" });
          }
        });
        //Set values in Employee class
        setFieldSetMapper(new EmployeeFieldSetMapper());
      }
    });

    EmployeeCompletionPolicyReader policyReader = new EmployeeCompletionPolicyReader();
    policyReader.setChunkSize(3);
    policyReader.setDelegate(customFlatFileItemReader);
    return policyReader;

  }

  @Bean
  public ConsoleItemWriter<Employee> writer()
  {
    return new ConsoleItemWriter<Employee>();
  }

  private Resource outputResource = new FileSystemResource("output/outputData.csv");

  @Bean
  public CustomFlatFileItemWriter fileWriter()
  {
    //Create writer instance
    CustomFlatFileItemWriter writer = new CustomFlatFileItemWriter();

    //All job repetitions should "append" to same output file
    writer.setAppendAllowed(true);


    //Name field values sequence based on object properties
    writer.setLineAggregator(new DelimitedLineAggregator<Employee>() {
      {
        setDelimiter(",");
        setFieldExtractor(new BeanWrapperFieldExtractor() {
          {
            setNames(new String[] { "id", "firstName", "lastName" });
          }
        });
      }
    });
    return writer;
  }

}