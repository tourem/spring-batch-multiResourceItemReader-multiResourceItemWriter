package com.larbotech.batch.tasklet;

import com.larbotech.batch.util.UnzipUtility;
import java.io.File;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@StepScope
public class UnzipTasklet implements Tasklet{

  private static final String OUT_PUT_DIR = "outputs";

  private String zipFilePath;
  private String partitionId;
  private JobExecution jobExecution;


  public UnzipTasklet(@Value("#{stepExecutionContext[partitionId]}") String partitionId,
      @Value("#{stepExecutionContext[zipFilePath]}") String zipFilePath,
      @Value("#{stepExecution.jobExecution}") JobExecution jobExecution) {
    this.zipFilePath = zipFilePath;
    this.partitionId = partitionId;
    this.jobExecution = jobExecution;
  }

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    try{

      Path output = Files.createTempDirectory(Paths.get(OUT_PUT_DIR), partitionId+"-csvs");
      UnzipUtility.unzip(zipFilePath, output.toFile().getAbsolutePath());
      jobExecution.getExecutionContext().putString(partitionId, output.toFile().getAbsolutePath());

    }catch(Exception e){
      log.error("Error unzip file", e);
    }
    return RepeatStatus.FINISHED;
  }
}
