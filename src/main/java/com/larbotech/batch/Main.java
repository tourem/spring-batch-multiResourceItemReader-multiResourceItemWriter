package com.larbotech.batch;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class Main {


  public static void main(String[] args) throws Exception {
    ConfigurableApplicationContext ctx = SpringApplication.run(Main.class, args);

    ExitCodeGenerator ecg = ctx.getBean(ExitCodeGenerator.class);

    int code = ecg.getExitCode();
    ctx.close();
    log.info("result code " + code);

  }


 /* @Autowired
  JobLauncher jobLauncher;

  @Autowired
  Job job;

  public static void main(String[] args)
  {
    SpringApplication.run(Main.class, args);
  }
*/

  //@Scheduled(cron = "0 */1 * * * ?")
/*  public void perform() throws Exception
  {
    JobParameters params = new JobParametersBuilder()
        .addString("JobID", String.valueOf(System.currentTimeMillis()))
        .toJobParameters();
    jobLauncher.run(job, params);
  }*/
}