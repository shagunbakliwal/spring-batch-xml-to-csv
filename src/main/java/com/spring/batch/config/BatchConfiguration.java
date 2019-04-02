package com.spring.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

import com.spring.batch.model.Report;
import com.spring.batch.processor.FilterReportProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	FilterReportProcessor filterReportProcessor;
	
	@Autowired
	JobRepository jobRepository;

	public StaxEventItemReader<Report> reader() {
		StaxEventItemReader<Report> reader = new StaxEventItemReader<Report>();
		reader.setResource(new ClassPathResource("report.xml"));
		reader.setFragmentRootElementName("record");
		reader.setUnmarshaller(marshaller());
		return reader;

	}

	public Jaxb2Marshaller marshaller() {
		Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
		marshaller.setClassesToBeBound(Report.class);
		return marshaller;
	}

	public SimpleJobLauncher jobLauncher() throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);
		return jobLauncher;
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<Report, Report>chunk(1).reader(reader()).processor(filterReportProcessor)
				.writer(writer()).build();
	}

	@Bean
	public Job reportJob() throws Exception {
		return jobBuilderFactory.get("reportJob").repository(jobRepository).start(step()).build();
	}

	public FlatFileItemWriter<Report> writer() {
		FlatFileItemWriter<Report> writer = new FlatFileItemWriter<Report>();
		writer.setShouldDeleteIfExists(true);
		writer.setResource(new FileSystemResource("report.csv"));
		writer.setLineAggregator(new DelimitedLineAggregator<Report>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<Report>() {
					{
						setNames(new String[] { "refId", "name", "age", "csvDob", "income" });
					}
				});
			}
		});
		return writer;
	}
	
	@Bean
	public PlatformTransactionManager getTransactionManager() {
	    return new ResourcelessTransactionManager();
	}

	@Bean
	public JobRepository getJobRepo(ResourcelessTransactionManager transactionManager) throws Exception {
	    return new MapJobRepositoryFactoryBean(transactionManager).getObject();
	}
}
