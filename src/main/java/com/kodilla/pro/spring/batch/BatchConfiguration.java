package com.kodilla.pro.spring.batch;

import com.kodilla.pro.spring.batch.dto.InData;
import com.kodilla.pro.spring.batch.dto.OutData;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public LocalDate currentLocalDate() {
        return LocalDate.now();
    }

    @Bean
    public CalculateAge calculateAge() {
        return new CalculateAge();
    }

    public ConversionService conversionService() {
        DefaultConversionService conversionService = new DefaultConversionService();
        DefaultConversionService.addDefaultConverters(conversionService);
        conversionService.addConverter(new Converter<String, LocalDate>() {
            @Override
            public LocalDate convert(String text) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
                    return LocalDate.parse(text, formatter);
                } catch (RuntimeException re) {
                    return null;
                }
            }
        });
        return conversionService;
    }

    @Bean
    FlatFileItemReader<InData> reader() {
        FlatFileItemReader<InData> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("input.csv"));

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("name", "surname", "birthDate");

        BeanWrapperFieldSetMapper<InData> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(InData.class);
        mapper.setConversionService(conversionService());

        DefaultLineMapper<InData> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    AgeProcessor processor() {
        return new AgeProcessor();
    }

    @Bean
    FlatFileItemWriter<OutData> writer() {
        BeanWrapperFieldExtractor<OutData> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] {"name", "surname", "age"});

        DelimitedLineAggregator<OutData> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<OutData> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(aggregator);

        return writer;
    }

    @Bean
    Step ageCalc(ItemReader<InData> reader,
                 ItemProcessor<InData, OutData> processor,
                 ItemWriter<OutData> writer) {

        return stepBuilderFactory.get("ageCalc")
                .<InData, OutData>chunk(100)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    Job changePriceJob(Step priceChange) {
        return jobBuilderFactory.get("ageCalcJob")
                .incrementer(new RunIdIncrementer())
                .flow(priceChange)
                .end()
                .build();
    }
}
