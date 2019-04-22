package batch;

import batch.entity.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;

@EnableBatchProcessing
@SpringBootApplication
public class BatchDemoApplication {

    public static void main(String[] args) {
        System.setProperty("input", "file://" +
                        new File("/Users/brizhatov/Desktop/batch_cv_to_db/src/main/resources/in.csv"));

        System.setProperty("output", "file://" +
                new File("/Users/brizhatov/Desktop/batch_cv_to_db/src/main/resources/out.csv"));

        SpringApplication.run(BatchDemoApplication.class, args);
    }

    @Bean
    FlatFileItemReader<Person> fileReader(@Value("${input}") Resource in) {
        return new FlatFileItemReaderBuilder<Person>()
                .name("file-reader")
                .resource(in)
                .targetType(Person.class)
                .delimited().delimiter(",").names(new String[]{"firstName", "age", "email"})
                .build();
    }

    @Bean
    JdbcBatchItemWriter<Person> jdbcWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("insert into \"People\" (age, first_name, email) values (:age, :firstName, :email)")
                .beanMapped()
                .build();
    }

    @Bean
    Job job(JobBuilderFactory jobFactory,
            StepBuilderFactory stepFactory,
            ItemReader<Person> itemReader,
            ItemWriter<Person> itemWriter) {

        Step step = stepFactory.get("file-db")
                .<Person, Person>chunk(100)
                .reader(itemReader)
                .writer(itemWriter)
                .build();

        return jobFactory.get("etl")
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }
}
