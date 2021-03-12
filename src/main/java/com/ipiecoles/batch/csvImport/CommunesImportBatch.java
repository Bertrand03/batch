package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.exception.NetworkException;
import com.ipiecoles.batch.model.Commune;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.retry.backoff.FixedBackOffPolicy;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class CommunesImportBatch {

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public EntityManagerFactory entityManagerFactory;


    @Value("${communesImportBatch.chunksize}")
    private Integer chunksize;

    public Integer getChunksize() {
        return chunksize;
    }

    public void setChunksize(Integer chunksize) {
        this.chunksize = chunksize;
    }

    // Instanciations @Bean pour les classes
    @Bean
    public CommuneCSVItemProcessor communeCSVItemProcessor(){
        return new CommuneCSVItemProcessor();
    }

    @Bean
    public CommuneCSVImportStepListener communeCSVImportStepListener() {
        return new CommuneCSVImportStepListener();
    }
//    @Bean
//    public CommuneCSVException communeCSVException(){
//        return new CommuneCSVException();
//    }

    // MES STEPS
    @Bean
    public Step stepHelloWorld() {
        return stepBuilderFactory.get("stepHelloWorld")
                .tasklet(helloWorldTasklet())
                .listener(helloWorldTasklet())
                .build();
    }

    @Bean
    public Tasklet helloWorldTasklet() {
        return new MyTasklet();
    }

    @Bean
    public Job importCsvJob(Step stepHelloWorld, Step stepImportCSV, Step stepGetMissingCoordinates){
        return jobBuilderFactory.get("importCsvJob")
                .incrementer(new RunIdIncrementer())
                .flow(stepHelloWorld)
                .next(stepImportCSV)
                .on("COMPLETED_WITH_MISSING_COORDINATES").to(stepGetMissingCoordinates)
                .end().build();
    }


    @Bean
    public Step stepGetMissingCoordinates(){
        FixedBackOffPolicy policy = new FixedBackOffPolicy();
        policy.setBackOffPeriod(2000);
        return stepBuilderFactory.get("getMissingCoordiantes")
                .<Commune, Commune> chunk(10)
                .reader(communeMissingCoordinatesJpaItemReader())
                .processor(communeMissingCoordinatesItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .retryLimit(5)
                .retry(NetworkException.class)
                .backOffPolicy(policy)
                .build();
    }

    @Bean
    public CommuneMissingCoordinatesItemProcessor communeMissingCoordinatesItemProcessor(){
        return new CommuneMissingCoordinatesItemProcessor();
    }

    @Bean
    public JpaPagingItemReader<Commune> communeMissingCoordinatesJpaItemReader() {
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("communeMissingOrdinateJpaItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c where c.latitude is null or c.longitude is null")
                .build();
    }

    @Bean
    public StepExecutionListener communeCSVImportTestListener(){
        return new CommuneCSVImportStepListener();
    }

    @Bean
    public ChunkListener communeCSVImportChunkListener(){
        return new CommuneCSVImportChunkListener();
    }

    @Bean
    public SkipListener<CommuneDto, Commune> communesCSVImportSkipListener(){
        return new CommunesCSVImportSkipListener();
    }

    @Bean
    public ItemReadListener<CommuneDto> communeCSVItemReadListener(){
        return new CommuneCSVItemListener();
    }
    @Bean
    public ItemWriteListener<Commune> communeCSVItemWriteListener(){
        return new CommuneCSVItemListener();
    }

    @Bean
    public Step stepImportCSV(JdbcBatchItemWriter<Commune> writerJDBC) {
        return stepBuilderFactory.get("stepImportCSV")
                .<CommuneDto, Commune> chunk(getChunksize())
                .reader(myCSVReader())
                .processor(communeCSVItemProcessor())
                .writer(writerJPA())
                .faultTolerant()
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .skip(CommuneCSVException.class)
                .skip(FlatFileParseException.class)
                .listener(communesCSVImportSkipListener())
//                .listener(communeCSVImportTestListener())
//                .listener(communeCSVImportChunkListener())
//                .listener(communeCSVItemReadListener())
                .listener(communeCSVItemWriteListener())
                .listener(communeCSVItemProcessor())
                .build();
    }

    @Bean
    public FlatFileItemReader<CommuneDto> myCSVReader(){
        return new FlatFileItemReaderBuilder<CommuneDto>()
                .name("myCSVReader").linesToSkip(1)
                .resource(new ClassPathResource("laposte_hexasmal_test_skip.csv"))
                .delimited().delimiter(";")
                .names("codeInsee", "nomCommune", "codePostal", "ligne5", "libelleAcheminement", "coordonneesGPS")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<CommuneDto>(){{
                    setTargetType(CommuneDto.class);
                }})
                .build();
    }


    // Partie Writer (soit JPA, soit JDBC, perfomances differentes selon type traitement)

    @Bean
    public JpaItemWriter<Commune> writerJPA(){
        return new JpaItemWriterBuilder<Commune>().entityManagerFactory(entityManagerFactory).build();
    }

       @Bean
    public JdbcBatchItemWriter<Commune> writerJDBC(DataSource dataSource){
        System.out.println("Entre dans le writerJDBC");
        return new JdbcBatchItemWriterBuilder<Commune>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO commune (codeInsee, nom, codePostal, latitude, longitude) "
                        + "VALUES (:codeInsee, :nom, :codePostal, :latitude, :longitude) as c "
                        + "ON DUPLICATE KEY UPDATE nom=c.nom, code_postal=c.code_postal, latitude=c.latitude, longitude=c.longitude")
                .dataSource(dataSource)
                .build();
    }

}
