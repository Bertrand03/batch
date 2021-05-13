package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.repository.CommuneRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.FormatterLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import javax.persistence.EntityManagerFactory;


@Configuration
@EnableBatchProcessing
public class CommunesDBExportBatch {
    @Value("10")
    private Integer chunkSize;

    @Autowired
    public EntityManagerFactory entityManagerFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public CommuneRepository communeRepository;

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Bean
    public Tasklet exportCommunesDataBaseToTextFileTasklet(){
        return new ExportCommunesDataBaseToTextFileTasklet();
    }

    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(Step stepExportFile){
        logger.info("*** Est passé dans exportCommunes ***");
        return jobBuilderFactory.get("exportCommunes")
                .incrementer(new RunIdIncrementer())
                .flow(stepExportDBToFileTasklet())
                .next(stepExportFile)
                .end().build();
    }

    // 1ere Step de mon JOB
    @Bean
    public Step stepExportDBToFileTasklet(){
        logger.info("*** Est passé dans stepExportDBToFileTasklet ***");
        return stepBuilderFactory.get("stepExportDBToFileTasklet")
                .tasklet(exportCommunesDataBaseToTextFileTasklet())
                .listener(exportCommunesDataBaseToTextFileTasklet())
                .build();
    }

    // 2e Step de mon JOB
    @Bean
    public Step stepExportFile(){
        logger.info("*** Est passé dans stepExportFile ***");
        return stepBuilderFactory.get("stepExportFile")
                .<Commune, Commune> chunk(chunkSize)
                .reader(readerBDD())
                .writer(writeInFile())
                .faultTolerant()
                .skip(FlatFileParseException.class)
                .listener(communesDBExportSkipListener())
                .build();
    }

    @Bean
    public JpaPagingItemReader<Commune> readerBDD() {
        logger.info("*** Passe dans readerBDD ***");
        return new JpaPagingItemReaderBuilder<Commune>()
                .name("readerBDD")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(10)
                .queryString("from Commune c order by code_postal, code_insee")
                .build();
    }

    @Bean
    public ItemWriter<Commune> writeInFile() {
        logger.info("*** Passe dans writeInFile ***");
        BeanWrapperFieldExtractor<Commune> beanWrapper = new BeanWrapperFieldExtractor<Commune>();
        beanWrapper.setNames(new String[] {"codePostal", "codeInsee", "nom", "latitude", "longitude"});

        FormatterLineAggregator<Commune> agg = new FormatterLineAggregator<>();
        agg.setFormat("%5s - %5s - %s : %.5f %.5f");
        agg.setFieldExtractor(beanWrapper);

        FlatFileItemWriter<Commune> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setName("fileWriter");
        flatFileItemWriter.setResource(new FileSystemResource("target/test.txt"));
        flatFileItemWriter.setLineAggregator(agg);
        flatFileItemWriter.setHeaderCallback(new HeaderCallBack(communeRepository));
        flatFileItemWriter.setFooterCallback(new FooterCallBack(communeRepository));

        return flatFileItemWriter;
    }

    @Bean
    public CommunesDBExportSkipListener communesDBExportSkipListener() {
        return new CommunesDBExportSkipListener();
    }

}


