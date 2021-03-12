package com.ipiecoles.batch.dbexport;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
@Configuration
public class CommunesDBExportBatch {


    @Bean
    @Qualifier("exportCommunes")
    public Job exportCommunes(){
        return null;
    }

}
