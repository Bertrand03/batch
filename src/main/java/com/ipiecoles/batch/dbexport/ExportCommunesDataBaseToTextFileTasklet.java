package com.ipiecoles.batch.dbexport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class ExportCommunesDataBaseToTextFileTasklet implements Tasklet {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        logger.info("Lance la tasklet ExportCommunesDataBaseToTextFile");
        return RepeatStatus.FINISHED;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) throws Exception {
        //Avant l'exécution de la Step
        logger.info("Avant le lancement de tasklet ExportCommunesDataBaseToTextFile");
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) throws Exception {
        //Une fois la Step
        logger.info("Apres le lancement de tasklet ExportCommunesDataBaseToTextFile");
        logger.info(stepExecution.getSummary());
        return ExitStatus.COMPLETED;
    }
}
