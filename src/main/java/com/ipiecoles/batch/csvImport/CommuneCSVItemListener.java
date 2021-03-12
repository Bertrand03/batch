package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.model.Commune;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;

import java.util.List;

public class CommuneCSVItemListener implements ItemReadListener<CommuneDto>, ItemWriteListener<Commune> {

    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public void beforeRead() {
        logger.info("Before Read CSV Import");
    }

    @Override
    public void afterRead(CommuneDto item) {
        logger.info("After Read CSV Import => " + item.toString());
    }

    @Override
    public void onReadError(Exception e) {
        logger.info("On Read CSV Import => " + e.getMessage());
    }

    @Override
    public void beforeWrite(List<? extends Commune> items) {
        logger.info("Before Write CSV Import => " + items.toString());
    }

    @Override
    public void afterWrite(List<? extends Commune> items) {
        logger.info("After Write CSV Import => " + items.toString());

    }

    @Override
    public void onWriteError(Exception e, List<? extends Commune> items) {
        logger.info("On Write Error CSV Import => " + items.toString());

    }
}
