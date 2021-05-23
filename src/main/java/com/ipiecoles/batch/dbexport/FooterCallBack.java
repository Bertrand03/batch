package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.Writer;

public class FooterCallBack implements FlatFileFooterCallback {
    //private final long count;

    @Autowired
    public CommuneRepository communeRepository;

    public FooterCallBack(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    @Override
    public void writeFooter(Writer writer) throws IOException {
        writer.write("Total communes : " + communeRepository.countDistinctAllCommunes());
    }
}
