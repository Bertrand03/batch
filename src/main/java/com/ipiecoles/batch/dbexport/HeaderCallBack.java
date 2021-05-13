package com.ipiecoles.batch.dbexport;

import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.Writer;

public class HeaderCallBack implements FlatFileHeaderCallback {
    @Autowired
    public CommuneRepository communeRepository;

    public HeaderCallBack(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }

    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write("Total codes postaux: " + communeRepository.countDistinctCodePostal());
    }
}
