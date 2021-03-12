package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.model.Commune;
import com.ipiecoles.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommuneMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception{
        Map<String, Double> coordinateOSM = OpenStreetMapUtils.getInstance().getCoordinates(item.getNom() + " " + item.getCodePostal());

        if (coordinateOSM != null && coordinateOSM.size() == 2) {
            item.setLongitude(coordinateOSM.get("lon"));
            item.setLatitude(coordinateOSM.get("lat"));

            return item;
        }
        return null;
    }
}
