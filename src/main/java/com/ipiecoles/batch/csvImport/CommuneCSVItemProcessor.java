package com.ipiecoles.batch.csvImport;

import com.ipiecoles.batch.dto.CommuneDto;
import com.ipiecoles.batch.exception.CommuneCSVException;
import com.ipiecoles.batch.model.Commune;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.item.ItemProcessor;

public class CommuneCSVItemProcessor implements ItemProcessor<CommuneDto, Commune> {

    private Integer nbCommunesWithoutCoordinates = 0;

    @Override
    public Commune process(CommuneDto item) throws Exception{
        Commune commune = new Commune();
        validateCommuneDto(item);
        commune.setCodeInsee(item.getCodeInsee());
        commune.setCodePostal(item.getCodePostal());
        commune.setNom(WordUtils.capitalizeFully(item.getNomCommune()));

        String nomCommune = WordUtils.capitalizeFully(item.getNomCommune());
        nomCommune = nomCommune.replaceAll("^L ", "L'");
        nomCommune = nomCommune.replaceAll(" L ", "L'");
        nomCommune = nomCommune.replaceAll("^D ", "D'");
        nomCommune = nomCommune.replaceAll(" D ", "D'");
        nomCommune = nomCommune.replaceAll("^St ", "Saint'");
        nomCommune = nomCommune.replaceAll(" St ", "Saint'");
        nomCommune = nomCommune.replaceAll("^Ste ", "Sainte'");
        nomCommune = nomCommune.replaceAll(" Ste ", "Sainte'");
        commune.setNom(nomCommune);

        // Latitude et Longitude

        String[] coordonnees = item.getCoordonneesGPS().split(",");
        if(coordonnees.length == 2){
            commune.setLatitude(Double.valueOf(coordonnees[0]));
            commune.setLongitude(Double.valueOf(coordonnees[1]));
        }

        System.out.println("commune vaut : " +
                commune.getCodeInsee() + " " +
                commune.getNom() + " " +
                commune.getCodePostal() + " " +
                commune.getLatitude() + "," + commune.getLongitude()
        );
        return commune;
    }

    private void validateCommuneDto(CommuneDto item) throws CommuneCSVException{
        //Contrôler Code INSEE 5 chiffres
        if (item.getCodeInsee() != null && !item.getCodeInsee().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Insee ne contient pas 5 chiffres");
        }
        //Contrôler Code postal 5 chiffres
        if (item.getCodePostal() != null && !item.getCodePostal().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code postal ne contient pas 5 chiffres");
        }
        //Contrôler nom de la communes lettres en majuscules, espaces, tirets, et apostrophes
        if (item.getNomCommune() != null && !item.getNomCommune().matches("^[A-Z-' ]+$")){
            throw new CommuneCSVException("La commune n'est pas composée uniquement de lettres");
        }
        //Contrôler les coordonnées GPS
        if (item.getCoordonneesGPS() != null && !item.getCoordonneesGPS().matches("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")){
            throw new CommuneCSVException("Les coordonnées GPS ne sont pas bonnes");
        }
    }

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("After Step CSV Import");
        logger.info(stepExecution.getJobExecution().getExecutionContext().getString("MSG"));
        logger.info(stepExecution.getSummary());
        if(nbCommunesWithoutCoordinates > 0){
            return new ExitStatus("COMPLETED_WITH_MISSING_COORDINATES");
        }
        return ExitStatus.COMPLETED;
    }

    @BeforeProcess
    public void beforeProcess(CommuneDto input){
        logger.info("Before Process => " + input.toString());
    }

    @AfterProcess
    public void afterProcess(CommuneDto input, Commune output){
        logger.info("After Process => " + input.toString() + " => " + output.toString());
    }

    @OnProcessError
    public void onProcessError(CommuneDto input, Exception ex){
        logger.error("Error Process => " + input.toString() + " => " + ex.getMessage());
    }

}
