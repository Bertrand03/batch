package com.ipiecoles.batch.dto;


public class CommuneDto {
    private String codeInsee;
    private String nomCommune;
    private String codePostal;
    private String ligne5;
    private String libelleAcheminement;
    private String coordonneesGPS;

    public String getCodeInsee() {
        return codeInsee;
    }

    public void setCodeInsee(String codeInsee) {
        this.codeInsee = codeInsee;
    }

    public String getNomCommune() {
        return nomCommune;
    }

    public void setNomCommune(String nomCommune) {
        this.nomCommune = nomCommune;
    }

    public String getCodePostal() {
        return codePostal;
    }

    public void setCodePostal(String codePostal) {
        this.codePostal = codePostal;
    }

    public String getLigne5() {
        return ligne5;
    }

    public void setLigne5(String ligne5) {
        this.ligne5 = ligne5;
    }

    public String getLibelleAcheminement() {
        return libelleAcheminement;
    }

    public void setLibelleAcheminement(String libelleAcheminement) {
        this.libelleAcheminement = libelleAcheminement;
    }

    public String getCoordonneesGPS() {
        return coordonneesGPS;
    }

    public void setCoordonneesGPS(String coordonneesGPS) {
        this.coordonneesGPS = coordonneesGPS;
    }

    public CommuneDto(){}

    @Override
    public String toString() {
        return "CommuneDto{" +
                "codeInsee='" + codeInsee + '\'' +
                ", nomCommune='" + nomCommune + '\'' +
                ", codePostal='" + codePostal + '\'' +
                ", ligne5='" + ligne5 + '\'' +
                ", libelleAcheminement='" + libelleAcheminement + '\'' +
                ", coordonneesGPS='" + coordonneesGPS + '\'' +
                '}';
    }
}
