package com.optum.propel.enrichment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.optum.propel.pes.model.roster.Cultural_Competency;
import com.optum.propel.pes.model.roster.Records;
import lombok.Getter;
import lombok.Setter;
import javax.validation.Valid;
import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CulturalCompetencyResponse {

   private List<Cultural_Competency> cultural_competency;
   private ErrorDetail errorMessage;

}
