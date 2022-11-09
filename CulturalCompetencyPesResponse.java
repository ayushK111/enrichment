package com.optum.propel.enrichment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.optum.propel.pes.model.roster.Cultural_Competency;
import com.optum.propel.pes.model.roster.Records;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.Valid;
import java.util.List;


@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CulturalCompetencyPesResponse {
   private List<@Valid Records> records;
   private Metadata metadata;
   private ServiceFault serviceFault;


}
