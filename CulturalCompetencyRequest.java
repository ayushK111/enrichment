package com.optum.propel.enrichment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

import javax.validation.constraints.NotEmpty;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CulturalCompetencyRequest {
    @NotEmpty(message = "ProviderId is a required field")
    private String providerId;
    @NotEmpty(message = "taxId is a required field")
    private String tin;
    private String appName;
    private String attributeSet;
    private String adrSeq;

}
