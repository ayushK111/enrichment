package com.optum.propel.enrichment.model;

import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PesRequest {

    @NotEmpty(message = "ProviderId is a required field")
    private String providerId;
    private String tin;
    @NotEmpty(message = "Type is a required field")
    private String type;
    private String locationId;
    private String platformName;

}
