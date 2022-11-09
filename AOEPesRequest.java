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
public class AOEPesRequest {

	@NotEmpty(message = "ProviderId is a required field")
	private String providerId;
	@NotEmpty(message = "taxId is a required field")
	private String taxId;
	private String areaOfExpertiseTypeCode;
	private String applicationName;
	private Integer pageOffset;


}
