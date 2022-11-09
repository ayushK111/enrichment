package com.optum.propel.enrichment.model;

import java.util.List;

import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.optum.propel.pes.model.addresslookup.Record;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AOECodes {

	@JsonProperty("activeCode")
	private String activeCode;
	@JsonProperty("addressId")
	private String addressId;
	@JsonProperty("addressTypeCode")
	private String addressTypeCode;
	@JsonProperty("areaOfExpertiseTypeCode")
	private String areaOfExpertiseTypeCode;
	@JsonProperty("areaOfExpertiseClassTypeCode")
	private String areaOfExpertiseClassTypeCode;
	@JsonProperty("areaOfExpertiseTypeDescription")
	private String areaOfExpertiseTypeDescription;
	@JsonProperty("areaOfExpertiseClassTypeDescription")
	private String areaOfExpertiseClassTypeDescription;
	@JsonProperty("areaOfExpertiseProviderTypeCode")
	private String areaOfExpertiseProviderTypeCode;
	@JsonProperty("contractOrganizationCode")
	private String contractOrganizationCode;
	@JsonProperty("corporateBusinessSegmentCode")
	private String corporateBusinessSegmentCode;
	@JsonProperty("directoryIndicator")
	private String directoryIndicator;
	@JsonProperty("providerId")
	private String providerId;
	@JsonProperty("taxId")
	private String taxId;
	@JsonProperty("taxIdTypeCode")
	private String taxIdTypeCode;

}
