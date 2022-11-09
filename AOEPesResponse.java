package com.optum.propel.enrichment.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.optum.propel.pes.model.areaofexpertise.Data;
import com.optum.propel.pes.model.areaofexpertise.Meta;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.Valid;
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AOEPesResponse {

	@JsonProperty("meta")
	private Meta meta;
	@JsonProperty("data")
	private List<@Valid Data> data = null;

}
