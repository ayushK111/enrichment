package com.optum.propel.enrichment.model;

import java.util.List;

import javax.validation.Valid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.optum.propel.pes.model.addresslookup.Record;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)

public class AddressLookupResponse {

    private List<@Valid Record> records;
    private Metadata metadata;
    private ServiceFault serviceFault;


}
