package com.optum.propel.enrichment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.optum.propel.pes.model.combined.Record;
import com.optum.propel.pes.model.contracts.Communityandstate;
import com.optum.propel.pes.model.contracts.Employerandindividual;
import com.optum.propel.pes.model.contracts.Medicareandretirement;

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
public class PesResponse {

    private List<@Valid Record> records;
    private Metadata metadata;
    private ServiceFault serviceFault;
    private List<@Valid Employerandindividual> employerandindividual;
    private List<@Valid Medicareandretirement> medicareandretirement;
    private List<@Valid Communityandstate> communityandstate;
}
