package com.optum.propel.enrichment.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.optum.provider.contract.ndb.model.CosmosPanel;
import com.optum.provider.contract.ndb.model.FacetsContract;
import com.optum.provider.contract.ndb.model.UnetContract;
import com.optum.provider.credential.common.model.CredentialStatus;
import com.optum.provider.credential.common.model.CredentialingEntity;
import com.optum.provider.demographic.commons.model.OrganizationAffiliation;
import com.optum.provider.demographic.commons.model.Practitioner;
import com.optum.provider.demographic.commons.model.PractitionerRole;
import com.optum.provider.demographic.commons.model.ProviderLocation;
import com.optum.provider.demographic.commons.model.ProviderOrganization;
import com.optum.shared.platforms.common.model.Address;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)

/*** ProviderSourceTruthData contains all the fields being used to map to CDM ***/

public class ProviderSourceTruthData {

	//below all are provider Level
	private Practitioner practitioner;	//for provider
	private PractitionerRole practitionerRole;	// for provider

	private ProviderOrganization providerOrganization;	//for org
	private List<OrganizationAffiliation> organizationAffiliations;	//for org

	private List<ProviderLocation> providerLocations; //for both provider and org
	private CredentialStatus credentialStatus;	//for both provider and org
	private List<CredentialingEntity> credentialingEntities;	//for both provider and org
	private List<UnetContract> unetContracts;	//for both provider and org
	private List<FacetsContract> facetsContracts;	//for both provider and org
	private List<CosmosPanel> cosmosPanels;	//for both provider and org	
	private List<Address> addresses;
	private List<ErrorDetail> errorList;  //for both provider and org

}