package com.optum.propel.enrichment.services;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.validation.Valid;

import com.optum.propel.enrichment.model.*;
import com.optum.propel.pes.model.combined.TaxId;
import com.optum.propel.pes.model.roster.Records;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.optum.propel.enrichment.config.AppProperties;
import com.optum.propel.pes.model.combined.Record;
import com.optum.propel.pes.model.contracts.Communityandstate;
import com.optum.propel.pes.model.contracts.Employerandindividual;
import com.optum.propel.pes.model.contracts.Medicareandretirement;
import com.optum.provider.contract.ndb.model.CosmosPanel;
import com.optum.provider.contract.ndb.model.FacetsContract;
import com.optum.provider.contract.ndb.model.UnetContract;
import com.optum.provider.demographic.commons.model.PractitionerRole;
import com.optum.provider.demographic.commons.model.ProviderOrganization;
import com.optum.shared.platforms.common.model.Address;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

/**
 * Service Orchestrator class that execute the given request through a set of
 * workflow steps
 *
 * @Service marked this as a component and a bean will be created on init.
 *
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */
@Log4j2
@Service
@AllArgsConstructor
public class ServiceOrchestrator {

	/**
	 * Execute the given request through set of work steps
	 * <p>
	 * All exceptions should be caught here and a final response must be returned
	 * from here - Positive & Negative
	 *
	 * @param input
	 * @return
	 */

	ObjectMapper mapper;
	LocationService locationService;
	TokenService tokenService;
	ContractService contractService;
	TransformService transformService;
	AddressLookupService addressLookupService;
	CulturalCompService culturalCompService;
	AOEService aoeService;
	private final AppProperties appProperties;

	public ProviderSourceTruthData process(PesRequest pesRequest) throws JsonMappingException, JsonProcessingException {

		ProviderSourceTruthData providerSourceTruthData = null;
		final long start = System.currentTimeMillis();
		List<ErrorDetail> errorList = new ArrayList<>();
		List<com.optum.propel.pes.model.addresslookup.Record> countyRecords = null;

		if (ObjectUtils.isNotEmpty(pesRequest)) {

			/***
			 * Implementing Parallel,non-blocking and asynchronous calling using Completable
			 * future Interface from Java8
			 *
			 ***/


			log.info("!------ Calling LocationAPI------------------!");
			CompletableFuture<List<Record>> locationFuture
					= CompletableFuture.supplyAsync(() -> locationService.enrichLocationData(pesRequest, tokenService.getToken()));

			CompletableFuture<ProviderSourceTruthData> allContractFuture
					= CompletableFuture.supplyAsync(() -> getAllContracts(pesRequest));

			if (StringUtils.isNotBlank(pesRequest.getLocationId())) {
				CompletableFuture<List<com.optum.propel.pes.model.addresslookup.Record>> addressData = addressLookupService.enrichAddressLookupData(pesRequest, tokenService.getToken());
				try {
					countyRecords = new ArrayList<>(addressData.get().stream().collect(Collectors.toList()));
				} catch (InterruptedException | ExecutionException e) {
					log.warn("InterruptedException: {}", e);
					Thread.currentThread().interrupt();
				}
			}

			try {
				providerSourceTruthData = allContractFuture.get();
			} catch (InterruptedException | ExecutionException e) {
				log.warn("InterruptedException: {}", e);
				Thread.currentThread().interrupt();
			}

			List<Record> records = null;

			/*** collecting data from all the API's while blocking the main thread ***/
			try {
				records = new ArrayList<>(locationFuture.get().stream().collect(Collectors.toList()));

				log.info("Total Records for Location" + records.size());
			} catch (Exception e) {
				log.error("{}", e.getCause());

			}

			if (CollectionUtils.isNotEmpty(records ))
				transformService.mapLocation(providerSourceTruthData, records, pesRequest.getType());
			else {
				ErrorDetail errorDetail = new ErrorDetail();

				errorDetail.setMessage("No records found");
				errorDetail.setSource("Location");
				errorDetail.setStatusCode("006");
				errorList.add(errorDetail);
			}


			if (CollectionUtils.isNotEmpty(countyRecords)) {
				List<Address> addressDatas = transformService.mapCountyRecords(countyRecords);
				providerSourceTruthData.setAddresses(addressDatas);
			} else {
				ErrorDetail errorDetail = new ErrorDetail();

				errorDetail.setMessage("No records found");
				errorDetail.setSource("AddressLookup");
				errorDetail.setStatusCode("006");
				errorList.add(errorDetail);
			}


			if (AppConstants.PROVIDER_TYPE_PRACTITIONER.equalsIgnoreCase(pesRequest.getType()) && CollectionUtils.isNotEmpty(records) && ObjectUtils.isNotEmpty(providerSourceTruthData)) {
				List<Record> mpinlocationData = null;
				Record pesRecord = records.stream().filter(Objects::nonNull).findFirst().orElse(null);
				TaxId pesTaxId = pesRecord.getPTAData().getTaxId().stream().filter(Objects::nonNull).findFirst().orElse(null);
				pesRequest.setProviderId(pesTaxId.getPayeProvId());
				pesRequest.setTin(pesRequest.getTin());
				if (StringUtils.isNotBlank(pesRequest.getProviderId())) {
					mpinlocationData = locationService.enrichLocationData(pesRequest, tokenService.getToken());
				}
				Record mpinlocData = mpinlocationData.stream().filter(Objects::nonNull).findFirst().orElse(null);
				if (CollectionUtils.isNotEmpty(mpinlocationData)) {
					Record ptiProviderIdRecord = mpinlocData;
					ProviderOrganization providerOrganization = transformService.mapPtiProviderIdOrganization(ptiProviderIdRecord);
					providerSourceTruthData.setProviderOrganization(providerOrganization);
				}

			}


		}
		log.info("Took {} milliseconds to retrieve pes response", System.currentTimeMillis() - start);

		if (ObjectUtils.isNotEmpty(providerSourceTruthData) && CollectionUtils.isNotEmpty(errorList)) {
			if (Objects.isNull(providerSourceTruthData.getErrorList())) {
				List<ErrorDetail> newErrorList = new ArrayList<>();
				providerSourceTruthData.setErrorList(newErrorList);
			}
			providerSourceTruthData.getErrorList().addAll(errorList);
		}
		//		String providerSourceTruthDataString = mapper.writeValueAsString(providerSourceTruthData);
		//		providerSourceTruthData = mapper.readValue(providerSourceTruthDataString, ProviderSourceTruthData.class);
		return providerSourceTruthData;

	}

	public ProviderSourceTruthData getAllContracts(PesRequest pesRequest) {

		String token = tokenService.getToken();
		ProviderSourceTruthData providerSourceTruthData = null;
		final long start = System.currentTimeMillis();
		List<ErrorDetail> errorList = new ArrayList<>();

		if (ObjectUtils.isNotEmpty(pesRequest)) {
			providerSourceTruthData = new ProviderSourceTruthData();

			/***
			 * Implementing Parallel,non-blocking and asynchronous calling using Completable
			 * future Interface from Java8
			 *
			 ***/

			log.info("!------ Calling employerAndIndividual--------!");
			CompletableFuture<List<PesResponse>> pesResponseUnet = contractService
					.enrichContractData(pesRequest, token, appProperties.getPes().getUnetContract());


			log.info("!------ Calling medicareandretirement--------!");
			CompletableFuture<List<PesResponse>> pesResponseCosmos = contractService
					.enrichContractData(pesRequest, token, appProperties.getPes().getCosmosContract());

			log.info("!------ Calling communityandstate------------!");
			CompletableFuture<List<PesResponse>> pesResponseFacet = contractService
					.enrichContractData(pesRequest, token, appProperties.getPes().getFacetContract());
			List<Employerandindividual> employerandIndividual = null;
			List<Medicareandretirement> medicareandRetirement = null;
			List<Communityandstate> communityandstate = null;
			List<Record> records = null;
			try {
				/*** collecting data from all the API's while blocking the main thread ***/

				employerandIndividual = pesResponseUnet.get().stream()
						.filter(pesResp -> CollectionUtils.isNotEmpty(pesResp.getEmployerandindividual()))
						.flatMap(p -> p.getEmployerandindividual().stream()).collect(Collectors.toList());

				communityandstate = pesResponseFacet.get().stream().filter(
								pesResp -> CollectionUtils.isNotEmpty(pesResp.getCommunityandstate()))
						.flatMap(p -> p.getCommunityandstate().stream()).collect(Collectors.toList());

				medicareandRetirement = pesResponseCosmos.get().stream()
						.filter(pesResp -> CollectionUtils.isNotEmpty(pesResp.getMedicareandretirement()))
						.flatMap(p -> p.getMedicareandretirement().stream()).collect(Collectors.toList());


				log.info("Total Records For UnetContract: {}", employerandIndividual.size());
				log.info("Total Records For CosmosContract: {}", medicareandRetirement.size());
				log.info("Total Records for FacetContract: {}", communityandstate.size());


			} catch (Exception e) {
				log.error("{}", e.getCause());

			}

			log.info("Took {} milliseconds to retrieve pes response", System.currentTimeMillis() - start);

			if (CollectionUtils.isNotEmpty(employerandIndividual)) {
				List<UnetContract> unetContracts = transformService.mapUnet(employerandIndividual);
				providerSourceTruthData.setUnetContracts(unetContracts);
			} else {
				ErrorDetail errorDetail = new ErrorDetail();

				errorDetail.setMessage("No records found");
				errorDetail.setSource("UnetContract");
				errorDetail.setStatusCode("006");
				errorList.add(errorDetail);

			}
			if (CollectionUtils.isNotEmpty(medicareandRetirement )) {

				List<CosmosPanel> cosmosPanels = transformService.mapContract(medicareandRetirement);
				providerSourceTruthData.setCosmosPanels(cosmosPanels);
			} else {
				ErrorDetail errorDetail = new ErrorDetail();

				errorDetail.setMessage("No records found");
				errorDetail.setSource("CosmosContract");
				errorDetail.setStatusCode("006");
				errorList.add(errorDetail);
			}
			if (CollectionUtils.isNotEmpty(communityandstate)) {
				List<FacetsContract> facetContracts = transformService.mapFacet(communityandstate);

				providerSourceTruthData.setFacetsContracts(facetContracts);
			} else {
				ErrorDetail errorDetail = new ErrorDetail();

				errorDetail.setMessage("No records found");
				errorDetail.setSource("FacetContract");
				errorDetail.setStatusCode("006");
				errorList.add(errorDetail);
			}


		}

		if (ObjectUtils.isNotEmpty(providerSourceTruthData) && CollectionUtils.isNotEmpty(errorList)) {
			if (Objects.isNull(providerSourceTruthData.getErrorList())) {
				List<ErrorDetail> newErrorList = new ArrayList<>();
				providerSourceTruthData.setErrorList(newErrorList);
			}
			providerSourceTruthData.getErrorList().addAll(errorList);
		}
		return providerSourceTruthData;

	}


	public List<PractitionerRole> getAllTins(PesRequest pesRequest) {
		List<ErrorDetail> errorList = new ArrayList<>();
		log.info("!------ Calling Location API ----------------!");
		CompletableFuture<List<Record>> locationFuture = CompletableFuture.supplyAsync(() -> locationService.enrichLocationData(pesRequest, tokenService.getToken()));

		List<Record> records = null;

		/*** collecting data from all the API's while blocking the main thread ***/
		try {
			records = new ArrayList<>(locationFuture.get().stream().collect(Collectors.toList()));

			log.info("Total Records for Location: {}", records.size());
		} catch (Exception e) {
			log.error("{}", e.getCause());
		}

		List<PractitionerRole> practitionerRoleList = null;
		if (CollectionUtils.isNotEmpty(records ))
			practitionerRoleList = transformService.mapTinsOfProvider(records);
		else {
			ErrorDetail errorDetail = new ErrorDetail();

			errorDetail.setMessage("No records found");
			errorDetail.setSource("Location");
			errorDetail.setStatusCode("006");
			errorList.add(errorDetail);
		}


		return practitionerRoleList;
	}

	public List<AOEPesResponse> getAoeCodes(AOEPesRequest aoePesRequest) {
		List<AOEPesResponse> pesResponseAOE = null;
		if (ObjectUtils.isNotEmpty(aoePesRequest)) {
			pesResponseAOE =
					aoeService.enrichAoeData(aoePesRequest, tokenService.getToken());
		}
		return pesResponseAOE;
	}

	public CulturalCompetencyResponse getCulturalCompentency(CulturalCompetencyRequest culturalCompetencyRequest) {

		List<Records> record = culturalCompService.enrichCulturalComp(culturalCompetencyRequest, tokenService.getToken());

		CulturalCompetencyResponse errorResponse= new CulturalCompetencyResponse();
		if (CollectionUtils.isNotEmpty(record)) {
			CulturalCompetencyResponse response = transformService.mapCulturalCompetency(record);
			return response;

		}
		else {
			ErrorDetail errorDetail = new ErrorDetail();
			errorDetail.setMessage("No records found");
			errorDetail.setSource("Cultural Competency");
			errorDetail.setStatusCode("006");
			errorResponse.setErrorMessage(errorDetail);
		}

		return errorResponse;

	}
	//	private Record getPtiProviderIdRecord(PesRequest pesRequest, Record record) throws InterruptedException, ExecutionException {
	//		CompletableFuture<List<Record>> mpinlocationData = null;
	//
	//		log.info("Querying Location Data By PTI MPIN");
	//		pesRequest.setProviderId(record.getPTAData().getTaxId().get(0).getPayeProvId());
	//		if (!pesRequest.getProviderId().isEmpty()) {
	//			mpinlocationData = locationService.enrichLocationData(pesRequest, tokenService.getToken());
	//		}
	//
	//		return mpinlocationData.get().get(0);
	//	}
	public ProviderSourceTruthData getCountyInfo(@Valid PesRequest pesRequest) {

		ProviderSourceTruthData providerSourceTruthData = null;
		List<ErrorDetail> errorList = new ArrayList<>();

		List<com.optum.propel.pes.model.addresslookup.Record> countyRecords=null;
		if(ObjectUtils.isNotEmpty(pesRequest) && StringUtils.isNotBlank(pesRequest.getLocationId())){
			providerSourceTruthData = new ProviderSourceTruthData();
			CompletableFuture<List<com.optum.propel.pes.model.addresslookup.Record>> addressData = addressLookupService.enrichAddressLookupData(pesRequest,tokenService.getToken());
			try {
				countyRecords = new ArrayList<>(addressData.get().stream().collect(Collectors.toList()));
			} catch (InterruptedException | ExecutionException e) {
				log.warn("InterruptedException: {}", e);
				Thread.currentThread().interrupt();
			}
		}

		if(CollectionUtils.isNotEmpty(countyRecords)) {
			List<Address> addressDatas=transformService.mapCountyRecords(countyRecords);
			providerSourceTruthData.setAddresses(addressDatas);
		}
		else {
			ErrorDetail errorDetail = new ErrorDetail();

			errorDetail.setMessage("No records found");
			errorDetail.setSource("AddressLookup");
			errorDetail.setStatusCode("006");
			errorList.add(errorDetail);
		}

		if(ObjectUtils.isNotEmpty(providerSourceTruthData) && CollectionUtils.isNotEmpty(errorList)) {
			if(Objects.isNull(providerSourceTruthData.getErrorList())) {
				List<ErrorDetail> newErrorList = new ArrayList<>();
				providerSourceTruthData.setErrorList(newErrorList);
			}
			providerSourceTruthData.getErrorList().addAll(errorList);
		}

		return providerSourceTruthData;
	}
}
