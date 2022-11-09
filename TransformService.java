package com.optum.propel.enrichment.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.optum.constants.CorrespondenceIndicator;
import com.optum.constants.ElectronicCommunicationType;
import com.optum.constants.LevelType;
import com.optum.constants.LocationPVOType;
import com.optum.constants.LocationType;
import com.optum.constants.PhoneType;
import com.optum.constants.PrimaryIndicator;
import com.optum.constants.States;
import com.optum.constants.TaxIdType;
import com.optum.constants.TelehealthPatientType;
import com.optum.constants.TelehealthScheduleType;
import com.optum.constants.TelehealthType;
import com.optum.propel.enrichment.model.CulturalCompetencyResponse;
import com.optum.propel.enrichment.model.ErrorDetail;
import com.optum.propel.enrichment.model.ProviderSourceTruthData;
import com.optum.propel.pes.model.combined.DocumentKeyDate;
import com.optum.propel.pes.model.combined.Organization;
import com.optum.propel.pes.model.combined.ProvHoldCode;
import com.optum.propel.pes.model.combined.Provider;
import com.optum.propel.pes.model.combined.Record;
import com.optum.propel.pes.model.combined.TaxId;
import com.optum.propel.pes.model.contracts.Communityandstate;
import com.optum.propel.pes.model.contracts.Employerandindividual;
import com.optum.propel.pes.model.contracts.Medicareandretirement;
import com.optum.propel.pes.model.roster.Cultural_Competency;
import com.optum.propel.pes.model.roster.Records;
import com.optum.provider.contract.ndb.model.CosmosPanel;
import com.optum.provider.contract.ndb.model.FacetsContract;
import com.optum.provider.contract.ndb.model.UnetContract;
import com.optum.provider.credential.common.model.CredentialStatus;
import com.optum.provider.credential.common.model.CredentialingEntity;
import com.optum.provider.demographic.commons.model.HospitalAffiliation;
import com.optum.provider.demographic.commons.model.MedicareId;
import com.optum.provider.demographic.commons.model.NPI;
import com.optum.provider.demographic.commons.model.OrganizationAffiliation;
import com.optum.provider.demographic.commons.model.Practitioner;
import com.optum.provider.demographic.commons.model.PractitionerDegree;
import com.optum.provider.demographic.commons.model.PractitionerRole;
import com.optum.provider.demographic.commons.model.ProviderLocation;
import com.optum.provider.demographic.commons.model.ProviderLocationPhone;
import com.optum.provider.demographic.commons.model.ProviderLocationVerification;
import com.optum.provider.demographic.commons.model.ProviderOrganization;
import com.optum.provider.demographic.commons.model.Telehealth;
import com.optum.provider.demographic.ndb.model.BusinessSegmentAddressSequence;
import com.optum.provider.demographic.ndb.model.CosmosDemographics;
import com.optum.provider.demographic.ndb.model.ProviderHoldCode;
import com.optum.provider.demographic.ndb.model.ProviderLocationContractOrg;
import com.optum.provider.demographic.ndb.model.ProviderSpecialtyContractOrg;
import com.optum.shared.platforms.common.model.Address;
import com.optum.shared.platforms.common.model.ElectronicCommunication;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class TransformService {

    static final DateTimeFormatter DTF = EnrichmentUtil.DTF;
    @Autowired
    ObjectMapper mapper;

    /***
     * Mapping Data got from location API to CDM
     *
     * @throws JsonProcessingException
     * @throws JsonMappingException
     ***/
    public ProviderSourceTruthData mapLocation(ProviderSourceTruthData providerSourceTruthData, List<Record> pesData,
                                               String type) {

        List<OrganizationAffiliation> organizationAffiliations;
        List<CredentialingEntity> credentialingEntities = new ArrayList<>();
        List<ProviderLocation> providerLocations = new ArrayList<>();
        List<ProviderHoldCode> providerHoldCodes = new ArrayList<>();
        List<ProviderSpecialtyContractOrg> providerSpecialtyContractOrgs = new ArrayList<>();
        List<HospitalAffiliation> hospitalAffiliations = new ArrayList<>();
        List<MedicareId> medicareIds = new ArrayList<>();
        List<NPI> npis = new ArrayList<>();
        List<PractitionerDegree> degrees = new ArrayList<>();

        Record pesRecord = pesData.stream()
                .filter(Objects::nonNull)
                .findFirst().orElse(null);

        TaxId taxId = pesRecord.getPTAData().getTaxId()
                .stream()
                .filter(Objects::nonNull)
                .findFirst().orElse(null);

        DocumentKeyDate keyDate = pesRecord.getPTAData().getDocumentKeyDates()
                .stream()
                .filter(Objects::nonNull)
                .findFirst().orElse(null);

        Provider provider = pesRecord.getPTAData().getProvider()
                .stream()
                .filter(Objects::nonNull)
                .findFirst().orElse(null);

        com.optum.propel.pes.model.combined.CredentialStatus credStatus = pesRecord.getPTAData().getCredentialStatus()
                .stream()
                .filter(Objects::nonNull)
                .flatMap(credenStatus -> credenStatus.stream()
                        .filter(Objects::nonNull))
                .findFirst().orElse(null);

        if (AppConstants.PROVIDER_TYPE_PRACTITIONER.equalsIgnoreCase(type)) {

            Practitioner practitioner = new Practitioner();
            if (StringUtils.isNotBlank(pesRecord.getPTAData().getProvId())) {
                practitioner.setPractitionerId(pesRecord.getPTAData().getProvId());
            }
            try {
                if (StringUtils.isNotBlank(provider.getProvEffDt())) {
                    practitioner.setStartDate(LocalDate.parse(provider.getProvEffDt()));
                }
            } catch (Exception e) {
                log.warn("Problem Parsing provider.getProvEffDt: {}", e.getMessage());
            }

            if (StringUtils.isNotBlank(provider.getProvCancDt())) {
                try {
                    practitioner.setEndDate(LocalDate.parse(provider.getProvCancDt()));
                } catch (Exception e) {
                    log.warn("Problem Parsing provider.getProvCancDt: {}", e.getMessage());
                }
                practitioner.setActive(EnrichmentUtil.isActive(provider.getProvCancDt()));
            }


            //Mapping PractionerRole from PES
            PractitionerRole practitionerRole = new PractitionerRole();
            if (ObjectUtils.isNotEmpty(taxId)) {
                if (StringUtils.isNotBlank(taxId.getPayTypCd())) {
                    practitionerRole.setAffiliationType(taxId.getPayTypCd());
                }
                if (StringUtils.isNotBlank(taxId.getContrPaprTypCd())) {
                    practitionerRole.setContractDocumentType(taxId.getContrPaprTypCd());
                }
            }
            try {
                if (StringUtils.isNotBlank(keyDate.getTinEffDt())) {
                    practitionerRole.setStartDate(
                            LocalDate.parse(keyDate.getTinEffDt(), DTF));
                }
            } catch (Exception e) {
                log.warn("Problem Parsing getDocumentKeyDates.getTinEffDt: {}", e.getMessage());
            }
            if (StringUtils.isNotBlank(keyDate.getTinCancDt())) {
                practitionerRole.setActive(EnrichmentUtil.isActive(keyDate.getTinCancDt()));
                try {
                    practitionerRole.setEndDate(LocalDate.parse(keyDate.getTinCancDt(), DTF));
                } catch (Exception e) {
                    log.warn("Problem Parsing getDocumentKeyDates.getTinCancDt: {}", e.getMessage());
                }

            }
            if (StringUtils.isNotBlank(pesRecord.getPTAData().getTaxIdNbr())) {
                practitionerRole.setTaxId(pesRecord.getPTAData().getTaxIdNbr());
            }
            if (AppConstants.TAX_ID_TYP_TAX.equalsIgnoreCase(pesRecord.getPTAData().getTaxIdTypCd())) {
                practitionerRole.setTaxIdType(TaxIdType.TAX);
            } else if (AppConstants.TAX_ID_TYPE_SSN.equalsIgnoreCase(pesRecord.getPTAData().getTaxIdTypCd())) {
                practitionerRole.setTaxIdType(TaxIdType.SSN);
            } else {
                practitionerRole.setTaxIdType(TaxIdType.NONE);
            }
            providerSourceTruthData.setPractitionerRole(practitionerRole);
            //END of Mapping PractitionerRole from PES

            //Mapping ProvHoldCode from PES
            ProvHoldCode holdCode = pesRecord.getPTAData().getProvHoldCode()
                    .stream()
                    .filter(Objects::nonNull)
                    .flatMap(holdCodes1 -> holdCodes1.stream()
                            .filter(Objects::nonNull))
                    .findFirst().orElse(null);

            if (holdCode.isNotNullObject()) {
                ProviderHoldCode providerHoldCode = new ProviderHoldCode();
                providerHoldCode.setHoldCode(StringUtils.isNotBlank(holdCode.getProcXcptTypCd()) ? holdCode.getProcXcptTypCd() : null);
                try {
                    if (StringUtils.isNotBlank(holdCode.getProcEffDt())) {
                        providerHoldCode.setStartDate(LocalDate.parse(holdCode.getProcEffDt(), DTF));
                    }
                } catch (Exception e) {
                    log.warn("Problem Parsing holdCode.getProcEffDt: {}", e.getMessage());
                }
                if (StringUtils.isNotBlank(holdCode.getProcCancDt())) {
                    providerHoldCode.setActive(EnrichmentUtil.isActive(holdCode.getProcCancDt()));
                    try {
                        providerHoldCode.setEndDate(LocalDate.parse(holdCode.getProcCancDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem Parsing holdCode.getProcCancDt: {}", e.getMessage());
                    }
                }
                if (!providerHoldCodes.contains(providerHoldCode) &&
                        StringUtils.isNotBlank(providerHoldCode.getHoldCode())) {
                    providerHoldCodes.add(providerHoldCode);
                }
            }
            practitioner.setHoldCodes(CollectionUtils.isNotEmpty(providerHoldCodes) ? providerHoldCodes : null);
            //END of Mapping ProvHoldCode from PES

            //Mapping SpecialityContractOrg from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getSpecialtyContractOrg())) {
                pesRecord.getPTAData().getSpecialtyContractOrg().forEach(contractOrgs -> {
                    contractOrgs.forEach(contractOrg -> {
                        try {
                            ProviderSpecialtyContractOrg providerSpecialtyContractOrg = new ProviderSpecialtyContractOrg();
                            providerSpecialtyContractOrg.setSpecialtyCode(
                                    StringUtils.isNotBlank(contractOrg.getSpclTypCd()) ? contractOrg.getSpclTypCd() : null);
                            providerSpecialtyContractOrg.setContractOrg(
                                    StringUtils.isNotBlank(contractOrg.getContrOrgCd()) ? contractOrg.getContrOrgCd() : null);
                            if (AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(contractOrg.getSpclPriCd())) {
                                providerSpecialtyContractOrg.setPrimary(PrimaryIndicator.PRIMARY);
                            } else if (AppConstants.SECONDARY_INDICATOR.equalsIgnoreCase(contractOrg.getSpclPriCd())) {
                                providerSpecialtyContractOrg.setPrimary(PrimaryIndicator.SECONDARY);
                            } else {
                                providerSpecialtyContractOrg.setPrimary(PrimaryIndicator.NONE);
                            }
                            if (StringUtils.isNotBlank(contractOrg.getSpclTypFullDesc())) {
                                providerSpecialtyContractOrg.setSpecialtyDescription(contractOrg.getSpclTypFullDesc());
                            }
                            try {
                                if (StringUtils.isNotBlank(contractOrg.getSpclEffDt())) {
                                    providerSpecialtyContractOrg.setStartDate(LocalDate.parse(contractOrg.getSpclEffDt(), DTF));
                                }
                            } catch (Exception e) {
                                log.warn("Problem Parsing contractOrg.getSpclEffDt: {}", e.getMessage());
                            }
                            if (StringUtils.isNotBlank(contractOrg.getSpclCancDt())) {
                                providerSpecialtyContractOrg.setActive(EnrichmentUtil.isContractActive(contractOrg.getSpclCancDt()));
                                try {
                                    providerSpecialtyContractOrg.setEndDate(LocalDate.parse(contractOrg.getSpclCancDt(), DTF));
                                } catch (Exception e) {
                                    log.warn("Problem Parsing contractOrg.getSpclCancDt: {}", e.getMessage());
                                }
                            }
                            if (!providerSpecialtyContractOrgs.contains(providerSpecialtyContractOrg)
                                    && StringUtils.isNotBlank(providerSpecialtyContractOrg.getSpecialtyCode())) {
                                providerSpecialtyContractOrgs.add(providerSpecialtyContractOrg);
                            }
                        } catch (Exception e) {
                            log.warn("Problem Parsing ProviderSpecialtyContractOrg: {}", e.getMessage());
                        }
                    });

                });
            }
            practitioner.setProviderSpecialtyContractOrgs(
                    CollectionUtils.isNotEmpty(providerSpecialtyContractOrgs) ? providerSpecialtyContractOrgs : null);
            //END of SpecialityContractOrg from PES

            //Mapping Medicare ID from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getMedicare())) {
                pesRecord.getPTAData().getMedicare().forEach(medicares -> {
                    medicares.forEach(medicare -> {
                        if (ObjectUtils.isNotEmpty(medicare.getMcrId())) {
                            MedicareId medicareId = new MedicareId();
                            if (StringUtils.isNotBlank(medicare.getMcrId())) {
                                medicareId.setMedicareId(medicare.getMcrId());
                            }
                            medicareId.setActive(AppConstants.ACTIVE_CODE.
                                    equalsIgnoreCase(medicare.getMcrActvCd()) ? true : false);
                            if (!medicareIds.contains(medicareId) &&
                                    ObjectUtils.isNotEmpty(medicareId)) {
                                medicareIds.add(medicareId);
                            }
                        }
                    });
                });
            }
            practitioner.setMedicareIds(CollectionUtils.isNotEmpty(medicareIds) ? medicareIds : null);
            //END of Medicare ID from PES

            //Mapping NPI from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getNpi())) {
                pesRecord.getPTAData().getNpi().forEach(pesNpis -> {
                    pesNpis.forEach(pesNpi -> {
                        try {
                            if (ObjectUtils.isNotEmpty(pesNpi)) {
                                NPI npi = new NPI();
                                if (StringUtils.isNotBlank(pesNpi.getNpiId())) {
                                    npi.setNpi(pesNpi.getNpiId());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiTxnmCd())) {
                                    npi.setTaxonomy(pesNpi.getNpiTxnmCd());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiLvlCd())) {
                                    npi.setLevelTypeCode(pesNpi.getNpiLvlCd());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getDataSrcTypCd())) {
                                    npi.setDataSourceTypeCode(pesNpi.getDataSrcTypCd());
                                }
                                try {
                                    if (StringUtils.isNotBlank(pesNpi.getNpiEffDt())) {
                                        npi.setStartDate(LocalDate.parse(pesNpi.getNpiEffDt()));
                                    }
                                } catch (Exception e) {
                                    log.warn("Problem Parsing pesNpi.getNpiEffDt: {}", e.getMessage());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiCancDt())) {
                                    npi.setActive(EnrichmentUtil.isActive(pesNpi.getNpiCancDt()));
                                    try {
                                        npi.setEndDate(LocalDate.parse(pesNpi.getNpiCancDt()));

                                    } catch (Exception e) {
                                        log.warn("Problem Parsing pesNpi.getNpiCancDt: {}", e.getMessage());
                                    }
                                }
                                if (!npis.contains(npi) &&
                                        ObjectUtils.isNotEmpty(npi)) {
                                    npis.add(npi);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Problem Parsing NPI: {}", e.getMessage());
                        }
                    });
                });
            }
            practitioner.setNpis(CollectionUtils.isNotEmpty(npis) ? npis : null);
            //END of Mapping NPI from PES

            //Mapping Practitioner Degree from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getDegree())) {
                pesRecord.getPTAData().getDegree().forEach(pesDegrees -> {
                    pesDegrees.forEach(pesDegree -> {
                        try {
                            if (ObjectUtils.isNotEmpty(pesDegree)) {
                                PractitionerDegree degree = new PractitionerDegree();
                                degree.setActive(AppConstants.ACTIVE_CODE.equalsIgnoreCase(
                                        pesDegree.getDegActvCd()) ? true : false);
                                if (StringUtils.isNotBlank(pesDegree.getDegCd())) {
                                    degree.setDegreeCode(pesDegree.getDegCd());
                                }
                                if (StringUtils.isNotBlank(pesDegree.getDegCdDesc())) {
                                    degree.setDegreeName(pesDegree.getDegCdDesc());
                                }
                                degree.setPrimary(AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(
                                        pesDegree.getDegPriCd()) ? PrimaryIndicator.PRIMARY : PrimaryIndicator.SECONDARY);
                                if (!degrees.contains(degree) &&
                                        ObjectUtils.isNotEmpty(degree)) {
                                    degrees.add(degree);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Problem Parsing Degree: {}", e.getMessage());
                        }
                    });
                });
            }
            practitioner.setDegrees(CollectionUtils.isNotEmpty(degrees) ? degrees : null);
            //End of Mapping of Practitioner Degree from PES

            //Mapping HospitalAffiliations from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getHospitalAffiliations())) {
                pesRecord.getPTAData().getHospitalAffiliations().forEach(pesHospitalAffiliations -> {
                    pesHospitalAffiliations.forEach(pesHospitalAffiliation -> {
                        com.optum.propel.pes.model.combined.HospitalAffiliation newPesHospitalAffiliation = new com.optum.propel.pes.model.combined.HospitalAffiliation();
                        try {
                            if (!pesHospitalAffiliation.equals(newPesHospitalAffiliation)) {
                                HospitalAffiliation hospitalAffiliation = new HospitalAffiliation();
                                hospitalAffiliation.setHospitalId(StringUtils.isNotBlank(pesHospitalAffiliation.getAffilProvId())
                                        ? pesHospitalAffiliation.getAffilProvId() : null);
                                hospitalAffiliation
                                        .setAdmittingPrivileges(StringUtils.isNotBlank(pesHospitalAffiliation.getAdmitPrvlgTypCd())
                                                ? pesHospitalAffiliation.getAdmitPrvlgTypCd()
                                                : null);
                                hospitalAffiliation.setActive(Boolean.TRUE);
                                if (AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(pesHospitalAffiliation.getPriHospAffilCd())) {
                                    hospitalAffiliation.setPrimary(PrimaryIndicator.PRIMARY);
                                } else if (AppConstants.SECONDARY_INDICATOR.equalsIgnoreCase(pesHospitalAffiliation.getPriHospAffilCd())) {
                                    hospitalAffiliation.setPrimary(PrimaryIndicator.SECONDARY);
                                } else {
                                    hospitalAffiliation.setPrimary(PrimaryIndicator.NONE);
                                }
                                if (!hospitalAffiliations.contains(hospitalAffiliation)
                                        && ObjectUtils.isNotEmpty(hospitalAffiliation)) {
                                    hospitalAffiliations.add(hospitalAffiliation);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Problem Parsing HospitalAffiliation: {}", e.getMessage());
                        }
                    });

                });
            }
            practitioner.setHospitals(CollectionUtils.isNotEmpty(hospitalAffiliations) ? hospitalAffiliations : null);
            //END of Mapping HospitalAffliations from PES

            providerSourceTruthData.setPractitioner(practitioner);

        } else if (AppConstants.PROVIDER_TYPE_ORGANISATION.equalsIgnoreCase(type)) {
            ProviderOrganization providerOrganization = new ProviderOrganization();
            organizationAffiliations = new ArrayList<>();
            Organization org = pesRecord.getPTAData().getOrganization()
                    .stream()
                    .filter(Objects::nonNull)
                    .flatMap(orgs -> orgs.stream()
                            .filter(Objects::nonNull))
                    .findFirst().orElse(null);
            try {
                if (StringUtils.isNotBlank(org.getOrgTypCd())) {
                    providerOrganization.setOrganizationType(org.getOrgTypCd());
                }
            } catch (Exception e) {
                log.warn("Problem Parsing getOrganization.getOrgTypCd: {}", e.getMessage());
            }
            try {
                if (StringUtils.isNotBlank(keyDate.getTinEffDt())) {
                    providerOrganization.setStartDate(LocalDate.parse(keyDate.getTinEffDt(), DTF));
                }
            } catch (Exception e) {
                log.warn("Problem Parsing getDocumentKeyDates.getTinEffDt: {}", e.getMessage());
            }
            if (StringUtils.isNotBlank(keyDate.getAdrCancDt())) {
                providerOrganization.setActive(EnrichmentUtil.isActive(keyDate.getAdrCancDt()));
                try {
                    providerOrganization.setEndDate(LocalDate.parse(keyDate.getAdrCancDt(), DTF));
                } catch (Exception e) {
                    log.warn("Problem Parsing getDocumentKeyDates.getAdrCancDt: {}", e.getMessage());
                }

            }
            providerSourceTruthData.setProviderOrganization(providerOrganization);


            //Mapping OrganizationAffiliation from PES
            OrganizationAffiliation organizationAffiliation = new OrganizationAffiliation();
            if (StringUtils.isNotBlank(taxId.getPayTypCd())) {
                organizationAffiliation.setAffiliationType(taxId.getPayTypCd());
            }
            try {
                if (StringUtils.isNotBlank(keyDate.getTinEffDt())) {
                    organizationAffiliation.setStartDate(LocalDate.parse(keyDate.getTinEffDt(), DTF));
                }
            } catch (Exception e) {
                log.warn("Problem Parsing getDocumentKeyDates.getTinEffDt: {}", e.getMessage());
            }
            if (StringUtils.isNotBlank(keyDate.getTinCancDt())) {
                organizationAffiliation.setActive(EnrichmentUtil.isActive(keyDate.getTinCancDt()));
                try {
                    organizationAffiliation.setEndDate(LocalDate.parse(keyDate.getTinCancDt(), DTF));
                } catch (Exception e) {
                    log.warn("Problem Parsing getDocumentKeyDates.getTinCancDt: {}", e.getMessage());
                }
            }

            if (StringUtils.isNotBlank(pesRecord.getPTAData().getTaxIdNbr())) {
                organizationAffiliation.setTaxId(pesRecord.getPTAData().getTaxIdNbr());
            }
            if (AppConstants.TAX_ID_TYP_TAX.equalsIgnoreCase(pesRecord.getPTAData().getTaxIdTypCd())) {
                organizationAffiliation.setTaxIdType(TaxIdType.TAX);
            } else if (AppConstants.TAX_ID_TYPE_SSN.equalsIgnoreCase(pesRecord.getPTAData().getTaxIdTypCd())) {
                organizationAffiliation.setTaxIdType(TaxIdType.SSN);
            } else {
                organizationAffiliation.setTaxIdType(TaxIdType.NONE);
            }
            if (StringUtils.isNotBlank(taxId.getContrPaprTypCd())) {
                organizationAffiliation.setContractDocumentType(taxId.getContrPaprTypCd());
            }
            if (!organizationAffiliations.contains(organizationAffiliation) &&
                    ObjectUtils.isNotEmpty(organizationAffiliation)) {
                organizationAffiliations.add(organizationAffiliation);
            }
            providerSourceTruthData.setOrganizationAffiliations(organizationAffiliations);
            //END of Mapping OrganizationAffiliation from PES

            //Mapping NPI from PES
            if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getNpi())) {
                pesRecord.getPTAData().getNpi().forEach(pesNpis -> {
                    pesNpis.forEach(pesNpi -> {
                        try {
                            if (ObjectUtils.isNotEmpty(pesNpi)) {
                                NPI npi = new NPI();
                                if (StringUtils.isNotBlank(pesNpi.getNpiId())) {
                                    npi.setNpi(pesNpi.getNpiId());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiTxnmCd())) {
                                    npi.setTaxonomy(pesNpi.getNpiTxnmCd());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiLvlCd())) {
                                    npi.setLevelTypeCode(pesNpi.getNpiLvlCd());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getDataSrcTypCd())) {
                                    npi.setDataSourceTypeCode(pesNpi.getDataSrcTypCd());
                                }
                                try {
                                    if (StringUtils.isNotBlank(pesNpi.getNpiEffDt())) {
                                        npi.setStartDate(LocalDate.parse(pesNpi.getNpiEffDt()));
                                    }
                                } catch (Exception e) {
                                    log.warn("Problem Parsing pesNpi.getNpiEffDt: {}", e.getMessage());
                                }
                                if (StringUtils.isNotBlank(pesNpi.getNpiCancDt())) {
                                    npi.setActive(EnrichmentUtil.isActive(pesNpi.getNpiCancDt()));
                                    try {
                                        npi.setEndDate(LocalDate.parse(pesNpi.getNpiCancDt()));

                                    } catch (Exception e) {
                                        log.warn("Problem Parsing pesNpi.getNpiCancDt: {}", e.getMessage());
                                    }
                                }
                                if (!npis.contains(npi) &&
                                        ObjectUtils.isNotEmpty(npi)) {
                                    npis.add(npi);
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Problem Parsing NPI: {}", e.getMessage());
                        }
                    });
                });
            }
            providerOrganization.setNpis(CollectionUtils.isNotEmpty(npis) ? npis : null);
            //END of Mapping NPI from PES

        }

        //Mapping CredentialStatus from PES
        CredentialStatus credentialStatus = new CredentialStatus();
        if (StringUtils.isNotBlank(credStatus.getCrdntlStsCd())) {
            credentialStatus.setStatusCode(credStatus.getCrdntlStsCd());
        }
        providerSourceTruthData.setCredentialStatus(StringUtils.isNotBlank(
                credentialStatus.getStatusCode()) ? credentialStatus : null);
        //END of Mapping CredentialStatus from PES

        //Mapping ProviderLocation from PES
        pesData.stream()
                .forEach(record -> {
                    record.getPTAData().getAddress().forEach(pesAddress -> {
                        ProviderLocation providerLocation = new ProviderLocation();
                        List<String> addressLines = new ArrayList<>();

                        if (ObjectUtils.isNotEmpty(pesAddress)) {
                            if (StringUtils.isNotBlank(pesAddress.getAdrId())) {
                                providerLocation.setLocationId(pesAddress.getAdrId());
                            }

                            if (Objects.nonNull(pesAddress.getAdrTypCd())) {
                                providerLocation.setLocationType(LocationType.valueOf(
                                        getAddressLocationType(pesAddress.getAdrTypCd())));
                            }
                            if (StringUtils.isNotBlank(pesAddress.getAdrLn1Txt())) {
                                addressLines.add(pesAddress.getAdrLn1Txt());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getExtAdrLn1Txt())) {
                                addressLines.add(pesAddress.getExtAdrLn1Txt());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getExtAdrLn2Txt())) {
                                addressLines.add(pesAddress.getExtAdrLn2Txt());
                            }
                            if (ObjectUtils.isNotEmpty(addressLines)) {
                                providerLocation.setAddressLine(addressLines);
                            }
                            if (StringUtils.isNotBlank(pesAddress.getCtyNm())) {
                                providerLocation.setCity(pesAddress.getCtyNm());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getCntyNm())) {
                                providerLocation.setCounty(pesAddress.getCntyNm());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getVanCtyNm())) {
                                Set<String> vanityCities = new HashSet<>();
                                vanityCities.add(pesAddress.getVanCtyNm());
                                providerLocation.setVanityCity(vanityCities);
                            }
                            try {
                                if (StringUtils.isNotBlank(pesAddress.getStCd())) {
                                    providerLocation.setState(States.fromValue(pesAddress.getStCd()));
                                }
                            } catch (Exception ex){
                                log.warn("Possibly encountered an international state that is not supported");
                                providerLocation.setState(States.NONE);
                            }
                            if (StringUtils.isNotBlank(pesAddress.getZipCd())) {
                                providerLocation.setZipCode(pesAddress.getZipCd());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getZipPls4Cd())) {
                                providerLocation.setZipCodePlus4(pesAddress.getZipPls4Cd());
                            }
                            if (AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(pesAddress.getAdrPriCd())) {
                                providerLocation.setPrimary(PrimaryIndicator.PRIMARY);
                            } else if (AppConstants.SECONDARY_INDICATOR.equalsIgnoreCase(pesAddress.getAdrPriCd())) {
                                providerLocation.setPrimary(PrimaryIndicator.SECONDARY);
                            } else {
                                providerLocation.setPrimary(PrimaryIndicator.NONE);
                            }
                            if (AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(pesAddress.getCorspAdrInd())) {
                                providerLocation.setCorrespondence(CorrespondenceIndicator.PRIMARY);
                            } else if (AppConstants.SECONDARY_INDICATOR.equalsIgnoreCase(pesAddress.getCorspAdrInd())) {
                                providerLocation.setCorrespondence(CorrespondenceIndicator.SECONDARY);
                            } else {
                                providerLocation.setCorrespondence(CorrespondenceIndicator.NONE);
                            }
                            if (StringUtils.isNotBlank(pesAddress.getBilAdrId())) {
                                providerLocation.setAssociatedLocationId(pesAddress.getBilAdrId());
                            }
                            if (AppConstants.ADR_LOCTYP_BILL.equalsIgnoreCase(pesAddress.getBilAdrTyp())) {
                                providerLocation.setAssociatedLocationType(LocationType.BILL);
                            } else if (AppConstants.ADR_LOCTYP_COMBO.equalsIgnoreCase(pesAddress.getBilAdrTyp())) {
                                providerLocation.setAssociatedLocationType(LocationType.COMBO);
                            } else {
                                providerLocation.setAssociatedLocationType(LocationType.NONE);
                            }
                            try {
                                if (StringUtils.isNotBlank(pesAddress.getAdrEffDt())) {
                                    providerLocation.setStartDate(LocalDate.parse(pesAddress.getAdrEffDt(), DTF));
                                }
                            } catch (Exception e) {
                                log.warn("Problem Parsing pesAddress.getAdrEffDt: {}", e.getMessage());
                            }
                            if (StringUtils.isNotBlank(pesAddress.getAdrCancDt())) {
                                providerLocation.setActive(EnrichmentUtil.isAddressActive(pesAddress.getAdrCancDt()));
                                try {
                                    providerLocation.setEndDate(LocalDate.parse(pesAddress.getAdrCancDt(), DTF));
                                } catch (Exception e) {
                                    log.warn("Problem Parsing pesAddress.getAdrCancDt: {}", e.getMessage());
                                }
                            }
                        }

                        pesData.stream()
                                .filter(thRecord -> thRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && thRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(thRecord -> {
                                    thRecord.getPTAData().getTelehealth().forEach(pesTelehealths -> {
                                        pesTelehealths.forEach(pesTelehealth -> {
                                            if (ObjectUtils.isNotEmpty(pesTelehealth)) {
                                                Telehealth telehealth = new Telehealth();
                                                if (StringUtils.isNotBlank(pesTelehealth.getThTypCd())) {
                                                    if ((AppConstants.VIDEO).equalsIgnoreCase(pesTelehealth.getThTypCd()))
                                                        telehealth.setTelehealthType(TelehealthType.AUDIO_VIDEO);
                                                    else if ((AppConstants.AUDIO).equalsIgnoreCase(pesTelehealth.getThTypCd())) {
                                                        telehealth.setTelehealthType(TelehealthType.AUDIO);
                                                    }
                                                } else {
                                                    telehealth.setTelehealthType(TelehealthType.NONE);
                                                }
                                                if (StringUtils.isNotBlank(pesTelehealth.getThPtntCd())) {
                                                    if ((AppConstants.BOTH).equalsIgnoreCase(pesTelehealth.getThPtntCd())) {
                                                        telehealth.setTelehealthPatientType(TelehealthPatientType.BOTH);
                                                    } else if ((AppConstants.EXISTING).equalsIgnoreCase(pesTelehealth.getThPtntCd())) {
                                                        telehealth.setTelehealthPatientType(TelehealthPatientType.EXISTING);
                                                    }
                                                } else {
                                                    telehealth.setTelehealthPatientType(TelehealthPatientType.NONE);
                                                }
                                                if (StringUtils.isNotBlank(pesTelehealth.getThSchedTypCd())) {
                                                    if (AppConstants.ON_DEMAND.equalsIgnoreCase(pesTelehealth.getThSchedTypCd())) {
                                                        telehealth.setTelehealthScheduleType(TelehealthScheduleType.ON_DEMAND);
                                                    } else if (AppConstants.SCHEDULED.equalsIgnoreCase(pesTelehealth.getThSchedTypCd())) {
                                                        telehealth.setTelehealthScheduleType(TelehealthScheduleType.SCHEDULED);
                                                    } else if (AppConstants.BOTH.equalsIgnoreCase(pesTelehealth.getThSchedTypCd())) {
                                                        telehealth.setTelehealthScheduleType(TelehealthScheduleType.BOTH);
                                                    }
                                                } else {
                                                    telehealth.setTelehealthScheduleType(TelehealthScheduleType.NONE);
                                                }
                                                if (StringUtils.isNotBlank(pesAddress.getAdrCancDt())) {
                                                    telehealth.setActive(EnrichmentUtil.isAddressActive(pesAddress.getAdrCancDt()));
                                                }
                                                if (ObjectUtils.isNotEmpty(telehealth))
                                                    providerLocation.setTelehealth(telehealth);
                                            }
                                        });
                                    });
                                });

                        List<ElectronicCommunication> electronicCommunications = new ArrayList<>();
                        pesData.stream()
                                .filter(ecRecord -> ecRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && ecRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(ecRecord -> {
                                    ecRecord.getPTAData().getElectronicCommunication().forEach(electronicComms -> {
                                        electronicComms.forEach(ec -> {
                                            try {
                                                ElectronicCommunication electronicCommunication = new ElectronicCommunication();
                                                if (ec.isNotNullObject()) {
                                                    if (AppConstants.WEBADDRESS.equalsIgnoreCase(ec.getElcCmnctTypCd())) {
                                                        electronicCommunication.setType(ElectronicCommunicationType.WEBADDRESS);
                                                    } else if (AppConstants.EMAIL.equalsIgnoreCase(ec.getElcCmnctTypCd())) {
                                                        electronicCommunication.setType(ElectronicCommunicationType.EMAIL);
                                                    } else if (AppConstants.PUBLIC_EMAIL.equalsIgnoreCase(ec.getElcCmnctTypCd())) {
                                                        electronicCommunication.setType(ElectronicCommunicationType.PUBLIC_EMAIL);
                                                    } else {
                                                        electronicCommunication.setType(ElectronicCommunicationType.NONE);
                                                    }
                                                    if (AppConstants.ADDRESS.equalsIgnoreCase(ec.getElcLvlCd())) {
                                                        electronicCommunication.setLevelTypeCode(LevelType.ADDRESS);
                                                    } else if (AppConstants.PROVIDER_TYPE_PRACTITIONER.equalsIgnoreCase(ec.getElcLvlCd())
                                                            || AppConstants.PROVIDER_TYPE_ORGANISATION.equalsIgnoreCase(ec.getElcLvlCd())) {
                                                        electronicCommunication.setLevelTypeCode(LevelType.PROVIDER);
                                                    } else {
                                                        electronicCommunication.setLevelTypeCode(LevelType.NONE);
                                                    }
                                                    if (StringUtils.isNotBlank(ec.getElcCmnctTxt())) {
                                                        electronicCommunication.setText(ec.getElcCmnctTxt());
                                                    }
                                                    electronicCommunication.setActive(AppConstants.ACTIVE_CODE.equalsIgnoreCase(
                                                            ec.getElcActvCd()) ? true : false);
                                                    if (!electronicCommunications.contains(electronicCommunication)
                                                            && ObjectUtils.isNotEmpty(electronicCommunication)) {
                                                        electronicCommunications.add(electronicCommunication);
                                                    }
                                                }
                                            } catch (Exception e) {
                                                log.warn("Problem Parsing ElectronicCommunication: {}", e.getMessage());
                                            }
                                        });
                                    });
                                });
                        providerLocation.setCommunications(CollectionUtils.isNotEmpty(
                                electronicCommunications) ? electronicCommunications : null);

                        List<ProviderLocationPhone> providerLocationPhones = new ArrayList<>();
                        pesData.stream()
                                .filter(phRecord -> phRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && phRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(phRecord -> {
                                    phRecord.getPTAData().getPhone().forEach(phones -> {
                                        phones.forEach(phone -> {
                                            try {
                                                ProviderLocationPhone providerLocationPhone = new ProviderLocationPhone();
                                                if (StringUtils.isNotBlank(phone.getTelUseTypCd())) {
                                                    providerLocationPhone.setPhoneType(PhoneType.valueOf(
                                                            getPhoneType(phone.getTelUseTypCd())));
                                                }
                                                if (StringUtils.isNotBlank(phone.getTelNbr())) {
                                                    providerLocationPhone.setPhoneNumber(phone.getTelNbr());
                                                }
                                                if (StringUtils.isNotBlank(phone.getTelAreaCd())) {
                                                    providerLocationPhone.setAreaCode(phone.getTelAreaCd());
                                                }
                                                if (StringUtils.isNotBlank(phone.getTelExtNbr())) {
                                                    providerLocationPhone.setExtension(phone.getTelExtNbr());
                                                }
                                                if (AppConstants.PRIMARY_INDICATOR.equalsIgnoreCase(phone.getTelPriCd())) {
                                                    providerLocationPhone.setPrimary(PrimaryIndicator.PRIMARY);
                                                } else if (AppConstants.SECONDARY_INDICATOR.equalsIgnoreCase(phone.getTelPriCd())) {
                                                    providerLocationPhone.setPrimary(PrimaryIndicator.SECONDARY);
                                                } else {
                                                    providerLocationPhone.setPrimary(PrimaryIndicator.NONE);
                                                }

                                                providerLocationPhone.setActive(AppConstants.ACTIVE_CODE.equalsIgnoreCase(
                                                        phone.getTelActvCd()) ? true : false);

                                                if (!providerLocationPhones.contains(providerLocationPhone) &&
                                                        ObjectUtils.isNotEmpty(providerLocationPhone)) {
                                                    providerLocationPhones.add(providerLocationPhone);
                                                }

                                            } catch (Exception e) {
                                                log.warn("Problem Parsing Phone: {}", e.getMessage());
                                            }
                                        });
                                    });
                                });
                        providerLocation.setPhones(CollectionUtils.isNotEmpty(providerLocationPhones) ? providerLocationPhones : null);

                        List<ProviderLocationVerification> providerLocationVerifications = new ArrayList<>();
                        pesData.stream()
                                .filter(pvRecord -> pvRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && pvRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(pvRecord -> {
                                    pvRecord.getPTAData().getPvoStatus().forEach(pvoStatusList -> {
                                        pvoStatusList.forEach(pvoStatus -> {
                                            try {
                                                ProviderLocationVerification providerLocationVerification = new ProviderLocationVerification();
                                                if (StringUtils.isNotBlank(pvoStatus.getPrvDmdDatTypId())) {
                                                    providerLocationVerification.setType(LocationPVOType.valueOf(
                                                            getPrvDmdDatTypId(pvoStatus.getPrvDmdDatTypId())));
                                                }
                                                if (StringUtils.isNotBlank(pvoStatus.getPvoVerfDt())) {
                                                    providerLocationVerification
                                                            .setVerificationDate(LocalDate.parse(pvoStatus.getPvoVerfDt(), DTF));
                                                }
                                                if (!providerLocationVerifications.contains(providerLocationVerification)
                                                        && ObjectUtils.isNotEmpty(providerLocationVerification.getType())) {
                                                    providerLocationVerifications.add(providerLocationVerification);
                                                }
                                            } catch (Exception e) {
                                                log.warn("Problem Parsing ProviderLocationVerification: {}", e.getMessage());
                                            }
                                        });
                                    });
                                });

                        providerLocation.setProviderLocationVerification(
                                CollectionUtils.isNotEmpty(providerLocationVerifications) ? providerLocationVerifications : null);

                        List<CosmosDemographics> cosmosDemographicsList = new ArrayList<>();
                        pesData.stream()
                                .filter(cdRecord -> cdRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && cdRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(cdRecord -> {
                                    cdRecord.getPTAData().getCosmosDemo().forEach(cosmosList -> {
                                        cosmosList.forEach(cosmos -> {
                                            try {
                                                CosmosDemographics cosmosDemographics = new CosmosDemographics();
                                                if (StringUtils.isNotBlank(cosmos.getCosProvNum())) {
                                                    cosmosDemographics.setProviderId(cosmos.getCosProvNum());
                                                }
                                                if (StringUtils.isNotBlank(cosmos.getCosProvType())) {
                                                    cosmosDemographics.setProviderType(cosmos.getCosProvType());
                                                }
                                                if (StringUtils.isNotBlank(cosmos.getCosDiv())) {
                                                    cosmosDemographics.setDiv(cosmos.getCosDiv());
                                                }
                                                try {
                                                    if (StringUtils.isNotBlank(cosmos.getCosEffDt())) {
                                                        cosmosDemographics.setStartDate(LocalDate.parse(cosmos.getCosEffDt(), DTF));
                                                    }
                                                } catch (Exception e) {
                                                    log.warn("Problem Parsing cosmos.getCosEffDt: {}", e.getMessage());
                                                }

                                                if (StringUtils.isNotBlank(cosmos.getCosCancDate())) {
                                                    cosmosDemographics.setActive(EnrichmentUtil.isActive(cosmos.getCosCancDate()));
                                                    try {
                                                        cosmosDemographics.setEndDate(LocalDate.parse(cosmos.getCosCancDate(), DTF));
                                                    } catch (Exception e) {
                                                        log.warn("Problem Parsing cosmos.getCosCancDate: {}", e.getMessage());
                                                    }
                                                }

                                                if (!cosmosDemographicsList.contains(cosmosDemographics)
                                                        && StringUtils.isNotBlank(cosmosDemographics.getProviderId())) {
                                                    cosmosDemographicsList.add(cosmosDemographics);
                                                }
                                            } catch (Exception e) {
                                                log.warn("Problem Parsing CosmosDemographics: {}", e.getMessage());
                                            }

                                        });
                                    });
                                });
                        providerLocation.setCosmosDemographics(CollectionUtils.isNotEmpty(cosmosDemographicsList) ? cosmosDemographicsList : null);

                        List<ProviderLocationContractOrg> providerLocationContractOrgs = new ArrayList<>();
                        pesData.stream()
                                .filter(contractRecord -> contractRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && contractRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(contractRecord -> {
                                    contractRecord.getPTAData().getAddressContractOrg().forEach(contractOrgs -> {
                                        contractOrgs.forEach(contractOrg -> {

                                            try {
                                                ProviderLocationContractOrg providerLocationontractOrg = new ProviderLocationContractOrg();
                                                if (StringUtils.isNotBlank(contractOrg.getAdrContrOrgCd())) {
                                                    providerLocationontractOrg.setContractOrg(contractOrg.getAdrContrOrgCd());
                                                }
                                                try {
                                                    if (StringUtils.isNotBlank(contractOrg.getAdrContrOrgEffDt())) {
                                                        providerLocationontractOrg
                                                                .setStartDate(LocalDate.parse(contractOrg.getAdrContrOrgEffDt(), DTF));
                                                    }
                                                } catch (Exception e) {
                                                    log.warn("Problem Parsing contractOrg.getAdrContrOrgEffDt: {}", e.getMessage());
                                                }

                                                if (StringUtils.isNotBlank(contractOrg.getAdrContrOrgCancDt())) {
                                                    providerLocationontractOrg.setActive(
                                                            EnrichmentUtil.isContractActive(contractOrg.getAdrContrOrgCancDt()));
                                                    try {
                                                        providerLocationontractOrg
                                                                .setEndDate(LocalDate.parse(contractOrg.getAdrContrOrgCancDt(), DTF));
                                                    } catch (Exception e) {
                                                        log.warn("Problem Parsing contractOrg.getAdrContrOrgCancDt: {}", e.getMessage());
                                                    }

                                                }

                                                if (!providerLocationContractOrgs.contains(providerLocationontractOrg) &&
                                                        ObjectUtils.isNotEmpty(providerLocationontractOrg)) {
                                                    providerLocationContractOrgs.add(providerLocationontractOrg);
                                                }
                                            } catch (Exception e) {
                                                log.warn("Problem Parsing ProviderLocationContractOrg: {}", e.getMessage());
                                            }
                                        });
                                    });
                                });

                        providerLocation.setProviderLocationContractOrgs(CollectionUtils
                                .isNotEmpty(providerLocationContractOrgs) ? providerLocationContractOrgs : null);

                        //Mapping BusinessSegmentAddressSequence from PES
                        List<BusinessSegmentAddressSequence> businessSegmentAddressSequences = new ArrayList<>();
                        pesData.stream()
                                .filter(bsRecord -> bsRecord.getPTAData().getAdrId().equalsIgnoreCase(pesAddress.getAdrId())
                                        && bsRecord.getPTAData().getAdrTypCd().equalsIgnoreCase(pesAddress.getAdrTypCd()))
                                .forEach(bsRecord -> {
                                    bsRecord.getPTAData().getFacets().forEach(facets -> {
                                        facets.forEach(facet -> {
                                            try {
                                                BusinessSegmentAddressSequence businessSegmentAddressSequence = new BusinessSegmentAddressSequence();
                                                businessSegmentAddressSequence.setAddressSequence(StringUtils.isNotBlank(facet.getBsarAdrRelSeqNbr()) ? Double.parseDouble(facet.getBsarAdrRelSeqNbr()) : null);
                                                businessSegmentAddressSequence.setInstanceCode(StringUtils.isNotBlank(facet.getBsarInsncCd()) ? facet.getBsarInsncCd() : null);
                                                businessSegmentAddressSequence.setStartDate(StringUtils.isNotBlank(facet.getBsarEffDt()) ? LocalDate.parse(facet.getBsarEffDt(), DTF) : null);
                                                businessSegmentAddressSequence.setEndDate(StringUtils.isNotBlank(facet.getBsarCancDt()) ? LocalDate.parse(facet.getBsarCancDt(), DTF) : null);
                                                if (!businessSegmentAddressSequences.contains(businessSegmentAddressSequence)
                                                        && businessSegmentAddressSequence.getEndDate() != null) {
                                                    businessSegmentAddressSequences.add(businessSegmentAddressSequence);
                                                }
                                            } catch (Exception e) {
                                                log.warn("Problem Parsing BusinessSegmentAddressSequence: {}", e.getMessage());
                                            }
                                        });
                                    });
                                });
                        providerLocation.setBusinessSegmentAddressSequences(CollectionUtils
                                .isNotEmpty(businessSegmentAddressSequences) ? businessSegmentAddressSequences : null);


                        if (ObjectUtils.isNotEmpty(providerLocation)) {
                            providerLocations.add(providerLocation);
                        }
                    });
                });
        //END of Mapping ProviderLocation from PES

        if (CollectionUtils.isNotEmpty(providerLocations)) {
            providerSourceTruthData.setProviderLocations(providerLocations);
        }

        if (CollectionUtils.isNotEmpty(pesRecord.getPTAData().getEntityData())) {
            pesRecord.getPTAData().getEntityData().forEach(entityList -> {
                entityList.forEach(entity -> {
                    try {
                        CredentialingEntity credentialingEntity = new CredentialingEntity();
                        if (ObjectUtils.isNotEmpty(entity.getEntyEffDt())) {
                            try {
                                credentialingEntity.setStartDate(LocalDate.parse(entity.getEntyEffDt(), DTF));
                            } catch (Exception e) {
                                log.warn("Problem parsing entity.getEntyEffDt: {}", e.getMessage());
                            }
                        }
                        if (ObjectUtils.isNotEmpty(entity.getEntyCancDt())) {
                            credentialingEntity.setActive(EnrichmentUtil.isActive(entity.getEntyCancDt()));
                            try {
                                credentialingEntity.setEndDate(LocalDate.parse(entity.getEntyCancDt(), DTF));
                            } catch (Exception e) {
                                log.warn("Problem parsing entity.getEntyCancDt: {}", e.getMessage());
                            }

                        }
                        if (ObjectUtils.isNotEmpty(entity.getEntyTyp())) {
                            credentialingEntity.setEntityType(entity.getEntyTyp());
                        }
                        if (ObjectUtils.isNotEmpty(entity.getEntyName())) {
                            credentialingEntity.setEntityName(entity.getEntyName());
                        }
                        if (ObjectUtils.isNotEmpty(entity.getEntyId())) {
                            credentialingEntity.setEntityId(entity.getEntyId());
                        }
                        if (!credentialingEntities.contains(credentialingEntity)
                                && StringUtils.isNotBlank(credentialingEntity.getEntityType())) {
                            credentialingEntities.add(credentialingEntity);
                        }
                    } catch (Exception e) {
                        log.warn("Problem Parsing CredentialingEntity: {}", e.getMessage());
                    }
                });
            });
        }
        providerSourceTruthData
                .setCredentialingEntities(CollectionUtils.isNotEmpty(credentialingEntities) ? credentialingEntities : null);

        return providerSourceTruthData;

    }

    /** Mapping Data from UNET Contract to CDM **/
    /**
     * @param dataForUnetContract
     * @return
     */
    public List<UnetContract> mapUnet(List<Employerandindividual> dataForUnetContract) {
        List<UnetContract> unetContracts = new ArrayList<>();

        dataForUnetContract.forEach(contract -> {
            try {
                UnetContract unetContract = new UnetContract();
                if (StringUtils.isNotBlank(contract.getPncTaxIdNbr())) {
                    unetContract.setTaxId(contract.getPncTaxIdNbr());
                }
                if (StringUtils.isNotBlank(contract.getPncMktNbr())) {
                    unetContract.setMarketNumber(contract.getPncMktNbr());
                }
                if (StringUtils.isNotBlank(contract.getPncContrTypCd())) {
                    unetContract.setContractType(contract.getPncContrTypCd());
                }
                if (StringUtils.isNotBlank(contract.getCdkContrOrgCd())) {
                    unetContract.setContractOrg(contract.getCdkContrOrgCd());
                }
                if (StringUtils.isNotBlank(contract.getCdkPrdctCatgyCd())) {
                    unetContract.setProductCategory(contract.getCdkPrdctCatgyCd());
                }
                if (StringUtils.isNotBlank(contract.getPncPrdctOfrId())) {
                    unetContract.setProductOfferId(contract.getPncPrdctOfrId());
                }
                if (StringUtils.isNotBlank(contract.getPncProvContrRoleCd())) {
                    unetContract.setRoleCode(contract.getPncProvContrRoleCd());
                }
                if (StringUtils.isNotBlank(contract.getPncEffDt())) {
                    try {
                        unetContract.setStartDate(LocalDate.parse(contract.getPncEffDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem parsing unetContract.getPncEffDt: {}", e.getMessage());
                    }
                }
                if (StringUtils.isNotBlank(contract.getPncCancDt())) {
                    unetContract.setActive(EnrichmentUtil.isContractActive(contract.getPncCancDt()));
                    try {
                        unetContract.setEndDate(LocalDate.parse(contract.getPncCancDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem parsing unetContract.getPncCancDt: {}", e.getMessage());
                    }

                }
                if (!unetContracts.contains(unetContract) && ObjectUtils.isNotEmpty(unetContract)) {
                    unetContracts.add(unetContract);
                }
            } catch (Exception e) {
                log.warn("Problem Parsing UnetContract: {}", e.getMessage());
            }
        });
        return unetContracts;
    }

    /** Mapping Data from Facet Contract to CDM **/
    /**
     * @param dataForFacetContract
     * @return
     */
    public List<FacetsContract> mapFacet(List<Communityandstate> dataForFacetContract) {
        List<FacetsContract> facetsContracts = new ArrayList<>();

        dataForFacetContract.forEach(contract -> {
            try {
                FacetsContract facetsContract = new FacetsContract();
                if (StringUtils.isNotBlank(contract.getTaxIdNbr())) {
                    facetsContract.setTaxId(contract.getTaxIdNbr());
                }
                if (StringUtils.isNotBlank(contract.getOvtnEffDt())) {
                    try {
                        facetsContract.setStartDate(LocalDate.parse(contract.getOvtnEffDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem parsing contract.getOvtnEffDt: {}", e.getMessage());
                    }
                }
                if (StringUtils.isNotBlank(contract.getOvtnCancDt())) {
                    facetsContract.setActive(EnrichmentUtil.isContractActive(contract.getOvtnCancDt()));
                    try {
                        facetsContract.setEndDate(LocalDate.parse(contract.getOvtnCancDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem parsing contract.getOvtnCancDt: {}", e.getMessage());
                    }
                }
                if (StringUtils.isNotBlank(contract.getOvtnContrId())) {
                    facetsContract.setAgreementId(contract.getOvtnContrId());
                }
                if(StringUtils.isNotBlank(contract.getOvtnPcpInd())) {
                    facetsContract.setPcpIndicator(contract.getOvtnPcpInd());
                }
                if (!facetsContracts.contains(facetsContract) && ObjectUtils.isNotEmpty(facetsContract)) {
                    facetsContracts.add(facetsContract);
                }
            } catch (Exception e) {
                log.warn("Problem Parsing FacetsContract: {}", e.getMessage());
            }
        });

        return facetsContracts;

    }

    /** Mapping Data from Cosmos Contract to CDM **/
    /**
     * @param dataForMedicareContract
     * @return
     */
    public List<CosmosPanel> mapContract(List<Medicareandretirement> dataForMedicareContract) {
        List<CosmosPanel> cosmosPanels = new ArrayList<>();

        dataForMedicareContract.forEach(contract -> {
            try {
                CosmosPanel cosmosPanel = new CosmosPanel();
                if (StringUtils.isNotBlank(contract.getTaxIdNbr())) {
                    cosmosPanel.setTaxId(contract.getTaxIdNbr());
                }
                if (StringUtils.isNotBlank(contract.getCosProvNum())) {
                    cosmosPanel.setProviderId(StringUtils.stripStart(contract.getCosProvNum(), AppConstants.ZERO));
                }
                if (StringUtils.isNotBlank(contract.getCosProvType())) {
                    cosmosPanel.setProviderType(contract.getCosProvType());
                }
                if (StringUtils.isNotBlank(contract.getCosDiv())) {
                    cosmosPanel.setDiv(contract.getCosDiv());
                }
                if (StringUtils.isNotBlank(contract.getCosStatusCode())) {
                    cosmosPanel.setStatusCode(contract.getCosStatusCode());
                }
                if (StringUtils.isNotBlank(contract.getCosPanelNum())) {
                    cosmosPanel.setPanel(contract.getCosPanelNum());
                }
                if (StringUtils.isNotBlank(contract.getCosRole())) {
                    cosmosPanel.setRoleCode(contract.getCosRole());
                }
                if (StringUtils.isNotBlank(contract.getCosEffDt())) {
                    try {
                        cosmosPanel.setStartDate(LocalDate.parse(contract.getCosEffDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem Parsing contract.getCosEffDate: {}", e.getMessage());
                    }
                }
                if (StringUtils.isNotBlank(contract.getCosCancDate())) {
                    cosmosPanel.setActive(EnrichmentUtil.isContractActive(contract.getCosCancDate()));
                    try {
                        cosmosPanel.setEndDate(LocalDate.parse(contract.getCosCancDate(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem Parsing contract.getCosCancDate: {}", e.getMessage());
                    }
                }
                if (!cosmosPanels.contains(cosmosPanel) && ObjectUtils.isNotEmpty(cosmosPanel)) {
                    cosmosPanels.add(cosmosPanel);
                }
            } catch (Exception e) {
                log.warn("Problem Parsing CosmosPanel: {}", e.getMessage());
            }
        });

        return cosmosPanels;

    }

    /**
     * @param addressRecords
     * @return
     */
    public List<Address> mapCountyRecords(List<com.optum.propel.pes.model.addresslookup.Record> addressRecords) {
        List<Address> addresses = new ArrayList<>();
        addressRecords.forEach(pesAddress -> {
            try {
                Address address = new Address();
                List<String> addressLines = new ArrayList<>();
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getAdrLn1Txt())) {
                    addressLines.add(pesAddress.getAddressData().getAdrLn1Txt());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getExtAdrLn1Txt())) {
                    addressLines.add(pesAddress.getAddressData().getExtAdrLn1Txt());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getExtAdrLn2Txt())) {
                    addressLines.add(pesAddress.getAddressData().getExtAdrLn2Txt());
                }
                if (ObjectUtils.isNotEmpty(addressLines)) {
                    address.setAddressLine(addressLines);
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getAdrId())) {
                    address.setAddressId(pesAddress.getAddressData().getAdrId());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getVanCtyNm())) {
                    Set<String> vanityCities = new HashSet<>();
                    vanityCities.add(pesAddress.getAddressData().getVanCtyNm());
                    if (CollectionUtils.isNotEmpty(vanityCities)) {
                        address.setVanityCity(vanityCities);
                    }
                }
                try {
                    if (StringUtils.isNotBlank(pesAddress.getAddressData().getStCd())) {
                        address.setState(States.fromValue(pesAddress.getAddressData().getStCd()));
                    }
                } catch (Exception ex){
                    log.warn("Possibly encountered an international state that is not supported");
                    address.setState(States.NONE);
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getZipCd())) {
                    address.setZipCode(pesAddress.getAddressData().getZipCd());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getZipPls4Cd())) {
                    address.setZipCodePlus4(pesAddress.getAddressData().getZipPls4Cd());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getCtyNm())) {
                    address.setCity(pesAddress.getAddressData().getCtyNm());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getCntyNm())) {
                    address.setCounty(pesAddress.getAddressData().getCntyNm());
                }
                if (StringUtils.isNotBlank(pesAddress.getAddressData().getCancDt())) {
                    address.setActive(EnrichmentUtil.isAddressActive(pesAddress.getAddressData().getCancDt()));
                    try {
                        address.setEndDate(LocalDate.parse(pesAddress.getAddressData().getCancDt(), DTF));
                    } catch (Exception e) {
                        log.warn("Problem Parsing pesAddress.getAddressData.getCancDt: {}", e.getMessage());
                    }
                }
                if (!addresses.contains(address) && ObjectUtils.isNotEmpty(address)) {
                    addresses.add(address);
                }
            } catch (Exception e) {
                log.warn("Problem Parsing Address: {}", e.getMessage());
            }
        });

        return addresses;

    }

    public ProviderOrganization mapPtiProviderIdOrganization(Record record) {
        ProviderOrganization providerOrganization = new ProviderOrganization();
        if(CollectionUtils.isNotEmpty(record.getPTAData().getOrganization())) {
            Organization providerOrg = record.getPTAData().getOrganization()
                    .stream().filter(Objects::nonNull)
                    .flatMap(provOrg -> provOrg
                            .stream().filter(Objects::nonNull))
                    .findFirst().orElse(null);


            try {
                if (StringUtils.isNotBlank(providerOrg.getOrgTypCd())) {
                    providerOrganization.setOrganizationType(providerOrg.getOrgTypCd());
                }
                if (StringUtils.isNotBlank(record.getPTAData().getProvId())) {
                    providerOrganization.setOrganizationId(record.getPTAData().getProvId());
                }

            } catch (Exception e) {
                log.warn("Problem Parsing getOrganization.getOrgTypCd: {}", e.getMessage());

            }
            return providerOrganization;
        }
        return null;
    }

    /**
     * @param records
     * @return
     */
    public List<PractitionerRole> mapTinsOfProvider(List<Record> records) {

        List<PractitionerRole> practitionerRoleList = new ArrayList<>();
        records.parallelStream().forEach(record -> {
            DocumentKeyDate keyDate = record.getPTAData().getDocumentKeyDates()
                    .stream().filter(Objects::nonNull)
                    .findFirst().orElse(null);

            PractitionerRole practitionerRole = new PractitionerRole();
            try {
                if (StringUtils.isNotBlank(keyDate.getTinEffDt())) {
                    practitionerRole.setStartDate(LocalDate.parse(keyDate.getTinEffDt(), DTF));
                }
            } catch (Exception e) {
                log.warn("Problem Parsing getDocumentKeyDates.getTinEffDt: {}", e.getMessage());
            }

            if (StringUtils.isNotBlank(keyDate.getTinCancDt())) {
                practitionerRole.setActive(EnrichmentUtil.isActive(keyDate.getTinCancDt()));
                try {
                    practitionerRole.setEndDate(LocalDate.parse(keyDate.getTinCancDt(), DTF));
                } catch (Exception e) {
                    log.warn("Problem Parsing getDocumentKeyDates.getTinCancDt: {}", e.getMessage());
                }
            }

            if (StringUtils.isNotBlank(record.getPTAData().getTaxIdNbr())) {
                practitionerRole.setTaxId(record.getPTAData().getTaxIdNbr());
            }
            if (AppConstants.TAX_ID_TYP_TAX.equalsIgnoreCase(record.getPTAData().getTaxIdTypCd())) {
                practitionerRole.setTaxIdType(TaxIdType.TAX);
            } else if (AppConstants.TAX_ID_TYPE_SSN.equalsIgnoreCase(record.getPTAData().getTaxIdTypCd())) {
                practitionerRole.setTaxIdType(TaxIdType.SSN);
            } else {
                practitionerRole.setTaxIdType(TaxIdType.NONE);
            }

            if (!practitionerRoleList.contains(practitionerRole) && ObjectUtils.isNotEmpty(practitionerRole)) {
                practitionerRoleList.add(practitionerRole);
            }

        });
        return practitionerRoleList;

    }

    //this method is used to map the PES response fields.

    /**
     * @param records
     * @return
     */
    public CulturalCompetencyResponse mapCulturalCompetency(List<Records> records) {

        List<Cultural_Competency> cultural_competencyList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(records)) {
            CulturalCompetencyResponse culturalCompetencyResponse = new CulturalCompetencyResponse();
            Records record = records.stream()
                    .filter(Objects::nonNull)
                    .findFirst().orElse(null);

            if (ObjectUtils.isNotEmpty(record.getPTAData().getCultural_competency())) {
                Arrays.stream(record.getPTAData().getCultural_competency()).forEach(cultural_competencies -> {
                    Arrays.stream(cultural_competencies).forEach(cultural_competency ->
                            {
                                if (ObjectUtils.isNotEmpty(cultural_competency)) {
                                    cultural_competencyList.add(cultural_competency);
                                }
                            }
                    );
                });
            }
            if (CollectionUtils.isNotEmpty(cultural_competencyList)) {
                culturalCompetencyResponse.setCultural_competency(cultural_competencyList);
            } else {
                ErrorDetail errorDetail = new ErrorDetail();
                errorDetail.setMessage("No records found");
                errorDetail.setSource("Cultural Competency");
                errorDetail.setStatusCode("006");
                culturalCompetencyResponse.setErrorMessage(errorDetail);
            }
            return culturalCompetencyResponse;
        }
        return null;
    }

    public String getAddressLocationType(String locationType) {
        String adrLocType = "";

        if (Objects.nonNull(locationType))
            switch (locationType) {

                case AppConstants.ADR_LOCTYP_BILL:
                    adrLocType = LocationType.BILL.toString();
                    break;
                case AppConstants.ADR_LOCTYP_COMBO:
                    adrLocType = LocationType.COMBO.toString();
                    break;
                case AppConstants.ADR_LOCTYP_MAIL:
                    adrLocType = LocationType.MAIL.toString();
                    break;
                case AppConstants.ADR_LOCTYP_CREDENTIAL:
                    adrLocType = LocationType.CREDENTIAL.toString();
                    break;
				case AppConstants.ADR_LOCTYP_PLACE_OF_SERVICE:
					adrLocType = LocationType.PLACE_OF_SERVICE.toString();
					break;

                default:
                    adrLocType = LocationType.NONE.toString();

            }
        return adrLocType;
    }

    public String getPhoneType(String pesPhoneType) {

        String phoneType = "";

        switch (pesPhoneType) {

            case AppConstants.BILL:
                phoneType = PhoneType.BILL.toString();
                break;
            case AppConstants.PLACE_OF_SERVICE:
                phoneType = PhoneType.PLACE_OF_SERVICE.toString();
                break;
            case AppConstants.FAX:
                phoneType = PhoneType.FAX.toString();
                break;
            case AppConstants.APPOINTMENT:
                phoneType = PhoneType.APPOINTMENT.toString();
                break;
            case AppConstants.EMERGENCY:
                phoneType = PhoneType.EMERGENCY.toString();
                break;

            default:
                phoneType = PhoneType.NONE.toString();
        }
        return phoneType;
    }

    public String getPrvDmdDatTypId(String prvDmdDatTypId) {
        String locationPVOType = "";
        switch (prvDmdDatTypId) {
            case AppConstants.LOC_PVO_ADDRESS:
                locationPVOType = LocationPVOType.ADDRESS.toString();
                break;
            case AppConstants.LOC_PVO_TELEPHONE:
                locationPVOType = LocationPVOType.TELEPHONE.toString();
                break;
            case AppConstants.LOC_PVO_FAX:
                locationPVOType = LocationPVOType.FAX.toString();
                break;
            case AppConstants.LOC_PVO_HANDICAP_ACCESS:
                locationPVOType = LocationPVOType.HANDICAP_ACCESS.toString();
                break;
            case AppConstants.LOC_PVO_EMAIL:
                locationPVOType = LocationPVOType.EMAIL.toString();
                break;
            case AppConstants.LOC_PVO_LANGUAGE_SPOKEN:
                locationPVOType = LocationPVOType.LANGUAGE_SPOKEN.toString();
                break;
            case AppConstants.LOC_PVO_AVAILABILITY_EXCEPTION:
                locationPVOType = LocationPVOType.AVAILABILITY_EXCEPTION.toString();
                break;

            default:
                locationPVOType = LocationPVOType.NONE.toString();
        }
        return locationPVOType;
    }

}