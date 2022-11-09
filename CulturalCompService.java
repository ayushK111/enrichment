package com.optum.propel.enrichment.services;

import com.optum.propel.enrichment.config.AppConstants;
import com.optum.propel.enrichment.config.AppProperties;
import com.optum.propel.enrichment.model.CulturalCompetencyPesResponse;
import com.optum.propel.enrichment.model.CulturalCompetencyRequest;
import com.optum.propel.pes.model.roster.Records;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Log4j2
public class CulturalCompService {

    private final WebClient.Builder webClientBuilder;
    private final AppProperties appProperties;
    public CulturalCompService(WebClient.Builder webClientBuilder, AppProperties appProperties) {
        this.webClientBuilder = webClientBuilder;
        this.appProperties = appProperties;
    }

    List<Records> enrichCulturalComp(CulturalCompetencyRequest req, String token)
    {

        if (ObjectUtils.isNotEmpty(req) && StringUtils.isNotBlank(req.getTin()))
        {
            ThreadContext.put("tin",req.getTin());
        }

        if (ObjectUtils.isNotEmpty(req) && StringUtils.isNotBlank(req.getProviderId()))
        {
            ThreadContext.put("providerId",req.getProviderId());
        }

        int total = 0;

        CulturalCompetencyPesResponse pesResponse =  null;
        List<CulturalCompetencyPesResponse> aggregatedResponse = new ArrayList<>();
        final long start = System.currentTimeMillis();

        try {

            req.setAdrSeq(com.optum.propel.enrichment.services.AppConstants.ADR_SEQ);
            req.setAppName(com.optum.propel.enrichment.services.AppConstants.APP_NAME);
            req.setAttributeSet(com.optum.propel.enrichment.services.AppConstants.ATTRIBUTE_SET);
            pesResponse = pesCall(getCultCompDataForm(req,0),token).block();
            Objects.requireNonNull(pesResponse, "received null response from PES api");

        }

        catch (RuntimeException e){
            log.error("Caught exception while getting the PES response. " + e.getMessage());
            return Collections.emptyList();
        }
        try {

            if (ObjectUtils.isNotEmpty(pesResponse.getMetadata())) {
                aggregatedResponse.add(pesResponse);
                total = pesResponse.getMetadata().getTotal();
                if (total > appProperties.getPes().getPaginationSize()) {
                    /****
                     * getting list of integers which will be used as start index for pagination and implementing
                     * parallel calls
                     ****/
                    Optional<List<CulturalCompetencyPesResponse>> pesReponseList = Optional
                            .ofNullable(Flux.fromIterable(EnrichmentUtil.getPaginationList(total, appProperties.getPes().getPaginationSize()))
                                    .delayElements(Duration.ofMillis(200)).flatMap(startCount -> {
                                        log.debug("Working on {} with thread {}", startCount,
                                                Thread.currentThread().getName());
                                        return pesCall(getCultCompDataForm(req, startCount), token).doOnError(e -> log
                                                .error("Pes called failed with error message: {}", e.getMessage()));
                                    }).collectList().block());
                    pesReponseList.ifPresent(aggregatedResponse::addAll);
                }
            }


            /** Aggregating Data got from cult comp API **/

            List<Records> mergedList = aggregatedResponse.stream()
                    .filter(pesResp -> CollectionUtils.isNotEmpty(pesResp.getRecords()))
                    .flatMap(p -> p.getRecords().stream()).collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(mergedList)) {
                return mergedList;
            } else {
                log.info("No record found for Cultural Competency API: {}", pesResponse.getServiceFault().getMessage());
                return Collections.emptyList();
            }

        } catch (Exception exception) {
            log.error("Error occurred while reading PES response");
            return Collections.emptyList();
        } finally {
            ThreadContext.clearAll();
        }
    }

    public Mono<CulturalCompetencyPesResponse> pesCall(MultiValueMap<String, String> req, String token){


        return webClientBuilder.baseUrl(appProperties.getPes().getHost())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE).build().post()
                .uri(uriBuilder -> uriBuilder.path(appProperties.getPes().getCulturalCompetency()).build())
                .body(BodyInserters.fromFormData(req)).headers(httpHeaders -> httpHeaders.setBearerAuth(token))
                .retrieve().onStatus(HttpStatus::is4xxClientError, clientResponse -> {
                    return clientResponse.bodyToMono(String.class).flatMap(message -> {
                        log.error("Exception occurred: {}", message);
                        return Mono.error(new RuntimeException("Received 4xx error: " + message));
                    });
                }).onStatus(HttpStatus::is5xxServerError,
                        clientResponse -> clientResponse.bodyToMono(String.class).flatMap(message -> {
                            log.error("Error occurred while retrieving pes response: {}", message);
                            return Mono.error(new RuntimeException(
                                    "Received 5xx status code from Pes call, internal server error" + message));
                        }))
                .onStatus(HttpStatus::isError,
                        clientResponse -> clientResponse.bodyToMono(String.class).flatMap(message -> {
                            log.error("General Error occurred while retrieving pes response: {}", message);
                            return Mono.error(new RuntimeException(
                                    "Received generic exception, internal server error" + message));
                        }))
                .bodyToMono(CulturalCompetencyPesResponse.class)/*** Retrying In case of API is down ***/
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
                        .doBeforeRetry(retrySignal -> log.warn("Retrying request..."))
                        .filter(throwable -> throwable.getClass() != WebClientResponseException.NotFound.class))
                .doOnError(Throwable::printStackTrace);
    }


    private MultiValueMap<String, String> getCultCompDataForm(CulturalCompetencyRequest req, int start) {
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        if(StringUtils.isNotBlank(req.getTin())) {
            map.add(com.optum.propel.enrichment.config.AppConstants.TAX_ID, req.getTin());
        }
        if(StringUtils.isNotBlank(req.getProviderId())) {
            map.add(com.optum.propel.enrichment.config.AppConstants.PROV_ID, req.getProviderId());
        }
        map.add(com.optum.propel.enrichment.config.AppConstants.COUNT, String.valueOf(appProperties.getPes().getCount()));
        map.add(com.optum.propel.enrichment.config.AppConstants.START, String.valueOf(start));
        map.add(AppConstants.APP_NAME, appProperties.getPes().getAppName());
        map.add(AppConstants.ADDRESS_SEQ, com.optum.propel.enrichment.services.AppConstants.ADR_SEQ);
        map.add(AppConstants.ATTRIBUTE_SET,com.optum.propel.enrichment.services.AppConstants.ATTRIBUTE_SET);
        return map;
    }


}
