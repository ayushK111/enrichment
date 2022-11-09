/*
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc.,
 *  and developed as part of Modern Engineering Blockchain services. Unauthorized use and distribution are prohibited.
 */

package com.optum.propel.enrichment.services;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.optum.propel.enrichment.config.AppConstants;
import com.optum.propel.enrichment.config.AppProperties;
import com.optum.propel.enrichment.model.PesResponse;
import com.optum.propel.enrichment.model.PesRequest;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
@Log4j2
public class ContractService {

	private final WebClient.Builder webClientBuilder;
	private final AppProperties appProperties;

	public ContractService(WebClient.Builder webClientBuilder, AppProperties appProperties) {
		this.webClientBuilder = webClientBuilder;
		this.appProperties = appProperties;
	}

	/*** Asynchronous call to get Unet Contract Data with Pagination Details ***/

	@Async("asyncExecutor")
	public CompletableFuture<List<PesResponse>> enrichContractData(PesRequest req, String token, String attributeSet) {

		int total = 0;
		PesResponse pesResponse = null;
		final long start = System.currentTimeMillis();
		List<PesResponse> aggregatedResponse = new ArrayList<>();

		try {
			pesResponse = pesCall(buildMultiValueMap(req, appProperties.getPes().getStart(), attributeSet), token).block();

		} catch (RuntimeException e) {
			log.error("Caught exception while getting the PES response contract api employerandindividual call ");
		}

		if (ObjectUtils.isNotEmpty(pesResponse)){
			if (ObjectUtils.isNotEmpty(pesResponse.getMetadata())) {
				aggregatedResponse.add(pesResponse);
				total = pesResponse.getMetadata().getTotal();
				if (total > appProperties.getPes().getPaginationSize()) {
					/****
					 * getting list of integers which will be used as start index for pagination and implementing
					 * parallel calls
					 ****/

					Optional<List<PesResponse>> pesReponseList = Optional
							.ofNullable(Flux.fromIterable(EnrichmentUtil.getPaginationList(total, appProperties.getPes().getPaginationSize()))
									.delayElements(Duration.ofMillis(200)).flatMap(startCount -> {
										log.debug("Working on {} with thread {}", startCount,
												Thread.currentThread().getName());
										return pesCall(buildMultiValueMap(req, startCount, attributeSet), token)
												.doOnError(e -> log.error("Pes Employerandindividual contract call failed with error message: {}",
														e.getMessage()));
									}).collectList().block());
					pesReponseList.ifPresent(aggregatedResponse::addAll);
				}
			}
			log.info("Took {} milliseconds to retrieve pes employerandindividual contract response", System.currentTimeMillis() - start);

			if (CollectionUtils.isNotEmpty(aggregatedResponse)) {
				log.info("Final result list size aggregatedResponse: {}", aggregatedResponse.size());
				return CompletableFuture.completedFuture(aggregatedResponse);
			} else {
				log.info("No record found for Unet contract : {}", pesResponse.getServiceFault().getMessage());
				return CompletableFuture.completedFuture(Collections.emptyList());
			}
		} else {
			log.error("Exception occurred, received null response from PES api Employerandindividual");
			return CompletableFuture.completedFuture(Collections.emptyList());
		}

	}

	public Mono<PesResponse> pesCall(MultiValueMap<String, String> req, String token) {
		/** Rest call to get Data from Contract API **/

		return webClientBuilder.baseUrl(appProperties.getPes().getHost())
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE).build().post()
				.uri(uriBuilder -> uriBuilder.path(appProperties.getPes().getContract()).build())
				.body(BodyInserters.fromFormData(req)).headers(httpHeaders -> httpHeaders.setBearerAuth(token))
				.retrieve().onStatus(HttpStatus::is4xxClientError, clientResponse -> {
					return clientResponse.bodyToMono(String.class).flatMap(message -> {
						log.error("Exception occurred: {}", message);
						return Mono.error(new RuntimeException("Received 4xx error: " + message));
					});
				}).onStatus(HttpStatus::is5xxServerError,
						clientResponse -> clientResponse.bodyToMono(String.class).flatMap(message -> {
							log.error("Error occurred while retrieving pes response: {}", message,this.getClass().getName());
							return Mono.error(new RuntimeException(
									"Received 5xx status code from Pes call, internal server error" + message));
						}))
				.onStatus(HttpStatus::isError,
						clientResponse -> clientResponse.bodyToMono(String.class).flatMap(message -> {
							log.error("General Error occurred while retrieving pes response: {}", message,log.getClass().getName());
							return Mono.error(new RuntimeException(
									"Received generic exception, internal server error" + message));
						}))
				.bodyToMono(PesResponse.class)
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
						.doBeforeRetry(retrySignal -> log.warn("Retrying request..."))
						.filter(throwable -> throwable.getClass() != WebClientResponseException.NotFound.class))
				.doOnError(Throwable::printStackTrace);
	}

	public MultiValueMap<String, String> buildMultiValueMap(PesRequest req,int start, String attributeSet) {
		/** Setting attribute set for Unet Contract **/
		MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
		if(StringUtils.isNotBlank(req.getTin())) {
			map.add(AppConstants.TAX_ID,req.getTin());
		}
		map.add(AppConstants.APP_NAME, appProperties.getPes().getAppName());
		map.add(AppConstants.PROV_ID, req.getProviderId());
		map.add(AppConstants.ATTRIBUTE_SET, attributeSet);
		map.add(AppConstants.COUNT,String.valueOf(appProperties.getPes().getCount()));
		map.add(AppConstants.START, String.valueOf(start));

		return map;
	}
}