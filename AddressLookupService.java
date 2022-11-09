package com.optum.propel.enrichment.services;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
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
import com.optum.propel.enrichment.model.AddressLookupResponse;
import com.optum.propel.enrichment.model.PesRequest;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
@Log4j2
@AllArgsConstructor
public class AddressLookupService {

	private final WebClient.Builder webClientBuilder;
	private final AppProperties appProperties;

	/*** Asynchronous call to get location Data with Pagination Details ***/
	@Async
	public  CompletableFuture<List<com.optum.propel.pes.model.addresslookup.Record>> enrichAddressLookupData(PesRequest req, String token) {

		int total = 0;
		AddressLookupResponse addressLookupResponse = null;
		List<AddressLookupResponse> aggregatedResponse = new ArrayList<>();
		final long start = System.currentTimeMillis();
		try {
			addressLookupResponse = pesCall(getAddressFormData(req, 0), token).block();
		} catch (RuntimeException e) {
			log.error("Caught exception while getting the PES response");
		}
		if (ObjectUtils.isNotEmpty(addressLookupResponse)) {
			if (ObjectUtils.isNotEmpty(addressLookupResponse.getMetadata())) {
				aggregatedResponse.add(addressLookupResponse);
				total = addressLookupResponse.getMetadata().getTotal();
				if (total > appProperties.getPes().getPaginationSize()) {
					/****
					 * getting list of integers which will be used as start index for pagination and implementing
					 * parallel calls
					 ****/
					Optional<List<AddressLookupResponse>> addressReponseList = Optional
							.ofNullable(Flux.fromIterable(EnrichmentUtil.getPaginationList(total, appProperties.getPes().getPaginationSize()))
									.delayElements(Duration.ofMillis(200)).flatMap(startCount -> {
										log.debug("Working on {} with thread {}", startCount,
												Thread.currentThread().getName());
										return pesCall(getAddressFormData(req, startCount), token).doOnError(e -> log
												.error("Pes called failed with error message: {}", e.getMessage()));
									}).collectList().block());
					addressReponseList.ifPresent(aggregatedResponse::addAll);
				}
			}
			log.info("Took {} milliseconds to retrieve pes response", System.currentTimeMillis() - start);

			/** Aggregating Data got from location API **/

			List<com.optum.propel.pes.model.addresslookup.Record> mergedList = aggregatedResponse.stream()
					.filter(pesResp -> CollectionUtils.isNotEmpty(pesResp.getRecords()))
					.flatMap(p -> p.getRecords().stream()).collect(Collectors.toList());

			if (CollectionUtils.isNotEmpty(mergedList)) {
				log.info("Final result list size for Address Lookup: {}", mergedList.size());
				return CompletableFuture.completedFuture(mergedList);
			} else {
				log.info("No record found for Address Lookup : {}", addressLookupResponse.getServiceFault().getMessage());
				return CompletableFuture.completedFuture(Collections.emptyList());
			}
		} else {
			log.error("Exception occurred, received null response from PES api");
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
	}

	public Mono<AddressLookupResponse> pesCall(MultiValueMap<String, String> req, String token) {

		/** Rest call to get Data from location API **/

		return webClientBuilder.baseUrl(appProperties.getPes().getHost())
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE).build().post()
				.uri(uriBuilder -> uriBuilder.path(appProperties.getPes().getAddress()).build())
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
				.bodyToMono(AddressLookupResponse.class)/*** Retrying In case of API is down ***/
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
						.doBeforeRetry(retrySignal -> log.warn("Retrying request..."))
						.filter(throwable -> throwable.getClass() != WebClientResponseException.NotFound.class))
				.doOnError(Throwable::printStackTrace);
	}



	private MultiValueMap<String, String> getAddressFormData(PesRequest req, int start) {
		MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
		map.add(AppConstants.TAX_ID, req.getTin());
		map.add(AppConstants.PROV_ID, req.getProviderId());
		map.add(AppConstants.COUNT, String.valueOf(appProperties.getPes().getCount()));
		map.add(AppConstants.START, String.valueOf(start));
		map.add(AppConstants.APP_NAME, appProperties.getPes().getAppName());
		map.add(AppConstants.ADDRESS_ID,req.getLocationId());

		return map;
	}

}
