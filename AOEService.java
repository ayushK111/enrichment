package com.optum.propel.enrichment.services;

import java.time.Duration;
import java.util.*;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.optum.propel.enrichment.config.AppProperties;
import com.optum.propel.enrichment.model.AOEPesRequest;
import com.optum.propel.enrichment.model.AOEPesResponse;

import lombok.extern.log4j.Log4j2;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Log4j2
@Service
public class AOEService {

	private final WebClient.Builder webClientBuilder;
	private final AppProperties appProperties;

	public AOEService(WebClient.Builder webClientBuilder, AppProperties appProperties) {
		this.webClientBuilder = webClientBuilder;
		this.appProperties = appProperties;
	}


	public Mono<AOEPesResponse> pesAoeCall(AOEPesRequest req, String token) {


		return webClientBuilder.baseUrl(appProperties.getPes().getHost())
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).build().post()
				.uri(uriBuilder -> uriBuilder.path(appProperties.getPes().getAoe()).build())
				.body(Mono.just(req),AOEPesRequest.class).headers(httpHeaders -> {
					httpHeaders.setBearerAuth(token);
					httpHeaders.set("Content-Type","application/json;charset=UTF-8");
				}).retrieve().onStatus(HttpStatus::is4xxClientError, clientResponse -> {
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
				.bodyToMono(AOEPesResponse.class)/*** Retrying In case of API is down ***/
				.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
						.doBeforeRetry(retrySignal -> log.warn("Retrying request..."))
						.filter(throwable -> throwable.getClass() != WebClientResponseException.NotFound.class))
				.doOnError(Throwable::printStackTrace);
	}


	public  List<AOEPesResponse> enrichAoeData(AOEPesRequest req, String token) {

		if(ObjectUtils.isNotEmpty(req) && StringUtils.isNotBlank(req.getTaxId())) {
			ThreadContext.put("tin", req.getTaxId());
		}

		ThreadContext.put("providerId", req.getProviderId());

		int total = 0;
		AOEPesResponse pesResponse = null;
		List<AOEPesResponse> aggregatedResponse = new ArrayList<>();
		final long start = System.currentTimeMillis();
		try {
			req.setApplicationName(AppConstants.APP_NAME);
			pesResponse = pesAoeCall(req, token).block();

			Objects.requireNonNull(pesResponse, "received null response from PES api");
		} catch (RuntimeException e) {
			log.error("Caught exception while getting the PES response. " + e.getMessage());
			return aggregatedResponse;
		}
		try {
			if (ObjectUtils.isNotEmpty(pesResponse.getMeta())) {
				aggregatedResponse.add(pesResponse);
				total = Integer.parseInt(pesResponse.getMeta().getTotalRecordCount());
				if (total > appProperties.getPes().getPaginationSize()) {
					/****
					 * getting list of integers which will be used as start index for pagination and implementing
					 * parallel calls
					 ****/
					Optional<List<AOEPesResponse>> pesReponseList = Optional
							.ofNullable(Flux.fromIterable(EnrichmentUtil.getPaginationList(total, appProperties.getPes().getPaginationSize()))
									.delayElements(Duration.ofMillis(200)).flatMap(startCount -> {
										log.debug("Working on {} with thread {}", startCount,
												Thread.currentThread().getName());
										req.setPageOffset(startCount);
										return pesAoeCall(req, token).doOnError(e -> log
												.error("Pes called failed with error message: {}", e.getMessage()));
									}).collectList().block());
					pesReponseList.ifPresent(aggregatedResponse::addAll);
				}
			}

			if (aggregatedResponse.isEmpty()) {
				log.info("No record found for AOE API: {}", pesResponse.getMeta());
			}
			return aggregatedResponse;

		} catch (Exception exception) {
			log.error("Error occurred while reading PES response");
			return aggregatedResponse;
		} finally {
			ThreadContext.clearAll();
		}
	}

}
