/*
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc.,
 *  and developed as part of Modern Engineering Blockchain services. Unauthorized use and distribution are prohibited.
 */

package com.optum.propel.enrichment.services;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;
//import com.github.benmanes.caffeine.cache.Caffeine;
import com.optum.propel.enrichment.config.AppProperties;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
@Log4j2
//@CacheConfig(cacheNames = {"authToken"})
public class TokenService {

    private final WebClient.Builder webClientBuilder;
    private final AppProperties appProperties;
    CacheManager cacheManager;
    //Caffeine< Object, Object > caffeine;

    public TokenService(WebClient.Builder webClientBuilder, AppProperties appProperties, CacheManager cacheManager) {
        this.webClientBuilder = webClientBuilder;
        this.appProperties = appProperties;
        this.cacheManager=cacheManager;
        //this.caffeine = caffeine;
    }

    // @Cacheable
    public String getToken(){
        log.info("Calling Token Service");
        Optional<JsonNode> token = Optional.ofNullable(webClientBuilder
                .baseUrl(appProperties.getOauth().getUri()).defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .build().post()
                .uri(uriBuilder -> uriBuilder
                        .queryParam("client_id", appProperties.getOauth().getClientId())
                        .queryParam("grant_type", appProperties.getOauth().getGrantType())
                        .queryParam("client_secret", appProperties.getOauth().getClientSecret())

                        .build())
                .retrieve()
                .onStatus(HttpStatus::is4xxClientError, clientResponse -> clientResponse.bodyToMono(String.class)
                        .flatMap(message -> {
                            log.error("Error occurred while retrieving oauth token: {}",clientResponse.rawStatusCode());
                            return Mono.error(new RuntimeException("Received 4xx status code from Oauth call - " + message));
                        }))
                .onStatus(HttpStatus::is5xxServerError, clientResponse -> clientResponse.bodyToMono(String.class)
                        .flatMap(message -> {
                            log.error("Error occurred while retrieving oauth token: {}",clientResponse.rawStatusCode());
                            return Mono.error(new RuntimeException("Received 5xx status code from Oauth call, internal server error - " + message));
                        }))
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(3))
                        .doBeforeRetry(retrySignal -> log.warn("Retrying token request..."))
                        .filter(throwable -> throwable.getClass() != WebClientResponseException.NotFound.class))
                .block());

        if(token.isPresent()){
            String oauthToken = token.get().get("access_token").asText();
//            caffeine.expireAfterWrite(token.get().get("expires_in").asLong(), TimeUnit.SECONDS);
//            if(cacheManager.getCache("authToken")!=null) {
//            	 caffeine.refreshAfterWrite(30, TimeUnit.MINUTES);
//            }
//            log.info("Received Oauth Token: {}", oauthToken);
            return oauthToken;
        }else{
            log.error("Error while retrieving Oauth Token");
            return null;
        }
    }
}
