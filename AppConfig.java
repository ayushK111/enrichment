/**
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc., 
 *  and developed as part of Modern Engineering InnerSource Framework. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.config;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Application general configurations
 *  
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */

//@Log4j2
@Configuration
@EnableAsync
public class AppConfig {
	

//    @Value("${app-configs.maxMemorySize}")
//    private int maxMemorySize;
    
    @Bean
    public ObjectMapper mapper(){
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.setSerializationInclusion(Include.NON_EMPTY);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(String.class, new StringDeserializer());
        mapper.registerModule(module);
        return mapper;
    }
    
    @JsonComponent
    public class StringDeserializer extends com.fasterxml.jackson.databind.deser.std.StringDeserializer {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            String value = super.deserialize(p, ctxt);
            
            if(StringUtils.isNotBlank(value)) {
            	value = value.strip();
            }else {
            	value=null;
            }
            return StringUtils.isNotBlank(value)? value: null ;
        }
    }
    
    /*** Default Thread pool for async call ***/
    @Bean(name="asyncExecutor")
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(50);
        return executor;
    }
    
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
    return new ThreadPoolTaskExecutor();
}
//    @Bean
//    public CacheManager cacheManager() {
//     CaffeineCacheManager cacheManager = new CaffeineCacheManager("authToken");
//     cacheManager.setCaffeine(caffeine());
//     return cacheManager;
//    }
//
//    @Bean
//    Caffeine < Object, Object > caffeine() {
//     return Caffeine.newBuilder()
//      .initialCapacity(1)
//      .maximumSize(500)
////      .refreshAfterWrite(60, TimeUnit.MINUTES)
////      .expireAfterWrite(5, TimeUnit.MINUTES)
//      .weakKeys()
//      .recordStats();
//    } 
    
//    // use to request non SSL explicit URL
//    @Bean
//    WebClient webClient() {
//        return WebClient.builder().exchangeStrategies(ExchangeStrategies.builder().codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(maxMemorySize)).build()).build();
//    }

}