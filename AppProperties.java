/*
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc.,
 *  and developed as part of Modern Engineering Blockchain services. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties("app")
public class AppProperties {

	private Oauth oauth = new Oauth();
	private Pes pes = new Pes();

	@Getter
	@Setter
	public static class Oauth{
		private String uri;
		private String clientId;
		private String grantType;
		private String clientSecret;
	}

	@Getter
	@Setter
	public static class Pes{
		private String host;
		private String appName;
		private String location;
		private String contract;
		private String culturalCompetency;
		private String address;
		private int count;
		private int start;
		private int paginationSize;
		private String cosmosContract;
		private String facetContract;
		private String unetContract;
		private String aoe;
	}

}