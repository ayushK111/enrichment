/**
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc., 
 *  and developed as part of Modern Engineering InnerSource Framework. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.api.doc;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.service.ObjectVendorExtension;
import springfox.documentation.service.StringVendorExtension;
import springfox.documentation.service.Tag;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;



/**
 * Swagger Documentation
 * 
 *  
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */

@Configuration
@EnableSwagger2
public class SwaggerConfig {

	@Value("${spring.application.name}")
	private String appName;
	
	@Value("${spring.info.api.version}")
	String apiVersion;
	
	@Value("${spring.info.team.url}")
	String teamUrl;
	
	@Value("${spring.info.github.url}")
	String githubUrl;
	
	@Value("${spring.info.contact}")
	String contactEmail;
	
	@Value("${spring.application.description}")
	private String appDescription;
	
	@Bean
	public Docket postsApi() {
		
		ObjectVendorExtension ext = new ObjectVendorExtension("externalDocs");
	    ext.addProperty(new StringVendorExtension("description", "Github URL"));
	    ext.addProperty(new StringVendorExtension("url", githubUrl));

		return new Docket(DocumentationType.SWAGGER_2)
				.extensions(Collections.singletonList(ext))
				.select()
				.apis(RequestHandlerSelectors.withClassAnnotation(EnableForSwagger.class))
				.paths(PathSelectors.any())
				.build()
				.apiInfo(metaData()).tags(new Tag("API Resources", "Please use below links to access API spec"));
	}

	/**
	 * API meta data
	 * @return
	 */
	private ApiInfo metaData() {
	
		return new ApiInfoBuilder()
				.title(appName)
				.description(appDescription)
				.version(apiVersion)
				.contact(new Contact("Modern Engineering Team ", teamUrl, contactEmail))
				.build();
	}
}