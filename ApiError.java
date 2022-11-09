/**
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc., 
 *  and developed as part of Modern Engineering InnerSource Framework. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.model;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.http.HttpStatus;

/**
 * Global Custom Error Status Bean
 *
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */
public class ApiError {

	private HttpStatus status;
	private List<ErrorDetail> errorList;
	private Date timestamp;
	public ApiError(HttpStatus status, List<ErrorDetail> errorList) {
		super();
		this.status = status;
		this.errorList = errorList;
		this.timestamp=new Date();
	}

	public ApiError(HttpStatus status, ErrorDetail error) {
		super();
		this.status = status;
		this.errorList = Arrays.asList(error);
		this.timestamp=new Date();
	}

	public HttpStatus getStatus() {
		return status;
	}

	public void setStatus(HttpStatus status) {
		this.status = status;
	}


	public List<ErrorDetail> getErrorList() {
		return errorList;
	}

	public void setErrorList(List<ErrorDetail> errorList) {
		this.errorList = errorList;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}


}
