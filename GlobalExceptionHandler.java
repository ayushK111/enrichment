/**
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc., 
 *  and developed as part of Modern Engineering InnerSource Framework. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.exception;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.optum.propel.enrichment.model.ApiError;
import com.optum.propel.enrichment.model.ErrorDetail;

/**
 * Global Exception handler
 *
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */
@ControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

	// Exception handler for any unhandled exceptions.
	@ExceptionHandler(value = { Exception.class })
	protected ResponseEntity<Object> handleConflict(RuntimeException ex, WebRequest request) {

		ApiError apiError = new ApiError(HttpStatus.INTERNAL_SERVER_ERROR,
				new ErrorDetail("serviceName", "something went wrong", "500"));

		return new ResponseEntity<>(apiError, new HttpHeaders(), HttpStatus.INTERNAL_SERVER_ERROR);
	}

	// Exception handler for validation errors.
	@Override
	protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
																  HttpHeaders headers, HttpStatus status, WebRequest request) {

		List<ErrorDetail> errorDetail = new ArrayList<>();

		// Get all errors
		List<FieldError> fieldErrors = ex.getBindingResult().getFieldErrors();
		for (FieldError error : fieldErrors) {
			errorDetail.add(new ErrorDetail(error.getField(), error.getDefaultMessage(), "400"));
		}

		ApiError apiError = new ApiError(HttpStatus.BAD_REQUEST, errorDetail);
		return new ResponseEntity<>(apiError, new HttpHeaders(), HttpStatus.BAD_REQUEST);

	}

}
