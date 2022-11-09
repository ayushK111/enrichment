package com.optum.propel.enrichment.aspect;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.ThreadContext;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.optum.engineering.common.logging.logging.ContextField;
import com.optum.engineering.common.logging.logging.EventFields;
import com.optum.engineering.common.logging.logging.EventStatus;
import com.optum.engineering.common.logging.model.RER;
import com.optum.engineering.common.logging.utils.LogUtils;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * This aspect use to add advice on controllers which have following annotations on controller methods
 *  org.springframework.web.bind.annotation.GetMapping
 *  org.springframework.web.bind.annotation.PostMapping
 *
 * <p>
 * Aspect advice have following logic
 *  <ul>
 *      <li>Log json message before and after method exeecution</li>
 *      <li>Find header fields and log in the ThreadContext, these fields will be available for the entire thread</li>
 *      <li>Calculate total method execution time</li>
 *      <li>log RER (Raw Error Rate) having app name, http status code and total time</li>
 *  </ul>
 * </p>
 *
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 */
@Aspect
@Log4j2
@Configuration
public class ControllerAspect {

	@Getter
	@Value("${spring.application.name}")
	private String appName;

	/**
	 * Point for the Get and Post Mapping for spring controller
	 */
	@Pointcut("@annotation(org.springframework.web.bind.annotation.GetMapping) || " +
			"@annotation(org.springframework.web.bind.annotation.PostMapping)")
	public void getMappingAnnotationMethodPointcut() {}

	@Pointcut("execution(* *(..))")
	public void atExecutionPointcut() {}

	@Around("getMappingAnnotationMethodPointcut() && atExecutionPointcut()")
	public Object aroundAdvice(ProceedingJoinPoint joinPoint) throws Throwable {

		long startTime = System.currentTimeMillis();
		LocalDateTime startTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTime), 
				TimeZone.getDefault().toZoneId());

		RER rer = new RER(appName);
		rer.setMessage(String.format("Request received %s", appName));
		rer.getFields().put(EventFields.ACTUAL_CLASS_NAME, joinPoint.getSignature().getDeclaringTypeName());
		rer.getFields().put(EventFields.ACTUAL_METHOD_NAME, joinPoint.getSignature().getName());
		rer.getFields().put(EventFields.STATUS, EventStatus.RECEIVED.getStatus());
		rer.getFields().put("receivedTime", startTimestamp.toString());
		log.info(LogUtils.getJSONString(rer));

		long totalTime;

		Object returnObj = null;
		ResponseEntity responseEntity = null;
		try {
			HttpServletRequest request = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();
			ThreadContext.put(ContextField.GLOBAL_ID, request.getHeader(ContextField.GLOBAL_ID));
			if (!StringUtils.isEmpty(request.getHeader(ContextField.TIN))) ThreadContext.put(ContextField.TIN, request.getHeader(ContextField.TIN));
			if (!StringUtils.isEmpty(request.getHeader(ContextField.PROVIDER_ID))) ThreadContext.put(ContextField.PROVIDER_ID, request.getHeader(ContextField.PROVIDER_ID));

			returnObj = joinPoint.proceed();
			if (returnObj instanceof ResponseEntity) {
				responseEntity = (ResponseEntity) returnObj;
			} else {
				throw new Exception("ResponseEntity instance is not used in Response of the controller method");
			}

		} catch (Throwable throwable) {

			rer.getFields().put(EventFields.ERROR,
					StringUtils.isEmpty(throwable.getMessage()) ? "No Message" : throwable.getMessage());
			throw throwable;
		} finally {
			totalTime = System.currentTimeMillis() - startTime;
			LocalDateTime totalTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(totalTime), 
					TimeZone.getDefault().toZoneId());
			//setting http status code. Set 500 when there is an exception or no code set.
			rer.getFields().put(EventFields.ACTUAL_CLASS_NAME, joinPoint.getSignature().getDeclaringTypeName());
			rer.getFields().put(EventFields.ACTUAL_METHOD_NAME, joinPoint.getSignature().getName());
			if (Objects.nonNull(responseEntity)) {
				rer.setMessage("Request processed");
				rer.getFields().put(EventFields.STATUS, EventStatus.COMPLETED.getStatus());
				rer.setStatusCode(responseEntity.getStatusCodeValue());
			} else {
				rer.setMessage("Request processed with errors");
				rer.getFields().put(EventFields.STATUS, EventStatus.COMPLETEDWITHERROR.getStatus());
				rer.setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR.ordinal());
			}
			rer.setResponseTime(totalTime);
			log.info(LogUtils.getJSONString(rer));

			ThreadContext.clearAll();
		}

		return returnObj;
	}


}