package com.optum.propel.enrichment.services;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;


@Log4j2
public class EnrichmentUtil {

	public static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	public static List<Integer> getPaginationList(int total, int paginationSize) {
		/** getting list of integer starting from 0 and incrementing by
		 *  adding pagination size which will used as start index for pagination **/

		AtomicInteger atomicInteger = new AtomicInteger(paginationSize);
		int counter = (int) Math.ceil((double) (total - paginationSize)
				/ paginationSize);
		return IntStream.rangeClosed(1, counter)
				.mapToObj(i -> atomicInteger.getAndAdd(paginationSize))
				.collect(Collectors.toList());
	}

	public static boolean isAddressActive(String addressCancelDate){
		if(StringUtils.isNotBlank(addressCancelDate)){
			if("9999-12-31".equalsIgnoreCase(addressCancelDate))
				return true;
			try{
				LocalDate now = LocalDate.now();
				if(!LocalDate.parse(addressCancelDate, DTF).isBefore(now))
					return true;
			}catch (DateTimeParseException e){
				log.error("Error while parsing address cancel date: {}", e.getMessage());
				return false;
			}
		}
		return false;
	}

	public static boolean isContractActive(String contractCancelDate){
		if(StringUtils.isNotBlank(contractCancelDate)){
			if("9999-12-31".equalsIgnoreCase(contractCancelDate))
				return true;
			try{
				LocalDate now = LocalDate.now();
				if(!LocalDate.parse(contractCancelDate, DTF).isBefore(now))
					return true;
			}catch (DateTimeParseException e){
				log.error("Error while parsing contract cancel date: {}", e.getMessage());
				return false;
			}
		}
		return false;
	}

	public static boolean isActive(String cancelDate){
		if(StringUtils.isNotBlank(cancelDate)){
			if("9999-12-31".equalsIgnoreCase(cancelDate))
				return true;
			try{
				LocalDate now = LocalDate.now();
				if(!LocalDate.parse(cancelDate, DTF).isBefore(now))
					return true;
			}catch (DateTimeParseException e){
				log.error("Error while parsing  cancel date: {}", e.getMessage());
				return false;
			}
		}
		return false;
	}
}
