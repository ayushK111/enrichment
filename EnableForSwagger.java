/**
 *  Copyright Optum Services Inc., 2020. All rights reserved.
 *  This software and documentation contain confidential and proprietary information owned by Optum Services Inc., 
 *  and developed as part of Modern Engineering InnerSource Framework. Unauthorized use and distribution are prohibited.
 */
package com.optum.propel.enrichment.api.doc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation interface for swagger documentation
 *  
 * @author Modern_Engineering_Global_DL@ds.uhc.com
 * @since 1.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EnableForSwagger {

}
