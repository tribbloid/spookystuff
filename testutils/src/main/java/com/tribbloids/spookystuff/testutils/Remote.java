package com.tribbloids.spookystuff.testutils;

import org.scalatest.TagAnnotation;

import java.lang.annotation.*;

@TagAnnotation
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Remote {}
