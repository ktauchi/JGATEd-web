#!/usr/local/groovy/bin/groovy

@Grapes([
	
	@Grab(group='org.apache.commons', module='commons-lang3', version='3.17.0'),
	/*
	*/
	
	@Grab(group='org.apache.camel', module='camel-core', version='4.10.3'),
	@Grab(group='org.apache.camel', module='camel-spring', version='4.10.3'),
	@Grab(group='org.apache.camel', module='camel-nats', version='4.10.3'),
	@Grab(group='org.apache.camel', module='camel-spring-xml', version='4.10.3'),
	
	@Grab(group='org.springframework', module='spring-web', version='6.2.6'),
	@Grab(group='org.springframework', module='spring-webmvc', version='6.2.6'),
	@Grab(group='org.springframework', module='spring-jdbc', version='6.2.6'),
	@Grab(group='org.thymeleaf', module='thymeleaf-spring5', version='3.1.3.RELEASE'),
	
	@Grab(group='org.apache.logging.log4j', module='log4j-core', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-api', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-1.2-api', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-slf4j-impl', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-slf4j2-impl', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-jcl', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-jul', version='2.24.3'),
	@Grab(group='org.apache.logging.log4j', module='log4j-web', version='2.24.3'),
	
	@Grab(group='org.apache.logging.log4j', module='log4j-slf4j-impl', version='2.24.3'),
	
	@Grab(group='org.msgpack', module='msgpack-core', version='0.9.9'),
	@Grab(group='org.msgpack', module='jackson-dataformat-msgpack', version='0.9.9'),
	
	@Grab(group='io.lettuce', module='lettuce-core', version='6.5.5.RELEASE'),
	
	@Grab(group='org.springframework.data', module='spring-data-redis', version='3.4.5'),
	
	/*
	@GrabExclude(group='io.netty', module='netty-transport'),
	@GrabExclude(group='io.netty', module='netty-transport-native-unix-common'),
	*/
	@Grab(group='io.netty', module='netty-common', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-handler', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-resolver', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-buffer', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-transport-native-epoll', version='4.1.120.Final', classifier='linux-x86_64'),
	@Grab(group='io.netty', module='netty-codec', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-transport', version='4.1.120.Final'),
	@Grab(group='io.netty', module='netty-transport-native-unix-common', version='4.1.120.Final'),
	
	/*
	@Grab(group='org.apache.logging.log4j', module='log4j-web', version='2.14.1'),
	@Grab(group='org.apache.logging.log4j', module='log4j-jcl', version='2.14.1'),
	@Grab(group='org.apache.logging.log4j', module='log4j-jul', version='2.14.1'),
	@Grab(group='org.apache.logging.log4j', module='log4j-api', version='2.14.1'),
	@Grab(group='org.apache.logging.log4j', module='log4j-1.2-api', version='2.14.1'),
	@Grab(group='org.apache.logging.log4j', module='log4j-slf4j-impl', version='2.14.1'),
	*/
	
	@Grab(group='com.mysql', module='mysql-connector-j', version='8.4.0'),
	
	@Grab(group='org.apache.commons', module='commons-dbcp2', version='2.13.0'),
	
	@Grab(group='org.mybatis', module='mybatis', version='3.5.19'),
	
	@Grab(group='org.mybatis', module='mybatis-spring', version='3.0.4'),
	
	@Grab(group='org.influxdb', module='influxdb-java', version='2.25'),
	
])

// 

import java.lang.System;

import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;

String dstDir = "/var/local/tomcat/lib";
//new File("./grapes").traverse(type: groovy.io.FileType.FILES, nameFilter: ~/jar/) { it ->
new File("./grapes").traverse(type: groovy.io.FileType.FILES, nameFilter: ~/.*.jar/){ it ->
    //System.out.println(it);
	String srcPath = it.getPath();
	String dstPath = String.format("%s/%s", dstDir, it.getName());
	
	System.out.println(srcPath + " => " + dstPath);
	//Files.copy(Paths.get(srcPath), Paths.get(dstPath), StandardCopyOption.REPLACE_EXISTING);
};


System.out.println("completed");

