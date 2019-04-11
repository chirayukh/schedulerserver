package com.iig.gcp.schedulerserver.driver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("com.iig.gcp.schedulerserver*")
@SpringBootApplication
public class JuniperOnPremSchedulerWsApplication {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SpringApplication.run(JuniperOnPremSchedulerWsApplication.class, args);
	}

}
