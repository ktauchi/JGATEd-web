package local.minkabu.jgate.processor;

import java.util.Map;
import java.util.List;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
//import org.apache.camel.Endpoint;
//import org.apache.camel.component.seda.SedaEndpoint;

import org.springframework.stereotype.Component;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component public class LoggingProcessor implements Processor{
	private static final Logger logger = LoggerFactory.getLogger(LoggingProcessor.class);
	
	public LoggingProcessor(){
	}
	
	private CamelContext camelContext;
	public void setCamelContext(CamelContext camelContext){
		this.camelContext = camelContext;
	}
	
	@Override public void process(Exchange exchange) throws Exception{
		@SuppressWarnings("unchecked") Map<String, List<String>> map = (Map<String, List<String>>)exchange.getIn().getBody();
		
		map.entrySet().parallelStream().forEach((e) -> {
			logger.debug("{}, {}", e.getKey(), e.getValue());
		});
	}
}

