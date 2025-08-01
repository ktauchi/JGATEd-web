package local.minkabu.jgate.processor;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
//import org.apache.camel.Endpoint;
//import org.apache.camel.component.seda.SedaEndpoint;

import org.springframework.stereotype.Component;

import org.msgpack.core.MessagePack.PackerConfig;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component public class UnpackProcessor implements Processor{
	private static final Logger logger = LoggerFactory.getLogger(UnpackProcessor.class);
	
	private ObjectMapper objectMapper;
	
	private TypeReference typeReference;
	
	private CamelContext camelContext;
	
	public void setCamelContext(CamelContext camelContext){
		this.camelContext = camelContext;
	}
	
	public UnpackProcessor(){
		PackerConfig packerConfig = new PackerConfig();
		packerConfig.withStr8FormatSupport(false);

		this.objectMapper = new ObjectMapper(new MessagePackFactory(packerConfig));
		this.typeReference = new TypeReference<Map<String, String>>(){};
	}

	@Override public void process(Exchange exchange) throws Exception {
		byte[] body = (byte[])exchange.getIn().getBody(byte[].class);
		
		@SuppressWarnings("unchecked") Map<String, String> map = (Map<String, String>)objectMapper.readValue(body, typeReference);
		
		//logger.debug("{}", map);
		
		exchange.getIn().setBody(map);
	}
}

