package local.minkabu.jgate.service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import org.apache.camel.Headers;
import org.apache.camel.Body;
import org.apache.camel.Exchange;

import org.apache.camel.component.nats.NatsConstants;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs.Builder;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.RedisCommandTimeoutException;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import local.minkabu.jgate.mapper.JGATEMapper;

@Service public class DA120Service{
	private static final Logger logger = LoggerFactory.getLogger(DA120Service.class);
	
	private JGATEMapper mapper;
	public void setMapper(JGATEMapper mapper){
		this.mapper = mapper;
	}
	
	private RedisClient redisClient;
	public void setRedisClient(RedisClient redisClient){
		this.redisClient = redisClient;
	}
	
	public DA120Service(){
	}
	
	public void init(){
	}
	
	public void close(){
	}
	
	/*
	private String lastMessageId = null;
	private void save(String key){
		if(!StringUtils.isEmpty(lastMessageId)){
			try(StatefulRedisConnection<String, String> redisConnection = localRedisClient.connect()){
				RedisCommands<String, String> redisCommands = redisConnection.sync();
				redisCommands.select(store);
				redisCommands.set(key, lastMessageId);
			}catch(Exception e){
				logger.error(e.getMessage());
			}
		}
		
		return;
	}
	
	private boolean isRunning = false;
	public void timer(@Body Exchange exchange){
		String stream = (String)exchange.getProperty(Exchange.TIMER_NAME);
		if(StringUtils.isEmpty(stream)){
			return;
		}
		
		if(isRunning){
			// 別スレッドで動作しているため、lastMessageId のみ保存して終了
			logger.info("stream: {} is running, lastMessageId: {}", stream, lastMessageId);
			return;
		}
		isRunning = true;
		
		try(StatefulRedisConnection<String, String> redisConnection = localRedisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			if(null == lastMessageId){
				redisCommands.select(store);
				String id = redisCommands.get(stream);
				lastMessageId = StringUtils.defaultIfBlank(id, "0-0");
			}
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		boolean isSave = false;
		try(StatefulRedisConnection<String, String> redisConnection = streamRedisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			
			redisCommands.select(db);
			boolean isNext = true;
			while(isNext){
				List<StreamMessage<String, String>> messages = redisCommands.xread(Builder.block(block).count(limit), StreamOffset.from(stream, lastMessageId));
				
				if(null == messages || 0 == messages.size()){ break; }
				isSave = true;
				
				for(StreamMessage<String, String> message : messages){
					lastMessageId = message.getId();
					Map<String, String> body = message.getBody();
					
					String[] messageId = StringUtils.split(lastMessageId, '-');
					
					body.put("msec", messageId[0]);
					try{
						mapper.underlyingBasic(body);
					}catch(Exception e){
						logger.error(e.getMessage());
					}
				}	// for
				
				logger.info("{}: {}, stream: {}, size: {}, lastMessageId: {}", Exchange.TIMER_COUNTER, exchange.getProperty(Exchange.TIMER_COUNTER), stream, messages.size(), lastMessageId);
			}	// while
			
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		isRunning = false;
		if(isSave){
			save(stream);
		}
	}
	*/
	
	public void parse(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		long msec = (long)headers.get(NatsConstants.NATS_MESSAGE_TIMESTAMP);
		
		Map<String, String> map = new HashMap<String, String>(body);
		
		//Map<String, String> data = new HashMap<>(body);
		// .(ドット)を含むキーを _(アンダースコア)でリプレース
		Map<String, String> data = map.entrySet().stream().collect(Collectors.toMap(entry -> StringUtils.replaceChars(entry.getKey(), '.', '_'), entry -> entry.getValue()));
		
		data.put(FieldUtils.MSEC, Long.toString(msec));
		try{
			mapper.underlyingBasic(data);
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		return;
	}
}

