package local.minkabu.jgate.service;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

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
import org.apache.commons.lang3.math.NumberUtils;

import com.tibco.tibrv.TibrvRvdTransport;
import com.tibco.tibrv.TibrvMsg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service public class RvdService{
	private static final Logger logger = LoggerFactory.getLogger(RvdService.class);
	
	/*
	private RedisClient streamRedisClient;
	public void setStreamRedisClient(RedisClient redisClient){
		this.streamRedisClient = redisClient;
	}
	
	private int block;
	public void setBlock(int block){
		this.block = block;
	}
	
	private int limit;
	public void setLimit(int limit){
		this.limit = limit;
	}
	
	private int db;
	public void setDb(int db){
		this.db = db;
	}
	
	private RedisClient localRedisClient;
	public void setLocalRedisClient(RedisClient redisClient){
		this.localRedisClient = redisClient;
	}
	
	private int store;
	public void setStore(int store){
		this.store = store;
	}
	*/
	
	private TibrvRvdTransport rvdTransport;
	public void setRvdTransport(TibrvRvdTransport rvdTransport){
		this.rvdTransport = rvdTransport;
	}
	
	private String prefix = StringUtils.EMPTY;
	public void setPrefix(String prefix){
		this.prefix = prefix;
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
	*/
	/*
	private boolean isRunning = false;
	public void timer(@Body Exchange exchange){
		String stream = (String)exchange.getProperty(Exchange.TIMER_NAME);
		if(StringUtils.isEmpty(stream)){
			return;
		}
		String subject = String.format("%s.%s", prefix, stream);
		
		if(isRunning){
			// 別スレッドで動作しているため、lastMessageId のみ保存して終了
			logger.info("stream: {} is running, lastMessageId: {}", stream, lastMessageId);
			save(subject);
			return;
		}
		isRunning = true;
		
		// XXX: keys name is subject
		try(StatefulRedisConnection<String, String> redisConnection = localRedisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			if(null == lastMessageId){
				redisCommands.select(store);
				String id = redisCommands.get(subject);
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
					
					TibrvMsg msg = new TibrvMsg();
					
					for(Map.Entry<String, String> e : body.entrySet()){
						msg.add(e.getKey(), e.getValue());
					}
					msg.setSendSubject(subject);
					
					rvdTransport.send(msg);
				}	//for
				
				logger.info("{}: {}, stream: {}, size: {}, lastMessageId: {}, subject: {}", Exchange.TIMER_COUNTER, exchange.getProperty(Exchange.TIMER_COUNTER), stream, messages.size(), lastMessageId, subject);
			} // while
			
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		isRunning = false;
		if(isSave){
			save(subject);
		}
	}
	*/
	
	public void publish(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = StringUtils.join(prefix, ".", StringUtils.substringAfterLast((String)headers.get(NatsConstants.NATS_SUBJECT), "."));
		logger.debug("rvd: {}, body: {}", subject, body);
		try{
			TibrvMsg msg = new TibrvMsg();
			
			msg.setSendSubject(subject);
			for(Map.Entry<String, String> e : body.entrySet()){
				msg.add(e.getKey(), StringUtils.defaultString(e.getValue()));
			}
			
			rvdTransport.send(msg);
		}catch(Exception e){
			logger.error("{}, {}, {}", e.getMessage(), subject, body);
		}
		
		return;
	}
}

