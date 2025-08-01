package local.minkabu.jgate.service;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
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
import org.apache.commons.lang3.math.NumberUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import local.minkabu.jgate.mapper.JGATEMapper;
import local.minkabu.jgate.service.FieldUtils;

@Service public class DA122Service{
	private static final Logger logger = LoggerFactory.getLogger(DA122Service.class);
	
	private JGATEMapper mapper;
	public void setMapper(JGATEMapper mapper){
		this.mapper = mapper;
	}
	
	private RedisClient redisClient;
	public void setRedisClient(RedisClient redisClient){
		this.redisClient = redisClient;
	}
	
	public DA122Service(){
	}
	
	public void init(){
	}
	
	public void close(){
	}
	
	private static final String STEP_SIZE = "step_size";
	private static final String LOWER_LIMIT = "lower_limit";
	private static final String UPPER_LIMIT = "upper_limit";
	private static final String IS_FRACTIONS = "is_fractions";
	private static final String DEC_IN_PREMIUM = "dec_in_premium";
	
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
			save(stream);
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
					
					// .(ドット)を含むキーを _(アンダースコア)でリプレース
					Map<String, String> data = new HashMap<String, String>();
					for(Map.Entry<String, String> e : body.entrySet()){
						String key = e.getKey();
						String value = e.getValue();
						
						if(StringUtils.contains(key, '.')){
							data.put(StringUtils.replaceChars(key, '.', '_'), value);
						}else{
							data.put(key, value);
						}
					}
					
					data.put("msec", messageId[0]);
					try{
						mapper.instClassBasic(data);
					}catch(Exception e){
						logger.error(e.getMessage());
					}
					
					
					// 数値始まりのデータ(PriceTick)処理
					int i = 0;
					while(body.containsKey(StringUtils.join(String.valueOf(i), ".", STEP_SIZE))){
						Map<String, String> m = new HashMap<String, String>();
						
						m.put("country", body.get("series.country"));
						m.put("market", body.get("series.market"));
						m.put("instrument", body.get("series.instrument_group"));
						m.put("modifier", body.get("series.modifier"));
						m.put("commodity", body.get("series.commodity"));
						m.put("expiration", body.get("series.expiration_date"));
						m.put("strike", body.get("series.strike_price"));
						
						m.put(STEP_SIZE, body.get(StringUtils.join(String.valueOf(i), ".", STEP_SIZE)));
						m.put(LOWER_LIMIT, body.get(StringUtils.join(String.valueOf(i), ".", LOWER_LIMIT)));
						m.put(UPPER_LIMIT, body.get(StringUtils.join(String.valueOf(i), ".", UPPER_LIMIT)));
						m.put(IS_FRACTIONS, body.get(StringUtils.join(String.valueOf(i), ".", IS_FRACTIONS)));
						
						m.put(DEC_IN_PREMIUM, body.get(DEC_IN_PREMIUM));
						
						m.put("msec", messageId[0]);
						try{
							mapper.priceTick(m);
						}catch(Exception e){
							logger.error(e.getMessage());
						}
				
						i++;
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
		
		FieldUtils.moveSeries(headers, body);
		String country = FieldUtils.country(headers);
		String market = FieldUtils.market(headers);
		String instrumentGroup = FieldUtils.instrument(headers);
		String modifier = FieldUtils.modifier(headers);
		String commodity = FieldUtils.commodity(headers);
		String expirationDate = FieldUtils.expiration(headers);
		String strikePrice = FieldUtils.strike(headers);
		
		// .(ドット)を含むキーを _(アンダースコア)でリプレース
		Map<String, String> data = body.entrySet().stream().collect(Collectors.toMap(entry -> StringUtils.replaceChars(entry.getKey(), '.', '_'), entry -> entry.getValue()));
		
		FieldUtils.put(data, headers);
		//FieldUtils.put(data, country, market, instrumentGroup, modifier, commodity, expirationDate, strikePrice);
		/*
		data.put(FieldUtils.COUNTRY, country);
		data.put(FieldUtils.MARKET, market);
		data.put(FieldUtils.INSTRUMENT, instrumentGroup);
		data.put(FieldUtils.MODIFIER, modifier);
		data.put(FieldUtils.COMMODITY, commodity);
		data.put(FieldUtils.EXPIRATION, expirationDate);
		data.put(FieldUtils.STRIKE, strikePrice);
		*/
		
		data.put(FieldUtils.MSEC, Long.toString(msec));
		try{
			mapper.instClassBasic(data);
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		// 数値始まりのデータ(PriceTick)処理
		Map<String, String> m = new HashMap<String, String>();
		
		FieldUtils.put(m, headers);
		/*
		m.put(FieldUtils.COUNTRY, country);
		m.put(FieldUtils.MARKET, market);
		m.put(FieldUtils.INSTRUMENT, instrumentGroup);
		m.put(FieldUtils.MODIFIER, modifier);
		m.put(FieldUtils.COMMODITY, commodity);
		m.put(FieldUtils.EXPIRATION, expirationDate);
		m.put(FieldUtils.STRIKE, strikePrice);
		*/
		
		int i = 0;
		while(body.containsKey(StringUtils.join(String.valueOf(i), ".", STEP_SIZE))){
			
			m.put(STEP_SIZE, body.get(StringUtils.join(String.valueOf(i), ".", STEP_SIZE)));
			m.put(LOWER_LIMIT, body.get(StringUtils.join(String.valueOf(i), ".", LOWER_LIMIT)));
			m.put(UPPER_LIMIT, body.get(StringUtils.join(String.valueOf(i), ".", UPPER_LIMIT)));
			m.put(IS_FRACTIONS, body.get(StringUtils.join(String.valueOf(i), ".", IS_FRACTIONS)));
			
			m.put(DEC_IN_PREMIUM, body.get(DEC_IN_PREMIUM));
			
			m.put(FieldUtils.MSEC, Long.toString(msec));
			try{
				mapper.priceTick(m);
			}catch(Exception e){
				logger.error(e.getMessage());
			}
		
			i++;
		}
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		return;
	}
}

