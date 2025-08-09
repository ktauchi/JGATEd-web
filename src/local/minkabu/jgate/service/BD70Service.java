package local.minkabu.jgate.service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.stream.Collectors;
import java.text.SimpleDateFormat;

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

import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.BatchPoints;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.FastTimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import local.minkabu.jgate.service.FieldUtils;

@Service public class BD70Service{
	private static final Logger logger = LoggerFactory.getLogger(BD70Service.class);
	
	private RedisClient redisClient;
	public void setRedisClient(RedisClient redisClient){
		this.redisClient = redisClient;
	}
	
	private InfluxDB influxdb;
	public void setInfluxDB(InfluxDB influxdb){
		this.influxdb = influxdb;
	}
	
	private int udpPort;
	public void setUdpPort(int udpPort){
		this.udpPort = udpPort;
	}
	
	private String measurement;
	public void setMeasurement(String measurement){
		this.measurement = measurement;
	}
	
	public BD70Service(){
	}
	
	public void init(){
	}
	
	public void close(){
	}
	
	private Number toNumber(String s){
		Number n = null;
		try{
			n = NumberUtils.createNumber(s);
		}catch(Exception e){
		}
		return n;
	}
	
	private boolean isRunning = false;
	public void timer(@Body Exchange exchange){
		/*
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
			
			InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
			
			boolean isNext = true;
			while(isNext){
				List<StreamMessage<String, String>> messages = redisCommands.xread(Builder.block(block).count(limit), StreamOffset.from(stream, lastMessageId));
				
				if(null == messages || 0 == messages.size()){ break; }
				isSave = true;
				
				BatchPoints batchPoints = BatchPoints.database(database).retentionPolicy(retentionPolicy).build();
				for(StreamMessage<String, String> message : messages){
					lastMessageId = message.getId();
					Map<String, String> body = message.getBody();
					// 40件以上の注文での約定で、BD70 が分割される
					// 分割された場合、初回以降のデータについては、deal_quantityが0
					// segment_number で比較したいが、分割無し segment_number = 0, 分割有り segment_number = 1,2,3,...,0 となるため
					// deal_quantity で検出
					long quantity = 0;
					quantity = NumberUtils.toLong(body.get("deal_quantity"), 0);
					if(0 == quantity){
						continue;
					}
					
					long sec = NumberUtils.toLong(body.get("timestamp_match.tv_sec"));
					long nsec = NumberUtils.toLong(body.get("timestamp_match.tv_nsec"));
					
					nsec += sec * 1_000_000_000;
					
					// 同一 nsec で約定が発生する場合が有るため、execution_event_nbr の下9桁で ms,us,ns を代用
					long event = NumberUtils.toLong(body.get("execution_event_nbr"));
					
					long time = sec * 1_000_000_000 + (event % 1_000_000_000);
					
					Number price = toNumber(body.get("deal_price"));
					Number state = toNumber(body.get("state_number"));
			
					// J-NET 取引, 11 or 12, ref: 4.6.5
					long trt = NumberUtils.toLong(body.get("trade_report_type"), 0);
					
					Point point = Point.measurement(measurement)
						.time(time, TimeUnit.NANOSECONDS)
						.tag("country", body.get("series.country"))
						.tag("market", body.get("series.market"))
						.tag("instrument", body.get("series.instrument_group"))
						.tag("modifier", body.get("series.modifier"))
						.tag("commodity", body.get("series.commodity"))
						.tag("expiration", body.get("series.expiration_date"))
						.tag("strike", body.get("series.strike_price"))
						.addField("nsec", nsec)
						.addField("price", price)
						.addField("quantity", quantity)
						.addField("state", state)
						.addField("event", event)
						.addField("trt", trt)
						.build();
					batchPoints.point(point);
				}	// for
				influxDB.write(batchPoints);
				
				logger.info("{}: {}, stream: {}, size: {}, lastMessageId: {}", Exchange.TIMER_COUNTER, exchange.getProperty(Exchange.TIMER_COUNTER), stream, messages.size(), lastMessageId);
			}	// while
			
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		
		isRunning = false;
		if(isSave){
			save(stream);
		}
		*/
	}
	
	public void parse(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		long msec = (long)headers.get(NatsConstants.NATS_MESSAGE_TIMESTAMP);
		
		Map<String, String> map = new HashMap<String, String>(body);
		
		// 40件以上の注文での約定で、BD70 が分割される
		// 分割された場合、初回以降のデータについては、deal_quantityが0
		// segment_number で比較したいが、分割無し segment_number = 0, 分割有り segment_number = 1,2,3,...,0 となるため
		// deal_quantity で検出
		long quantity = 0;
		quantity = NumberUtils.toLong(map.get("deal_quantity"), 0);
		if(0 == quantity){
			return;
		}
		
		long sec = NumberUtils.toLong(map.get("timestamp_match.tv_sec"));
		long nsec = NumberUtils.toLong(map.get("timestamp_match.tv_nsec"));
		
		nsec += sec * 1_000_000_000;
		
		// 同一 nsec で約定が発生する場合が有るため、execution_event_nbr の下9桁で ms,us,ns を代用
		long event = NumberUtils.toLong(map.get("execution_event_nbr"));
		
		long ns = sec * 1_000_000_000 + (event % 1_000_000_000);
		
		Number price = NumberUtils.createNumber(StringUtils.defaultIfEmpty(map.get("deal_price"), null));
		Number state = NumberUtils.createNumber(StringUtils.defaultIfEmpty(map.get("state_number"), null));
		
		// J-NET 取引, 11 or 12, ref: 4.6.5
		long trt = NumberUtils.toLong(map.get("trade_report_type"), 0);
		
		FieldUtils.moveSeries(headers, map);
		
		String country = FieldUtils.country(headers);
		String market = FieldUtils.market(headers);
		String instrumentGroup = FieldUtils.instrument(headers);
		String modifier = FieldUtils.modifier(headers);
		String commodity = FieldUtils.commodity(headers);
		String expirationDate = FieldUtils.expiration(headers);
		String strikePrice = FieldUtils.strike(headers);
		
		Point.Builder pointBuilder = Point.measurement(measurement)
			.time(ns, TimeUnit.NANOSECONDS)
			.tag(FieldUtils.COUNTRY, country)
			.tag(FieldUtils.MARKET, market)
			.tag(FieldUtils.INSTRUMENT, instrumentGroup)
			.tag(FieldUtils.MODIFIER, modifier)
			.tag(FieldUtils.COMMODITY, commodity)
			.tag(FieldUtils.EXPIRATION, expirationDate)
			.tag(FieldUtils.STRIKE, strikePrice)
			.addField("nsec", nsec)
			.addField("price", price)
			.addField("quantity", quantity)
			.addField("state", state)
			.addField("event", event)
			.addField("trt", trt)
		;
		influxdb.write(udpPort, pointBuilder.build());
		
		//String series = FieldUtils.series(country, market, instrumentGroup, modifier, commodity, expirationDate, strikePrice);
		String series = FieldUtils.series(headers);
		
		Map<String, String> data = map.entrySet().stream().collect(Collectors.toMap(entry -> StringUtils.join(FieldUtils.bd70, ".", entry.getKey()), entry -> entry.getValue()));
		
		try(StatefulRedisConnection<String, String> redisConnection = redisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			redisCommands.hmset(series, data);
		}catch(Exception e){
			logger.debug(e.getMessage());
		}
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		return;
	}
	
	public void line(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		return;
	}
}

