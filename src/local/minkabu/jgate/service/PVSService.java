package local.minkabu.jgate.service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Date;
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

// Price Volatility / Settlement Service, EB10 / RA62
@Service public class PVSService{
	private static final Logger logger = LoggerFactory.getLogger(PVSService.class);
	
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
	
	public PVSService(){
	}
	
	public void init(){
	}
	
	public void close(){
	}
	
	/*
	private Number toNumber(String s){
		Number n = null;
		try{
			n = NumberUtils.createNumber(s);
		}catch(Exception e){
		}
		return n;
	}
	
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
					
					String[] messageId = StringUtils.split(lastMessageId, '-');
					
					long ms = NumberUtils.toLong(messageId[0]);
					
					Number lastp = toNumber(body.get("last_price"));
					Number settlep = toNumber(body.get("settle_price"));
					Number actualv = toNumber(body.get("actual_vol"));
					Number theop = toNumber(body.get("theo_price"));
					Number curvwv = toNumber(body.get("current_vwv"));
					Number calvwv = toNumber(body.get("calc_vwv"));
					Number prevwv = toNumber(body.get("prev_vwv"));
					Number rfr = toNumber(body.get("risk_free_rate"));
					Number ulgp = toNumber(body.get("ulg_price"));
					
					// EB10
					Number implv = toNumber(body.get("implied_vol"));
					long spt = NumberUtils.toLong(body.get("settlement_price_type"), 0);
					
					// RA62
					Number divy = toNumber(body.get("dividend_yield"));
					Number divs = toNumber(body.get("dividend_sum"));
					
					Point point = Point.measurement(measurement)
						.time(ms, TimeUnit.MILLISECONDS)
						.tag("country", body.get("series.country"))
						.tag("market", body.get("series.market"))
						.tag("instrument", body.get("series.instrument_group"))
						.tag("modifier", body.get("series.modifier"))
						.tag("commodity", body.get("series.commodity"))
						.tag("expiration", body.get("series.expiration_date"))
						.tag("strike", body.get("series.strike_price"))
						.addField("ms", ms)
						.addField("lastp", lastp)
						.addField("settlep", settlep)
						.addField("actualv", actualv)
						.addField("theop", theop)
						.addField("curvwv", curvwv)
						.addField("calvwv", calvwv)
						.addField("prevwv", prevwv)
						.addField("rfr", rfr)
						.addField("ulgp", ulgp)
						.addField("implv", implv)
						.addField("spt", spt)
						.addField("divy", divy)
						.addField("divs", divs)
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
		
		Number lastp = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("last_price"), null));
		Number settlep = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("settle_price"), null));
		Number actualv = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("actual_vol"), null));
		Number theop = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("theo_price"), null));
		Number curvwv = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("current_vwv"), null));
		Number calvwv = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("calc_vwv"), null));
		Number prevwv = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("prev_vwv"), null));
		Number rfr = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("risk_free_rate"), null));
		Number ulgp = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("ulg_price"), null));
		
		// EB10
		Number implv = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("implied_vol"), null));
		long spt = NumberUtils.toLong(body.get("settlement_price_type"), 0);
		
		// RA62
		Number divy = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("dividend_yield"), null));
		Number divs = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get("dividend_sum"), null));
		
		Point.Builder pointBuilder = Point.measurement(measurement)
			.time(msec, TimeUnit.MILLISECONDS)
			.tag(FieldUtils.COUNTRY, country)
			.tag(FieldUtils.MARKET, market)
			.tag(FieldUtils.INSTRUMENT, instrumentGroup)
			.tag(FieldUtils.MODIFIER, modifier)
			.tag(FieldUtils.COMMODITY, commodity)
			.tag(FieldUtils.EXPIRATION, expirationDate)
			.tag(FieldUtils.STRIKE, strikePrice)
			.addField("ms", msec)
			.addField("lastp", lastp)
			.addField("settlep", settlep)
			.addField("actualv", actualv)
			.addField("theop", theop)
			.addField("curvwv", curvwv)
			.addField("calvwv", calvwv)
			.addField("prevwv", prevwv)
			.addField("rfr", rfr)
			.addField("ulgp", ulgp)
			.addField("implv", implv)
			.addField("spt", spt)
			.addField("divy", divy)
			.addField("divs", divs)
		;
		influxdb.write(udpPort, pointBuilder.build());
		
		return;
	}
}

