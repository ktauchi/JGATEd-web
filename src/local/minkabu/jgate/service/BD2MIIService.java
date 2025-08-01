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

@Service public class BD2MIIService{
	private static final Logger logger = LoggerFactory.getLogger(BD2MIIService.class);
	
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
	
	public BD2MIIService(){
	}
	
	private SimpleDateFormat distSimpleDateFormat = null;
	public void init(){
		distSimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		distSimpleDateFormat.setTimeZone(FastTimeZone.getGmtTimeZone());
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
					
					long msec = System.currentTimeMillis();
					String dist = body.get("date_time_of_dist");
					if(!StringUtils.isEmpty(dist)){
						try{
							Date date = distSimpleDateFormat.parse(dist);
							msec = date.getTime();
						}catch(Exception e){
						}
					}
					
					Number open = toNumber(body.get("opening_price"));
					Number high = toNumber(body.get("high_price"));
					Number low = toNumber(body.get("low_price"));
					Number last = toNumber(body.get("last_price"));
					Number close = toNumber(body.get("closing_price"));
					
					Point point = Point.measurement(measurement)
						.time(msec, TimeUnit.MILLISECONDS)
						.tag("country", body.get("series.country"))
						.tag("market", body.get("series.market"))
						.tag("instrument", body.get("series.instrument_group"))
						.tag("modifier", body.get("series.modifier"))
						.tag("commodity", body.get("series.commodity"))
						.tag("expiration", body.get("series.expiration_date"))
						.tag("strike", body.get("series.strike_price"))
						.addField("open", open)
						.addField("high", high)
						.addField("low", low)
						.addField("last", last)
						.addField("close", close)
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
	
	private static final String OPEN = "open";
	private static final String HIGH = "high";
	private static final String LOW = "low";
	private static final String LAST = "last";
	private static final String CLOSE = "close";
	
	public void parse(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		long msec = (long)headers.get(NatsConstants.NATS_MESSAGE_TIMESTAMP);
		
		String dist = body.get("date_time_of_dist");
		if(!StringUtils.isEmpty(dist)){
			try{
				Date date = distSimpleDateFormat.parse(dist);
				msec = date.getTime();
			}catch(Exception e){
				logger.error(e.getMessage());
			}
		}
		headers.put(FieldUtils.MSEC, msec);
		
		FieldUtils.moveSeries(headers, body);
		
		Map<String, String> data = new HashMap<String, String>(){
			{
				put(OPEN, body.get("opening_price"));
				put(HIGH, body.get("high_price"));
				put(LOW, body.get("low_price"));
				put(LAST, body.get("last_price"));
				put(CLOSE, body.get("closing_price"));
			}
		};
		
		exchange.getIn().setBody(data);
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		String series = FieldUtils.series(headers);
		
		logger.debug("subject: {}, series: {}, body: {}", subject, series, body);
		
		Map<String, String> data = body.entrySet().stream().collect(Collectors.toMap(entry -> StringUtils.join(FieldUtils.bd2, ".", entry.getKey()), entry -> entry.getValue()));
		data.put(StringUtils.join(FieldUtils.bd2, ".", FieldUtils.MSEC), Long.toString((long)headers.get(FieldUtils.MSEC)));
		
		try(StatefulRedisConnection<String, String> redisConnection = redisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			redisCommands.hmset(series, data);
		}catch(Exception e){
			logger.debug(e.getMessage());
		}
		
		return;
	}
	
	public void line(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		logger.debug("subject: {}, body: {}", subject, body);
		
		Point.Builder pointBuilder = Point.measurement(measurement)
			.time((long)headers.get(FieldUtils.MSEC), TimeUnit.MILLISECONDS)
			.tag(FieldUtils.COUNTRY, (String)headers.get(FieldUtils.COUNTRY))
			.tag(FieldUtils.MARKET, (String)headers.get(FieldUtils.MARKET))
			.tag(FieldUtils.INSTRUMENT, (String)headers.get(FieldUtils.INSTRUMENT))
			.tag(FieldUtils.MODIFIER, (String)headers.get(FieldUtils.MODIFIER))
			.tag(FieldUtils.COMMODITY, (String)headers.get(FieldUtils.COMMODITY))
			.tag(FieldUtils.EXPIRATION, (String)headers.get(FieldUtils.EXPIRATION))
			.tag(FieldUtils.STRIKE, (String)headers.get(FieldUtils.STRIKE))
		;
		
		body.entrySet().forEach(e -> {
			pointBuilder.addField(e.getKey(), NumberUtils.createNumber(StringUtils.defaultIfEmpty(e.getValue(), null)));
		});
		
		influxdb.write(udpPort, pointBuilder.build());
		
		return;
	}
}

