package local.minkabu.jgate.service;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.util.Objects;
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

@Service public class BD2MIBService{
	private static final Logger logger = LoggerFactory.getLogger(BD2MIBService.class);
	
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
	
	public BD2MIBService(){
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
					
					String[] messageId = StringUtils.split(lastMessageId, '-');
					
					long ms = NumberUtils.toLong(messageId[0]);
					
					// JST, -6h offset, 日替わりが 06:00(オフライン時刻)になる様調整
					ms -= (9 + 6) * 60 * 60 * 1_000;
					
					ms -= (ms % (24 * 60 * 60 * 1_000));
					
					long hhmmss = NumberUtils.toLong(body.get("hhmmss"));
					if(hhmmss <= ((9 + 6) * 1_00_00)){	// オフセット分調整
						ms += 24 * 60 * 60 * 1_000;
					}
					
					int hh = (int)(hhmmss / 1_00_00);
					ms += (hh * 60 * 60 * 1_000);
					
					int mm = (int)((hhmmss % 1_00_00) / 1_00);
					ms += (mm * 60 * 1_000);
					
					int ss = (int)(hhmmss % 1_00);
					ms += ss * 1_000;
					
					// MILLISECONDS -> NANOSECONDS
					long ns = ms * 1_000 * 1_000;
					
					// MILLISECONDS, MICROSECONDS, NANOSECONDS を補完, number_of_deals(値付回数)を利用
					long nod = NumberUtils.toLong(body.get("number_of_deals"));
					
					ns += (nod % 1_000_000_000);
					
					logger.debug("messageId: {}, hhmmss: {}, ms: {}, ns: {}", messageId, hhmmss, ms, ns);
					
					Number open = toNumber(body.get("opening_price"));
					Number high = toNumber(body.get("high_price"));
					Number low = toNumber(body.get("low_price"));
					Number last = toNumber(body.get("last_price"));
					Number volume = toNumber(body.get("volume"));
					Number turnover = toNumber(body.get("turnover"));
					String indicator = body.get("trend_indicator");
					
					Point point = Point.measurement(measurement)
						.time(ns, TimeUnit.NANOSECONDS)
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
						.addField("volume", volume)
						.addField("turnover", turnover)
						.addField("indicator", indicator)
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
		
		// JST, -7h offset, 日替わりが 06:00-07:00(オフライン時刻)になる様調整
		msec -= (9 + 7) * 60 * 60 * 1_000;
		
		msec -= (msec % (24 * 60 * 60 * 1_000));
		
		long hhmmss = NumberUtils.toLong(body.get("hhmmss"));
		if(hhmmss <= ((9 + 7) * 1_00_00)){	// オフセット分調整
			msec += 24 * 60 * 60 * 1_000;
		}
		
		int hh = (int)(hhmmss / 1_00_00);
		msec += (hh * 60 * 60 * 1_000);
		
		int mm = (int)((hhmmss % 1_00_00) / 1_00);
		msec += (mm * 60 * 1_000);
		
		int ss = (int)(hhmmss % 1_00);
		msec += ss * 1_000;
		
		// MILLISECONDS -> NANOSECONDS
		long nsec = msec * 1_000 * 1_000;
		
		// MILLISECONDS, MICROSECONDS, NANOSECONDS を補完, number_of_deals(値付回数)を利用
		long nod = NumberUtils.toLong(body.get("number_of_deals"));
		
		nsec += (nod % 1_000_000_000);
		
		FieldUtils.moveSeries(headers, body);
		/*
		headers.put(FieldUtils.COUNTRY, FieldUtils.removeSeriesCountry(body));
		headers.put(FieldUtils.MARKET, FieldUtils.removeSeriesMarket(body));
		headers.put(FieldUtils.INSTRUMENT, FieldUtils.removeSeriesInstrumentGroup(body));
		headers.put(FieldUtils.MODIFIER, FieldUtils.removeSeriesModifier(body));
		headers.put(FieldUtils.COMMODITY, FieldUtils.removeSeriesCommodity(body));
		headers.put(FieldUtils.EXPIRATION, FieldUtils.removeSeriesExpirationDate(body));
		headers.put(FieldUtils.STRIKE, FieldUtils.removeSeriesStrikePrice(body));
		
		String series = FieldUtils.series(country, market, instrumentGroup, modifier, commodity, expirationDate, strikePrice);
		*/
		
		logger.debug("hhmmss: {}, msec: {}, nsec: {}", hhmmss, msec, nsec);
		headers.put(FieldUtils.NSEC, nsec);
		headers.put(FieldUtils.HHMMSS, hhmmss);
		
		Map<String, String> data = new HashMap<String, String>(){
			{
				put(FieldUtils.OPEN, body.get(FieldUtils.OPENING_PRICE));
				put(FieldUtils.HIGH, body.get(FieldUtils.HIGH_PRICE));
				put(FieldUtils.LOW, body.get(FieldUtils.LOW_PRICE));
				put(FieldUtils.LAST, body.get(FieldUtils.LAST_PRICE));
				put(FieldUtils.VOLUME, body.get(FieldUtils.VOLUME));
				put(FieldUtils.TURNOVER, body.get(FieldUtils.TURNOVER));
			}
		};
		
		headers.put(FieldUtils.INDICATOR, body.get("trend_indicator"));
		/*
			.addField("open", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.OPENING_PRICE), null)))
			.addField("high", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.HIGH_PRICE), null)))
			.addField("low", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.LOW_PRICE), null)))
			.addField("last", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.LAST_PRICE), null)))
			.addField("volume", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.VOLUME), null)))
			.addField("turnover", NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(FieldUtils.TURNOVER), null)))
			.addField("indicator", body.get(FieldUtils.TREND_INDICATOR))
		;
		*/
		
		exchange.getIn().setBody(data);
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		String series = FieldUtils.series(headers);
		
		Map<String, String> data = body.entrySet().stream().collect(Collectors.toMap(entry -> StringUtils.join(FieldUtils.bd2, ".", entry.getKey()), entry -> entry.getValue()));
		
		String hhmmss = Objects.toString(headers.get(FieldUtils.HHMMSS));
		
		data.put(StringUtils.join(FieldUtils.bd2, ".", FieldUtils.HHMMSS), StringUtils.defaultString(StringUtils.leftPad(hhmmss, 6, '0')));
		data.put(FieldUtils.INDICATOR, (String)headers.get(FieldUtils.INDICATOR));
		
		logger.debug("redis: {}, series: {}, body: {}", subject, series, body);
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
			.time((long)headers.get(FieldUtils.NSEC), TimeUnit.NANOSECONDS)
			.tag(FieldUtils.COUNTRY, FieldUtils.country(headers))
			.tag(FieldUtils.MARKET, FieldUtils.market(headers))
			.tag(FieldUtils.INSTRUMENT, FieldUtils.instrument(headers))
			.tag(FieldUtils.MODIFIER, FieldUtils.modifier(headers))
			.tag(FieldUtils.COMMODITY, FieldUtils.commodity(headers))
			.tag(FieldUtils.EXPIRATION, FieldUtils.expiration(headers))
			.tag(FieldUtils.STRIKE, FieldUtils.strike(headers))
		;
		body.entrySet().forEach(e -> {
			pointBuilder.addField(e.getKey(), NumberUtils.createNumber(StringUtils.defaultIfEmpty(e.getValue(), null)));
		});
		pointBuilder.addField(FieldUtils.INDICATOR, (String)headers.get(FieldUtils.INDICATOR));
		
		influxdb.write(udpPort, pointBuilder.build());
		
		return;
	}
}

