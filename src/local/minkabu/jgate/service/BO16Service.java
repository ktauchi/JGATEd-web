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

// BO16, BO16@BID / BO16@ASK
@Service public class BO16Service{
	private static final Logger logger = LoggerFactory.getLogger(BO16Service.class);
	
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
	public BO16Service(){
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
	
	boolean isRunning = false;
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
					
					long msec = NumberUtils.toLong(messageId[0]);
					
					Number[] p = {null, null, null, null, null, null, null, null, null, null};
					Number[] q = {null, null, null, null, null, null, null, null, null, null};
					
					for(int i = 0; i < 10; i++){
						String pKey = String.format("%d.price", i);
						String qKey = String.format("%d.quantity", i);
						
						if(body.containsKey(pKey)){
							try{
								p[i] = toNumber(body.get(pKey));
							}catch(Exception e){
							}
						}else{
							break;
						}
						
						if(body.containsKey(qKey)){
							try{
								q[i] = toNumber(body.get(qKey));
							}catch(Exception e){
							}
						}
					}
					
					long sn = NumberUtils.toLong(body.get("synchronization_number"), 0);
					
					long nsec = (msec * 1_000_000) + (sn % 1_000_000_000);
					
					Point point = Point.measurement(measurement)
						.time(nsec, TimeUnit.NANOSECONDS)
						.tag("country", body.get("series.country"))
						.tag("market", body.get("series.market"))
						.tag("instrument", body.get("series.instrument_group"))
						.tag("modifier", body.get("series.modifier"))
						.tag("commodity", body.get("series.commodity"))
						.tag("expiration", body.get("series.expiration_date"))
						.tag("strike", body.get("series.strike_price"))
						.addField("nsec", nsec)
						.addField("p0", p[0])
						.addField("p1", p[1])
						.addField("p2", p[2])
						.addField("p3", p[3])
						.addField("p4", p[4])
						.addField("p5", p[5])
						.addField("p6", p[6])
						.addField("p7", p[7])
						.addField("p8", p[8])
						.addField("p9", p[9])
						.addField("q0", q[0])
						.addField("q1", q[1])
						.addField("q2", q[2])
						.addField("q3", q[3])
						.addField("q4", q[4])
						.addField("q5", q[5])
						.addField("q6", q[6])
						.addField("q7", q[7])
						.addField("q8", q[8])
						.addField("q9", q[9])
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
		
		long sn = NumberUtils.toLong(body.get("synchronization_number"), 0);
		long nsec = (msec * 1_000_000) + (sn % 1_000_000_000);
		
		FieldUtils.moveSeries(headers, body);
		/*
		headers.put(FieldUtils.COUNTRY, FieldUtils.removeSeriesCountry(body);
		headers.put(FieldUtils.MARKET, FieldUtils.removeSeriesMarket(body);
		headers.put(FieldUtils.INSTRUMENT, FieldUtils.removeSeriesInstrumentGroup(body);
		headers.put(FieldUtils.MODIFIER, FieldUtils.removeSeriesModifier(body);
		headers.put(FieldUtils.COMMODITY, FieldUtils.removeSeriesCommodity(body);
		headers.put(FieldUtils.EXPIRATION, FieldUtils.removeSeriesExpirationDate(body);
		headers.put(FieldUtils.STRIKE, FieldUtils.removeSeriesStrikePrice(body);
		*/
		
		Map<String, String> data = new HashMap<>();
		for(int i = 0; i < 10; i++){
			String pKey = String.format("%d.price", i);
			String qKey = String.format("%d.quantity", i);
			
			if(body.containsKey(pKey)){
				try{
					//p[i] = toNumber(body.get(pKey));
					Number p = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(pKey), null));
					data.put(String.format("%s.p%d", measurement, i), String.valueOf(p));
				}catch(Exception e){
					logger.error(e.getMessage());
				}
			}else{
				break;
			}
			
			if(body.containsKey(qKey)){
				try{
					//q[i] = toNumber(body.get(qKey));
					Number q = NumberUtils.createNumber(StringUtils.defaultIfEmpty(body.get(qKey), null));
					data.put(String.format("%s.q%d", measurement, i), String.valueOf(q));
				}catch(Exception e){
					logger.error(e.getMessage());
				}
			}
		}
		
		exchange.getIn().setBody(data);
		
		return;
	}
	
	public void redis(Exchange exchange, @Headers Map<String, Object> headers, @Body Map<String, String> body){
		String subject = (String)headers.get(NatsConstants.NATS_SUBJECT);
		String series = FieldUtils.series(headers);
		
		logger.debug("subject: {}, series: {}, body: {}", subject, series, body);
		
		Map<String, String> data = new HashMap<>(body);
		
		try(StatefulRedisConnection<String, String> redisConnection = redisClient.connect()){
			RedisCommands<String, String> redisCommands = redisConnection.sync();
			redisCommands.hmset(series, data);
		}catch(Exception e){
			logger.error(e.getMessage());
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
			.addField(FieldUtils.NSEC, (long)headers.get(FieldUtils.NSEC))
		;
		
		influxdb.write(udpPort, pointBuilder.build());
	}
}

