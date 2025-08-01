package local.minkabu.jgate.service;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class FieldUtils{
	public static final String SERIES_COUNTRY = "series.country";
	public static final String COUNTRY = "country";
	public static String country(Map<String, Object> headers){
		return (String)headers.get(COUNTRY);
	}
	
	public static final String SERIES_MARKET = "series.market";
	public static final String MARKET = "market";
	public static String market(Map<String, Object> headers){
		return (String)headers.get(MARKET);
	}
	
	public static final String SERIES_INSTRUMENT_GROUP = "series.instrument_group";
	public static final String INSTRUMENT = "instrument";
	public static String instrument(Map<String, Object> headers){
		return (String)headers.get(INSTRUMENT);
	}
	
	public static final String SERIES_MODIFIER = "series.modifier";
	public static final String MODIFIER = "modifier";
	public static String modifier(Map<String, Object> headers){
		return (String)headers.get(MODIFIER);
	}
	
	public static final String SERIES_COMMODITY = "series.commodity";
	public static final String COMMODITY = "commodity";
	public static final String commodity(Map<String, Object> headers){
		return (String)headers.get(COMMODITY);
	}
	
	public static final String SERIES_EXPIRATION_DATE = "series.expiration_date";
	public static final String EXPIRATION = "expiration";
	public static String expiration(Map<String, Object> headers){
		return (String)headers.get(EXPIRATION);
	}
	
	public static final String SERIES_STRIKE_PRICE = "series.strike_price";
	public static final String STRIKE = "strike";
	public static String strike(Map<String, Object> headers){
		return (String)headers.get(STRIKE);
	}
	
	public static void moveSeries(Map<String, Object> headers, Map<String, String> body){
		headers.put(COUNTRY, body.remove(SERIES_COUNTRY));
		headers.put(MARKET, body.remove(SERIES_MARKET));
		headers.put(INSTRUMENT, body.remove(SERIES_INSTRUMENT_GROUP));
		headers.put(MODIFIER, body.remove(SERIES_MODIFIER));
		headers.put(COMMODITY, body.remove(SERIES_COMMODITY));
		headers.put(EXPIRATION, body.remove(SERIES_EXPIRATION_DATE));
		headers.put(STRIKE, body.remove(SERIES_STRIKE_PRICE));
	}
	
	public static final String SERIES_SEPARATOR = "-";
	public static String series(Map<String, Object> headers){
		return StringUtils.join((String)headers.get(COUNTRY)
			, SERIES_SEPARATOR, (String)headers.get(MARKET)
			, SERIES_SEPARATOR, (String)headers.get(INSTRUMENT)
			, SERIES_SEPARATOR, (String)headers.get(MODIFIER)
			, SERIES_SEPARATOR, (String)headers.get(COMMODITY)
			, SERIES_SEPARATOR, (String)headers.get(EXPIRATION)
			, SERIES_SEPARATOR, (String)headers.get(STRIKE)
		);
	}
	
	public static void put(Map<String, String> map, Map<String, Object> headers){
		map.put(COUNTRY, (String)headers.get(COUNTRY));
		map.put(MARKET, (String)headers.get(MARKET));
		map.put(INSTRUMENT, (String)headers.get(INSTRUMENT));
		map.put(MODIFIER, (String)headers.get(MODIFIER));
		map.put(COMMODITY, (String)headers.get(COMMODITY));
		map.put(EXPIRATION, (String)headers.get(EXPIRATION));
		map.put(STRIKE, (String)headers.get(STRIKE));
	}
	
	public static final String bd2 = "bd2";
	public static final String bd70 = "bd70";
	
	public static final String MSEC = "msec";
	public static final String NSEC = "nsec";
	public static final String HHMMSS = "hhmmss";
	
	public static final String OPEN = "open";
	public static final String HIGH = "high";
	public static final String LOW = "low";
	public static final String LAST = "last";
	public static final String CLOSE = "close";
	public static final String VOLUME = "volume";
	public static final String TURNOVER = "turnover";
	public static final String INDICATOR = "indicator";
	
	public static final String OPENING_PRICE = "opening_price";
	public static final String HIGH_PRICE = "high_price";
	public static final String LOW_PRICE = "low_price";
	public static final String LAST_PRICE = "last_price";
	public static final String TREND_INDICATOR = "trend_indicator";
}

