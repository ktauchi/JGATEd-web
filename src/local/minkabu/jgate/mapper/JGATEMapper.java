package local.minkabu.jgate.mapper;

import java.util.Map;

public interface JGATEMapper{
	
	void underlyingBasic(Map<String, String> map);
	
	void instClassBasic(Map<String, String> map);
	
	void priceTick(Map<String, String> map);
	
	void instSeriesBasic(Map<String, String> map);
	
	void exchangeDa24(Map<String, String> map);
	
	void dealSource(Map<String, String> map);
	
	void clearingDate(Map<String, String> map);
	
	void nonTradingDays(Map<String, String> map);
}

