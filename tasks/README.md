
현재 적용 중인 정확한 제한 수치와 규칙은 항상 /api/v3/exchangeInfo 엔드포인트를 호출하여 rateLimits 배열을 확인하는 것이 가장 정확
# REQUEST_WEIGHT (요청 가중치)
기본 제한: 1분당 6,000 가중치 (IP 주소당)


# /api/v3/exchangeInfo
rateLimits array

# Error Logs
raise ValueError : 터미널 (or Celery worker 터미널) 


# 서버 시간 (GET /fapi/v1/time)
Request Weight : 1
Request Parameters : NONE

Response Example
{
  "serverTime": 1499827319559
}

b_servertime = requests.get("https://fapi.binance.com/fapi/v1/time")
l_servertime = int(time.time() * 1000)

b_servertime - l_servertime를 Latency 모니터링에 사용할 예정


# Current exchange trading rules and symbol information (GET /fapi/v1/exchangeInfo)
Request Weight : 1
Request Parameters : NONE

Response Example
{
	"exchangeFilters": [],
 	"rateLimits": [
 		{
 			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 2400,
   			"rateLimitType": "REQUEST_WEIGHT" 
   		},
  		{
  			"interval": "MINUTE",
   			"intervalNum": 1,
   			"limit": 1200,
   			"rateLimitType": "ORDERS"
   		}
   	],
 	"serverTime": 1565613908500,    // Ignore please. If you want to check current server time, please check via "GET /fapi/v1/time"
 	"assets": [ // assets information
 		{
 			"asset": "BTC",
   			"marginAvailable": true, // whether the asset can be used as margin in Multi-Assets mode
   			"autoAssetExchange": "-0.10" // auto-exchange threshold in Multi-Assets margin mode
   		},
 		{
 			"asset": "USDT",
   			"marginAvailable": true,
   			"autoAssetExchange": "0"
   		},
 		{
 			"asset": "BNB",
   			"marginAvailable": false,
   			"autoAssetExchange": null
   		}
   	],
 	"symbols": [
 		{
 			"symbol": "BLZUSDT",
 			"pair": "BLZUSDT",
 			"contractType": "PERPETUAL",
 			"deliveryDate": 4133404800000,
 			"onboardDate": 1598252400000,
 			"status": "TRADING",
 			"maintMarginPercent": "2.5000",   // ignore
 			"requiredMarginPercent": "5.0000",  // ignore
 			"baseAsset": "BLZ", 
 			"quoteAsset": "USDT",
 			"marginAsset": "USDT",
 			"pricePrecision": 5,	// please do not use it as tickSize
 			"quantityPrecision": 0, // please do not use it as stepSize
 			"baseAssetPrecision": 8,
 			"quotePrecision": 8, 
 			"underlyingType": "COIN",
 			"underlyingSubType": ["STORAGE"],
 			"settlePlan": 0,
 			"triggerProtect": "0.15", // threshold for algo order with "priceProtect"
 			"filters": [
 				{
 					"filterType": "PRICE_FILTER",
     				"maxPrice": "300",
     				"minPrice": "0.0001", 
     				"tickSize": "0.0001"
     			},
    			{
    				"filterType": "LOT_SIZE", 
     				"maxQty": "10000000",
     				"minQty": "1",
     				"stepSize": "1"
     			},
    			{
    				"filterType": "MARKET_LOT_SIZE",
     				"maxQty": "590119",
     				"minQty": "1",
     				"stepSize": "1"
     			},
     			{
    				"filterType": "MAX_NUM_ORDERS",
    				"limit": 200
  				},
  				{
    				"filterType": "MAX_NUM_ALGO_ORDERS",
    				"limit": 10
  				},
  				{
  					"filterType": "MIN_NOTIONAL",
  					"notional": "5.0", 
  				},
  				{
    				"filterType": "PERCENT_PRICE",
    				"multiplierUp": "1.1500",
    				"multiplierDown": "0.8500",
    				"multiplierDecimal": "4"
    			}
   			],
 			"OrderType": [
   				"LIMIT",
   				"MARKET",
   				"STOP",
   				"STOP_MARKET",
   				"TAKE_PROFIT",
   				"TAKE_PROFIT_MARKET",
   				"TRAILING_STOP_MARKET" 
   			],
   			"timeInForce": [
   				"GTC", 
   				"IOC", 
   				"FOK", 
   				"GTX" 
 			],
 			"liquidationFee": "0.010000",	// liquidation fee rate
   			"marketTakeBound": "0.30",	// the max price difference rate( from mark price) a market order can make
 		}
   	],
	"timezone": "UTC" 
}
