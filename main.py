import warnings
warnings.filterwarnings('ignore')
from smartapi import SmartConnect
from time import sleep
import pandas as pd
#import pandas_ta as ta
from datetime import datetime, date, timedelta
from pytz import timezone
import requests
import login as l
#from keep_alive import keep_alive
import pyotp
import sys

Trade_symbol1 = 'NIFTY'
Trade_symbol2 = 'BANKNIFTY'
Trade_symbol3 = 'FINNIFTY'
Trade_symbol4 = 'MIDCPNIFTY'

Target = 100                        #In percentage
StopLoss = 20                       #In percentage
MinProfit = 5                       #In percentage
TrailingStopLoss = 5                #In percentage
FirstJump = MinProfit + TrailingStopLoss

def place_order(symbol, token, qty, exch_seg, buy_sell, ordertype, price, squareoff, stoploss, trailingStopLoss):

  try:
    orderparams = {
      "variety": "ROBO",
      "tradingsymbol": symbol,
      "symboltoken": token,
      "transactiontype": buy_sell,
      "exchange": exch_seg,
      "ordertype": ordertype,
      "producttype": "INTRADAY",
      "duration": "DAY",
      "price": truncate(price),
      "squareoff": truncate(squareoff),
      "stoploss": truncate(stoploss),
      "trailingStopLoss": truncate(trailingStopLoss),
      "quantity": qty
    }
    orderId = obj.placeOrder(orderparams)
    print("The order id is: {}".format(orderId))
    return orderId
  except Exception as e:
    print("Order placement failed: {}".format(e.message))
  else:
    pass

def modify_order(orderJson, triggerPrice):
  try:
    price = triggerPrice
    orderparams = {
      "variety": orderJson['variety'],
      "orderid": orderJson['orderid'],
      "ordertype": orderJson['ordertype'],
      "producttype": orderJson['producttype'],
      "triggerprice": truncate(triggerPrice),
      'price': truncate(price),
      "quantity": orderJson['quantity'],
      "duration": "DAY",
      "tradingsymbol": orderJson['tradingsymbol'],
      "symboltoken": orderJson['symboltoken'],
      "exchange": orderJson['exchange']
    }
    res = obj.modifyOrder(orderparams)
    print(
      f"Order modified for {orderJson['tradingsymbol']} {res.get('data')} {triggerPrice} ")

  except Exception as e:
    print(f'Order modification process Failed {orderJson} {triggerPrice}: {e}')

def truncate(f):
  if f is None: return None
  ticksize = 0.05 * 100
  remainder = int(str(f * 100.0).split('.')[0][-2:])
  pp = int((int(remainder / ticksize) * ticksize))

  if len(str(pp)) == 1:
    return float(str(int(f)) + '.0' + str(pp))
  else:
    return float(str(int(f)) + '.' + str(pp))

def intializeSymbolTokenMap():
  url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
  d = requests.get(url).json()
  global token_df
  token_df = pd.DataFrame.from_dict(d)
  token_df['expiry'] = pd.to_datetime(token_df['expiry'])
  token_df = token_df.astype({'strike': float})
  l.token_map = token_df

def getTokenInfo_IDX (name, instrumenttype='AMXIDX'):
  df=l.token_map
  idx_eq_df = df[(df['name'] == name) & (df['instrumenttype'] == instrumenttype)]
  return idx_eq_df

def getTokenInfo(symbol, exch_seg='NSE', instrumenttype='OPTIDX', strike_price='', pe_ce='CE'):
  df = l.token_map
  strike_price = strike_price * 100
  if exch_seg == 'NSE':
    eq_df = df[(df['exch_seg'] == 'NSE')]
    return eq_df[eq_df['name'] == symbol]
  elif exch_seg == 'NFO' and (instrumenttype == 'OPTSTK'
                              or instrumenttype == 'OPTIDX'):
    return df[(df['exch_seg'] == 'NFO')
              & (df['instrumenttype'] == instrumenttype) &
              (df['name'] == symbol) & (df['strike'] == strike_price) &
              (df['symbol'].str.endswith(pe_ce)) &
              (df['expiry'] >= str(date.today()))].sort_values(by=['expiry'])

def calculate_inidcator(res_json):
  columns = ['timestamp','O','H','L','C','V']  
  df = pd.DataFrame(res_json['data'], columns=columns)
  #df['timestamp'] = pd.to_datetime(df['timestamp'])
  df = df.round(decimals=2)
  #print(df)
  return df

def getHistoricalAPI(token, exch='NSE', interval='ONE_DAY'):
  to_date = datetime.now(timezone('Asia/Kolkata'))
  from_date = to_date - timedelta(days=30)
  from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
  to_date_format = to_date.strftime("%Y-%m-%d %H:%M")
  try:
    historicParam = {
      "exchange": exch,
      "symboltoken": token,
      "interval": interval,
      "fromdate": from_date_format,
      "todate": to_date_format
    }
    candel_json = obj.getCandleData(historicParam)
    #return candel_json
    calculate_inidcator(candel_json)
    return calculate_inidcator(candel_json)
  except Exception as e:
    print("Historic Api failed: {}".format(e.message))

def getOrderbook():
  for i in range(1, 4):
    try:
      orderbookRes = obj.orderBook()
      if 'data' in orderbookRes and orderbookRes['data']:
        orderDf = pd.DataFrame(orderbookRes['data'])
        return orderDf
    except Exception as e:
      print(f'Orderbook  Failed: {e}')
    sleep(i * 2)

def weekly_expiry(trade_symbol):
    global symboldf
    symboldf = pd.read_csv('https://api.kite.trade/instruments')
    symboldf['expiry'] = pd.to_datetime(symboldf['expiry']).apply(lambda x: x.date())
    weekly_expiry = symboldf[(symboldf.segment == 'NFO-OPT') & (symboldf.name == trade_symbol)]['expiry'].unique().tolist()
    weekly_expiry.sort()
    WEEKLY_EXPIRY = weekly_expiry[0]
    Lot_size = symboldf[(symboldf['expiry'] == WEEKLY_EXPIRY) & (symboldf['name'] == trade_symbol)].lot_size.iloc[-1]
    return [WEEKLY_EXPIRY,Lot_size]

def weekly_expiry_list():
  global WEEKLY_EXPIRY1,WEEKLY_EXPIRY2,WEEKLY_EXPIRY3,WEEKLY_EXPIRY4,Lot_size1,Lot_size2,Lot_size3,Lot_size4
  WEEKLY_EXPIRY1 = weekly_expiry(Trade_symbol1)[0]
  WEEKLY_EXPIRY2 = weekly_expiry(Trade_symbol2)[0]
  WEEKLY_EXPIRY3 = weekly_expiry(Trade_symbol3)[0]
  WEEKLY_EXPIRY4 = weekly_expiry(Trade_symbol4)[0]
  Lot_size1 = weekly_expiry(Trade_symbol1)[1]
  Lot_size2 = weekly_expiry(Trade_symbol2)[1]
  Lot_size3 = weekly_expiry(Trade_symbol3)[1]
  Lot_size4 = weekly_expiry(Trade_symbol4)[1]

def login():
  global obj
  obj = SmartConnect(api_key=l.api_key)
  data = obj.generateSession(l.user_name, l.password, pyotp.TOTP(l.totp).now())
  refreshToken = data['data']['refreshToken']
  userProfile = obj.getProfile(refreshToken)
  Login = userProfile['message']
  #print(Login)
  return Login

def symbolScan(s,l):

  #login()

  rms_limit = obj.rmsLimit()['data']['availablecash']
  Trading_Fund = float(rms_limit)
  available_Fund = Trading_Fund / l
  Tolerance = 0.2 * available_Fund
  available_cash = available_Fund - Tolerance

  def lotQty():
    if Trading_Fund > 0 and Trading_Fund < 10000:
      lot_qty = 1
    if Trading_Fund > 10000 and Trading_Fund < 20000:
      lot_qty = 2
    if Trading_Fund > 20000 and Trading_Fund < 30000:
      lot_qty = 3
    if Trading_Fund > 30000 and Trading_Fund < 40000:
      lot_qty = 4
    if Trading_Fund > 40000 and Trading_Fund < 50000:
      lot_qty = 5
    return lot_qty

  def getStrikePrice():
    head = {
      'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
      "Upgrade-Insecure-Requests": "1",
      "DNT": "1",
      "Accept":
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Accept-Encoding": "gzip, deflate, br"
    }

    URL1 = "https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY"

    NIFTY_URL = 'https://www.nseindia.com/api/quote-derivative?symbol=NIFTY'
    BNIFTY_URL = 'https://www.nseindia.com/api/quote-derivative?symbol=BANKNIFTY'
    FNIFTY_URL = 'https://www.nseindia.com/api/quote-derivative?symbol=FINNIFTY'
    MCPNIFTY_URL = 'https://www.nseindia.com/api/quote-derivative?symbol=MIDCPNIFTY'

    homeRes = requests.get(URL1, headers=head)
    d = d = requests.get(NIFTY_URL, headers=head, cookies=homeRes.cookies).json() if s == Trade_symbol1 else requests.get(BNIFTY_URL, headers=head, cookies=homeRes.cookies).json() if s == Trade_symbol2 else requests.get(FNIFTY_URL, headers=head, cookies=homeRes.cookies).json() if s == Trade_symbol3 else requests.get(MCPNIFTY_URL, headers=head, cookies=homeRes.cookies).json()
    df = pd.DataFrame(d['stocks'])
    timestamp = d['opt_timestamp']
    df1 = pd.json_normalize(df['metadata'])
    df1.insert(0, 'TimeStamp', timestamp)
    df1['TimeStamp'] = pd.to_datetime(df1['TimeStamp'])
    df1['expiryDate'] = pd.to_datetime(df1['expiryDate'])
    df1 = df1[['TimeStamp', 'expiryDate', 'prevClose', 'lastPrice', 'numberOfContractsTraded', 'optionType', 'strikePrice']]
    df2 = pd.json_normalize(df['marketDeptOrderBook'])
    df2 = df2[['tradeInfo.openInterest', 'tradeInfo.changeinOpenInterest', 'otherInfo.impliedVolatility', 'totalBuyQuantity', 'totalSellQuantity', 'tradeInfo.vmap']]
    df2.fillna(0, inplace=True)

    final = pd.concat([df1, df2], axis='columns')
    final = final[(final['tradeInfo.openInterest'] > 0)
                  & (final['numberOfContractsTraded'] > 0)]
    final.columns = [c.strip() for c in final.columns.values.tolist()]
    final = final.rename(columns={'tradeInfo.openInterest': 'OI', 'otherInfo.impliedVolatility': 'IV'})

    W_ex = WEEKLY_EXPIRY1 if s == Trade_symbol1 else WEEKLY_EXPIRY2 if s == Trade_symbol2 else WEEKLY_EXPIRY3  if s == Trade_symbol3 else WEEKLY_EXPIRY4

    ce_final = final[(final['lastPrice'] < available_cash) & (final['optionType'] == 'Call') & (final.expiryDate.dt.date == W_ex)].sort_values(by=['lastPrice']).tail(1)
    pe_final = final[(final['lastPrice'] < available_cash) & (final['optionType'] == 'Put') & (final.expiryDate.dt.date == W_ex)].sort_values(by=['lastPrice']).tail(1)

    ceStrikePrice = ce_final.sort_values(by=['OI']).strikePrice.iloc[-1]
    peStrikePrice = pe_final.sort_values(by=['OI']).strikePrice.iloc[-1]

    return [ceStrikePrice, peStrikePrice]

  ceStrike = getStrikePrice()[0]
  peStrike = getStrikePrice()[1]

  #print(f'{s} CE Strike = {ceStrike}')
  #print(f'{s} PE Strike = {peStrike}')

  idx_token = getTokenInfo_IDX(s).iloc[0]['token']
  idx_ltpInfo = obj.ltpData('NSE',s,idx_token)
  idx_Ltp = idx_ltpInfo['data']['ltp']

  ce_tokenInfo = getTokenInfo(s, 'NFO', 'OPTIDX', ceStrike, 'CE').iloc[0]
  ce_symbol = ce_tokenInfo['symbol']
  ce_token = ce_tokenInfo['token']
  ce_Lotsize = int(ce_tokenInfo['lotsize'])
  ce_lot = lotQty() * ce_Lotsize

  pe_tokenInfo = getTokenInfo(s, 'NFO', 'OPTIDX', peStrike, 'PE').iloc[0]
  pe_symbol = pe_tokenInfo['symbol']
  pe_token = pe_tokenInfo['token']
  pe_Lotsize = int(pe_tokenInfo['lotsize'])
  pe_lot = lotQty() * pe_Lotsize

  idx_hist_data = getHistoricalAPI(idx_token)
  idx_2DHH = max(idx_hist_data.iloc[-2].H, idx_hist_data.iloc[-3].H) * (1 + 0.0015)
  idx_2DLL = min(idx_hist_data.iloc[-2].L, idx_hist_data.iloc[-3].L) * (1 - 0.0015)

  #print(f'{s} IDX 2DHH = {idx_2DHH}')
  #print(f'{s} IDX 2DLL = {idx_2DLL}')

  OrderBook = obj.orderBook()['data']
  #print(OrderBook)

  if idx_Ltp > idx_2DHH and OrderBook is None:
    ce_ltpInfo = obj.ltpData("NFO", ce_symbol, ce_token)
    current_ce_indexLtp = ce_ltpInfo['data']['ltp']
    ce_Limit = current_ce_indexLtp
    ce_SL = (StopLoss/100) * ce_Limit
    ce_SL_value = ce_Limit - ce_SL
    ce_target = (Target/100) * ce_Limit
    ce_target_value = ce_Limit + ce_target
    ce_TSL = ce_SL / 4
    ce_qty = ce_lot
    ceOrderid = place_order(ce_symbol, ce_token, ce_qty, 'NFO', 'BUY', 'LIMIT', ce_Limit, ce_target, ce_SL, ce_TSL)
    print(f'Buy Order Placed for {ce_symbol}, QTY {ce_qty}, TARGET {ce_target_value}, STOPLOSS {ce_SL_value} at price {ce_Limit}')
    #sys.exit()
    ce_SLprices = []
    while currentTime < endTime:
      try:
        login()
        orderDf = getOrderbook()
        ce_Info = obj.ltpData("NFO", ce_symbol, ce_token)
        ce_LTP = ce_Info['data']['ltp']
        ce_SLorderInfo = orderDf[orderDf.parentorderid == str(ceOrderid)].sort_values(by='price').iloc[0]
        ce_SLorder_price = ce_SLorderInfo.price

        ce_Trigger1 = round(((1 + FirstJump / 100) * ce_Limit),1)
        ce_Jump1 = round(((1 + MinProfit / 100) * ce_Limit),1)
        ce_Trigger2 = round(((1 + TrailingStopLoss / 100) * ce_SLorder_price),1)
        ce_Jump2 = round(((1 - TrailingStopLoss / 100) * ce_LTP),1)

        if ce_LTP > ce_target_value:
          print('TARGET HIT!')
          sys.exit()

        if ce_LTP < ce_SLorder_price:
          print('STOPLOSS HIT!')
          sys.exit()

        if ce_LTP > ce_Trigger1 and ce_SLorder_price < ce_Limit:
          print(f'CE SL is at {MinProfit}%')
          modify_order(ce_SLorderInfo,ce_Jump1)
          ce_modified_price = ce_Jump1
          ce_SLprices.append(ce_modified_price)

        if ce_LTP > ce_Trigger2 and ce_SLorder_price > ce_Limit and ce_SLprices[-1] < ce_Jump2:
          print('CE SL Modified!')
          modify_order(ce_SLorderInfo,ce_Jump2)
          ce_modified_price = ce_Jump2
          ce_SLprices.append(ce_modified_price)

      except Exception as e:
        print(f'Error in trailing Stoploss: {e}')
      sleep(1)

  sleep(1)

  if idx_Ltp < idx_2DLL and OrderBook is None:
    pe_ltpInfo = obj.ltpData("NFO", pe_symbol, pe_token)
    current_pe_indexLtp = pe_ltpInfo['data']['ltp']
    pe_Limit = current_pe_indexLtp
    pe_SL = (StopLoss/100) * pe_Limit
    pe_SL_value = pe_Limit - pe_SL
    pe_target = (Target/100) * pe_Limit
    pe_target_value = pe_Limit + pe_target
    pe_TSL = pe_SL / 4
    pe_qty = pe_lot
    peOrderid = place_order(pe_symbol, pe_token, pe_qty, 'NFO', 'BUY', 'LIMIT', pe_Limit, pe_target, pe_SL, pe_TSL)
    print(f'Buy Order Placed for {pe_symbol}, QTY {pe_qty}, TARGET {pe_target_value}, STOPLOSS {pe_SL_value} at price {pe_Limit}')
    #sys.exit()
    pe_SLprices = []
    while currentTime < endTime:
      try:
        login()
        orderDf = getOrderbook()
        pe_Info = obj.ltpData("NFO", pe_symbol, pe_token)
        pe_LTP = pe_Info['data']['ltp']
        pe_SLorderInfo = orderDf[orderDf.parentorderid == str(peOrderid)].sort_values(by='price').iloc[0]
        pe_SLorder_price = pe_SLorderInfo.price

        pe_Trigger1 = round(((1 + FirstJump / 100) * pe_Limit),1)
        pe_Jump1 = round(((1 + MinProfit / 100) * pe_Limit),1)
        pe_Trigger2 = round(((1 + TrailingStopLoss / 100) * pe_SLorder_price),1)
        pe_Jump2 = round(((1 - TrailingStopLoss / 100) * pe_LTP),1)

        if pe_LTP > pe_target_value:
          print('TARGET HIT!')
          sys.exit()

        if pe_LTP < pe_SLorder_price:
          print('STOPLOSS HIT!')
          sys.exit()

        if pe_LTP > pe_Trigger1 and pe_SLorder_price < pe_Limit:
          print(f'PE SL is at {MinProfit}%')
          modify_order(pe_SLorderInfo,pe_Jump1)
          pe_modified_price = pe_Jump1
          pe_SLprices.append(pe_modified_price)

        if pe_LTP > pe_Trigger2 and pe_SLorder_price > pe_Limit and pe_SLprices[-1] < pe_Jump2:
          print('PE SL Modified!')
          modify_order(pe_SLorderInfo,pe_Jump2)
          pe_modified_price = pe_Jump2
          pe_SLprices.append(pe_modified_price)

      except Exception as e:
        print(f'Error in trailing Stoploss: {e}')
      sleep(1)

  sleep(1)

  return None

def daily_cycle():

  print('The code is running...')

  while True:
    global startTime,endTime,currentTime
    startTime = datetime.now(timezone('Asia/Kolkata')).replace(hour=9, minute=20, second=0)
    endTime = datetime.now(timezone('Asia/Kolkata')).replace(hour=15, minute=0, second=0)
    currentTime = datetime.now(timezone('Asia/Kolkata'))

    while currentTime > startTime and currentTime < endTime:
      try:

        login()

        symbolScan(Trade_symbol1,Lot_size1)

        symbolScan(Trade_symbol2,Lot_size2)

        #symbolScan(Trade_symbol3,Lot_size3)

        #symbolScan(Trade_symbol4,Lot_size4)

        currentTime2 = datetime.now(timezone('Asia/Kolkata'))
        if currentTime2 > endTime:
          print('Trading time is over...')
          daily_cycle()

      except Exception as e:
        print(f'Error in Order Placement: {e}')
        sleep(1)
        daily_cycle()

      sleep(1)

    sleep(1)

if __name__ == '__main__':

  #keep_alive()
  weekly_expiry_list()
  intializeSymbolTokenMap()
  daily_cycle()
