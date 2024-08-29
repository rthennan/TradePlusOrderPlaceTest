import pandas as pd 
from datetime import date, datetime as dt
import traceback
import sys
from Connect import XTSConnect
import time
import threading




"""TradePlus Credentials"""
"""API Credentials from API APP -  https://rocketplus.tradeplusonline.com/dashboard"""  
API_KEY = 'API_key' #tenpTraderAppOne1
API_SECRET = 'API_Secret' #tenpTraderAppOne1
#XTS_API_BASE_URL = "https://xts-api.trading"
source = 'WEBAPI'
userID = 'user_ID'
ClientID='client_ID'
#Using this as an example. Trading symbol is irrelavent, as long as it a tradeable one.
tradingSymbol = 'NIFTY24OCT25000CE'

#Will start with one concurrent orders and will go upto this number
maxOrdersPerSecond = 3

def customLogger(txt):     
    logFile = f'orderAPI_rateLimitCheckLogs_{str(date.today())}.log'
    print(dt.now(),txt)
    logMsg = '\n'+str(dt.now())+'    ' + str(txt)
    with open(logFile,'a') as f:
        f.write(logMsg) 
        
        
def mktBuy_RP(rpAPIObj,tradingSymbol,exchInstID, orderQty, orderSeqNum=0,tradeNum=0):    
    #orderSeqNum will be 0 for single orders.
    try:
        #orderUniqueIdentifier" length must be less than or equal to 20 characters long
        RP_uniqueOrderId = f"t{tradeNum}Bseq{orderSeqNum}"
        msg = f'mktBuy_RP => Placing Buy Market Order tradingSymbol: {tradingSymbol} orderQty: {orderQty} orderSeqNum: {orderSeqNum}. uniqueID: {RP_uniqueOrderId}'
        customLogger(msg)
        exchInstID = int(exchInstID)
        orderQty = int(orderQty)
        mktOrderResponse = rpAPIObj.place_order(
            exchangeSegment=rpAPIObj.EXCHANGE_NSEFO,
            exchangeInstrumentID=exchInstID,
            productType=rpAPIObj.PRODUCT_MIS,
            orderType=rpAPIObj.ORDER_TYPE_MARKET,
            orderSide=rpAPIObj.TRANSACTION_TYPE_BUY,
            timeInForce=rpAPIObj.VALIDITY_DAY,
            orderQuantity=orderQty,
            disclosedQuantity = 0,
            #Disclosed Quantity not Allowed for NSEFO
            limitPrice=0,
            stopPrice=0,
            orderUniqueIdentifier = RP_uniqueOrderId,
            #It is user specific Order Unique Identifier
            clientID=ClientID)
        #msg = f'mktBuy_RP => mktOrderResponse for orderSeqNum : {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId}: {mktOrderResponse}'
        #customLogger(msg)
        #First check if the response is a dict
        if isinstance(mktOrderResponse, dict):
            if mktOrderResponse.get('type') == 'success':
                orderId = mktOrderResponse['result']['AppOrderID']
                msg = f'mktBuy_RP : order {orderId} placed successfully for orderSeqNum : {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId}'
                #mainLog(msg)
                customLogger(msg)
                return True
            #API Rate limit hit
            elif (mktOrderResponse.get('data').get('type') == 'error') and (mktOrderResponse.get('data').get('code') == 'e-apirl-0004'):
                msg = f'''mktBuy_RP => Rate limit hit.: orderSeqNum: {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId}.
                    orderPlace Response : {mktOrderResponse}
                '''
                customLogger(msg)
                return False
                
            else:
                msg = f'mktBuy_RP => mktOrderResponse is not success for  orderSeqNum: {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId} . Response -> {mktOrderResponse}'
                customLogger(msg)
                return False
        else:
            msg = f'mktBuy_RP => mktOrderResponse is not a dict for orderSeqNum: {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId}. Response -> {mktOrderResponse}'
            customLogger(msg)
            return False         
    except Exception as e:
        msg = f'''Exception {e} in mktBuy_RP for tradingSymbol: {tradingSymbol} exchInstID: {exchInstID} orderQty: {orderQty} orderSeqNum: {orderSeqNum} tradeNum: {tradeNum} uniqueID: {RP_uniqueOrderId}. Traceback: {traceback.format_exc()}'''
        customLogger(msg)    
        return False
        

def login_RP(RP_API_KEY,RP_API_SECRET,RP_source):
    try:
        """Make XTSConnect object by passing your interactive API appKey, secretKey and source"""
        RP_API = XTSConnect(RP_API_KEY, RP_API_SECRET, RP_source)
        
        ''''Rocket Plus / TradePlus - Interactive Login'''
        loginResponse = RP_API.interactive_login()
        #Login Only once. If relogin required, recreate RP_API object
        if isinstance(loginResponse, dict) and loginResponse.get('type') == 'success':
            msg = 'RocketPlus API login Success'
            customLogger(msg)
            #RP_token = loginResponse['result']['token']
            return RP_API
        else:
            msg = f'login_RP => RocketPlus API login Fail : {loginResponse}'
            customLogger(msg)
            sys.exit()
                
    except Exception as e:
        msg = f'''Exception {e} in login_RP. Traceback: {traceback.format_exc()}'''
        customLogger(msg)
        sys.exit()
        
def generateSymbolexchangeID_Dictionary(rpAPIObj):
    rocketPlusMasterObj = rpAPIObj.get_master(['NSEFO'])
    resultOnly = rocketPlusMasterObj['result']
    # Split the data into rows
    rows = resultOnly.split("\n")    
    # Split each row into columns and create a list of lists
    data = [row.split("|") for row in rows]    
    # Create the DataFrame
    df = pd.DataFrame(data)
    #Column #1 in exhange instrumentID, #Column #3 is name, #Column #4 is symbol, #Column #5 is InstrumentType - OPTSTK/OTPIDX
    filteredMaster = df[[1,3, 4, 5]]
    # Rename the columns to reflect their original headers
    filteredMaster.columns = ["exchangeInstrumentID", "name","symbol", "InstrumentType"]
    #Retain OPTIDX and NIFTY only
    filteredMaster = filteredMaster[
        (filteredMaster['name'] == 'NIFTY')&
        (filteredMaster['InstrumentType'] == 'OPTIDX')
    ]
    # Create the dictionary with symbol as key and exchangeInstrumentID as value
    symbol_to_exchangeID = {row['symbol']: row['exchangeInstrumentID'] for _, row in filteredMaster.iterrows()}
    return symbol_to_exchangeID

def marketBuyerAsynchWrapper(rpAPIObj,tradingSymbol,exchInstID, orderPlaceQty,ordersPerSecond):
    msg = f'\n\n\nstarting Transaction # {ordersPerSecond}. Attempting to place {ordersPerSecond} concurrent orders '
    customLogger(msg)
    buyOrdersToPlace = []
    tradeNum = ordersPerSecond
    orderSeqNum = 1
    for orderCount in range(1,ordersPerSecond+1):
        buyOrder = threading.Thread(target=mktBuy_RP, args=(rpAPIObj,tradingSymbol,exchInstID, orderPlaceQty,orderSeqNum,tradeNum))
        buyOrdersToPlace.append(buyOrder)
        buyOrder.start()
        orderSeqNum += 1
    for buyOrder in buyOrdersToPlace:
        buyOrder.join() 
    msg = f'Transaction #{ordersPerSecond} Completed.\n\n\n'
    customLogger(msg)    

if __name__ == '__main__':
    
    msg = 'Starting Main Trader'
    customLogger(msg)
    #Login to TradePlus
    rpAPIObj = login_RP(API_KEY,API_SECRET,source)
    msg = 'Generating Symbol to ExchangeID dictionary'
    customLogger(msg)
    #Dictionary to get Exhcange Instrument ID for given Trading Symbol
    symbolExID_dict = generateSymbolexchangeID_Dictionary(rpAPIObj)
    
    exchInstID = symbolExID_dict.get(tradingSymbol)
    #placing each order with 25 Qty 
    orderPlaceQty = 25
    
    msg = 'Done generating dictionary'
    customLogger(msg)       

    msg = 'Starting Order Placement'
    customLogger(msg)  
    
    #Try placing multiple orders per second, in a gradually increasing manner
    #Start with one order Per second and go upto 12 orders per second    

    for ordersPerSecond in range(1,maxOrdersPerSecond+1):    
        marketBuyerAsynchWrapper(rpAPIObj,tradingSymbol,exchInstID, orderPlaceQty,ordersPerSecond)
        msg = 'sleeping for 5 seconds'
        customLogger(msg)
        time.sleep(5)
        
    msg = 'All actions completed'
    customLogger(msg)          


    
