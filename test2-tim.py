# SubscriptionWithEventHandlerExample.py

import blpapi
from optparse import OptionParser
import time
import threading
import datetime as dt
import pandas as pd
import sys

import _thread
import pytz
import winsound

EXCEPTIONS = blpapi.Name("exceptions")
FIELD_ID = blpapi.Name("fieldId")
REASON = blpapi.Name("reason")
CATEGORY = blpapi.Name("category")
DESCRIPTION = blpapi.Name("description")


class SubscriptionEventHandler(object):
    def getTimeStamp(self):
#        return time.strftime("%Y/%m/%d %X")
        return getCurrentDateTime()

    def processSubscriptionStatus(self, event):
        timeStamp = self.getTimeStamp()
#        print( "Processing SUBSCRIPTION_STATUS")
#        for msg in event:
#            topic = msg.correlationIds()[0].value()
#            print("%s: %s - %s" % (timeStamp, topic, msg.messageType()))
#
#            if msg.hasElement(REASON):
#                # This can occur on SubscriptionFailure.
#                reason = msg.getElement(REASON)
#                print ("        %s: %s" % (
#                    reason.getElement(CATEGORY).getValueAsString(),
#                    reason.getElement(DESCRIPTION).getValueAsString()))
#
#            if msg.hasElement(EXCEPTIONS):
#                # This can occur on SubscriptionStarted if at least
#                # one field is good while the rest are bad.
#                exceptions = msg.getElement(EXCEPTIONS)
#                for exInfo in exceptions.values():
#                    fieldId = exInfo.getElement(FIELD_ID)
#                    reason = exInfo.getElement(REASON)
#                    print ("        %s: %s" % (
#                        fieldId.getValueAsString(),
#                        reason.getElement(CATEGORY).getValueAsString()))

    def processSubscriptionDataEvent(self, event):
        timeStamp = self.getTimeStamp()
#        print(ticker_info_dict)
#        print ("Processing SUBSCRIPTION_DATA")
        for msg in event:
            ticker = msg.correlationIds()[0].value()
#            print( "%s: %s - %s" % (timeStamp, topic, msg.messageType()))
            
#            msg_list.append(msg)
            if msg.messageType() == "MarketDataEvents":
            
                for field in msg.asElement().elements():
                    if field.numValues() < 1:
                        continue
    
                    if field.name() in subscription_list:
                        ticker_info_dict[ticker][str(field.name())] = field.getValue()
                        ticker_info_dict[ticker]["TIME_RT"] =  timeStamp#dt.datetime.strptime(timeStamp, '%Y/%m/%d %H:%M:%S') 
                        
#                    print( "        %s = %s" % (field.name(),
#                                               field.getValueAsString()))
            
    def processMiscEvents(self, event):
        timeStamp = self.getTimeStamp()
#        for msg in event:
#            print( "%s: %s" % (timeStamp, msg.messageType()))
#
    def processEvent(self, event, session):
        try:
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                return self.processSubscriptionDataEvent(event)
            elif event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS:
                return self.processSubscriptionStatus(event)
            else:
                return self.processMiscEvents(event)
        except blpapi.Exception as e:
            print( "Library Exception !!! %s" % e.description())
        return False


def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    parser.add_option("-t",
                      dest="topics",
                      help="topic name (default: IBM US Equity)",
                      metavar="topic",
                      action="append",
                      default=[])
    parser.add_option("-f",
                      dest="fields",
                      help="field to subscribe to (default: LAST_PRICE)",
                      metavar="field",
                      action="append",
                      default=[])
    parser.add_option("-o",
                      dest="options",
                      help="subscription options (default: empty)",
                      metavar="option",
                      default=[])

    (options, args) = parser.parse_args()

    if not options.topics:
        options.topics = [etf_ticker, nav_ticker]

    if not options.fields:
        options.fields =  subscription_list

    return options



def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print( "Connecting to %s:%d" % (options.host, options.port))

    eventHandler = SubscriptionEventHandler()
    # Create a Session
    session = blpapi.Session(sessionOptions, eventHandler.processEvent)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    print("Connected successfully")

    service = "//blp/mktdata"
    if not session.openService(service):
        print("Failed to open %s service" % service)
        return

    subscriptions = blpapi.SubscriptionList()
    for t in options.topics:
        topic = service
        if not t.startswith("/"):
            topic += "/"
        topic += t
        subscriptions.add(topic, options.fields, options.options,
                          blpapi.CorrelationId(t))

    print("Subscribing...")
    session.subscribe(subscriptions)
        
    # timer
    timer_func()
    
    while 1:
        now = getCurrentDateTime()
        # force to exit the program including all sub-thread
        if now.time() > dt.time(16, 10):
             _thread.interrupt_main()
        continue
    
def execute():
    try:
        global position_flag, buy_count, sell_count, gain_count, loss_count, last_exe_px_b, last_exe_px_s
        
        etf_bid = ticker_info_dict[etf_ticker]["BID"]
        etf_ask = ticker_info_dict[etf_ticker]["ASK"]
        nav_last = ticker_info_dict[nav_ticker]["LAST_PRICE"]
        
        exe_px = [etf_bid, etf_ask][position_flag == 0]# cross the spread, buy hit ask, sell hit bid
        net_diff = exe_px - nav_last
        now = getCurrentDateTime()
        print(now) 
        print("BID: " + str(etf_bid) + " ASK: " + str(etf_ask) + " NAV: " + str(nav_last) + " NAV_DIFF: " + str(round(net_diff, 4)) + " EXE_PX: " + str(exe_px)+ " POS_F: " + str(position_flag))
        
        
        # exit
        if cut_loss_flag == 1 and loss_count - gain_count >= 2:
            print("loss - gain >= 2")
            return
        
        if position_flag == 0 and now.time() > dt.time(15, 30):
            print("market almost close")
            return
        
        # buy
        if buy_count >= 0 and position_flag == 0 and net_diff < discount_threshold:
            position_flag = 1
            side = 'b'
            last_exe_px_b = exe_px
            buy_count += 1
            trade_log_df.loc[len(trade_log_df)] = [now.date(), now, side, nav_last, exe_px, net_diff]
            print(str(now.time()) + " buy at " + str(exe_px))
            winsound.Beep(1000, 1500)
            return
        
        # sell
        if position_flag == 1 and exe_px - last_exe_px_b >= tick_size*tick_num and net_diff > discount_threshold:
            position_flag = 0
            side = 's'
            last_exe_px_s = exe_px
            sell_count += 1
            gain_count += 1
            trade_log_df.loc[len(trade_log_df)] = [now.date(), now, side, nav_last, exe_px, net_diff]
            print(str(now.time()) + " sell at " + str(exe_px))
            winsound.Beep(1000, 1500)
            return
            
        # sell stop loss
        if stop_loss_flag == 1 and position_flag == 1 and exe_px - last_exe_px_b <= -tick_size*tick_num_stop_loss:
            position_flag = 0
            side = 's'
            last_exe_px_s = exe_px
            sell_count += 1
            loss_count += 1
            trade_log_df.loc[len(trade_log_df)] = [now.date(), now, side, nav_last, exe_px, net_diff]
            print(str(now.time()) + " stop loss at " + str(exe_px))
            return
        
        # force to close the position each day
        if position_flag == 1 and now.time() > dt.time(15, 59):
            position_flag = 0
            side = 's'
            sell_count += 1
            trade_log_df.loc[len(trade_log_df)] = [now.date(), now, side, nav_last, exe_px, net_diff]
            print(str(now.time()) + " force to close at " + str(exe_px))
            return
        
    except Exception as ex:
        print(str(ex) + " in line " + str(sys.exc_info()[-1].tb_lineno))
        
def timer_func():

    execute()
    
    timer = threading.Timer(5, timer_func)
    timer.setDaemon(True)
    timer.start()

def insertDB_etf_trade_log(table):
    conf = \
    {
        "server":"safe-mysql",
        "db":"test",
        "schema":"dbo",
        "table":"etf_trade_log",
        "type":"%s, %s, %s, %s, %s, %s"
    }
    res = sql.insertTable(conf, table)
    return res 

def getCurrentDateTime():
    return dt.datetime.now( pytz.timezone("US/Eastern") ).replace(microsecond=0, tzinfo=None)
#%% Variable
etf_ticker = "EMB US Equity"
nav_ticker = "EMBIV Index"
msg_list = []
ticker_info_dict = {etf_ticker:{}, nav_ticker:{}}
subscription_list =  ["LAST_PRICE", "BID", "ASK", "EVT_TRADE_PRICE_RT"] #LAST_TRADE is more similar to LAST_PRICE, LAST_PRICE(from exchange) update less frequent than EVT_TRADE_PRICE_RT
trade_log_df = pd.DataFrame(columns = ['trade_date', 'timestamp', 'side', 'nav', 'exe_px', 'net_diff'])
                     
                     
tick_size = 0.01
commission_rate = 0.0035# ibkr fix:0.005 tired:0.0035
initial_net_asset = 50000# usd
net_asset = initial_net_asset
cash = net_asset
share = 0

discount_threshold = 0.1
stop_loss_threshold = -0.0001
tick_num = 3
tick_num_stop_loss = 10
position_flag = 0
stop_loss_flag = 1
cut_loss_flag = 1
gain_count = 0
loss_count = 0 
buy_count = 0
sell_count = 0
         
last_exe_px_b = 0
last_exe_px_s = 0
            
if __name__ == "__main__":
    print("SubscriptionWithEventHandlerExample")
    try:
        main()

    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")
        
    finally:
        print("save data to DB")
        date = str(getCurrentDateTime().date())
        trade_log_df.to_excel(r"log\\" + date + '.xlsx')

        


