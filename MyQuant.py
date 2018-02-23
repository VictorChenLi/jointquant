# 克隆自聚宽文章：https://www.joinquant.com/post/6585
# 标题：首席质量因子 - Gross Profitability
# 作者：小兵哥

#enable_profile()
#version 1.41
import numpy as np
import talib
import pandas
import scipy as sp
import scipy.optimize
import datetime as dt
from scipy import linalg as sla
from scipy import spatial
from jqdata import gta
from jqdata import *
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import statsmodels.api as sm

def setup_var(context):
    # 引用 lib
    g.GP        = Gross_Profitability_lib()
    g.quantlib  = quantlib()
    g.GT = Grid_Trade_lib()

    g.quantlib.fun_set_var(context, 'version', 1.0)
    
    # ======= 调仓参数 =======#
    # 持仓周期
    g.quantlib.fun_set_var(context, 'hold_cycle', 30)
    # 当前周期内第几日
    g.quantlib.fun_set_var(context, 'hold_periods', 0)
    g.quantlib.fun_set_var(context, 'stock_list', [])
    g.quantlib.fun_set_var(context, 'position_price', {})
    # 跌幅超过25%清仓
    g.quantlib.fun_set_var(context, 'stop_loss_delt', 0.25) 
    # 涨幅超过50%清仓
    g.quantlib.fun_set_var(context, 'stop_win_delt', 0.5)

    # 自定义股票池
    fund = get_index_stocks("399951.XSHE") + get_index_stocks("000300.XSHG")
    fund = g.quantlib.fun_delNewShare(context, fund, 60)
    g.quantlib.fun_set_var(context, "fund", fund)
    # 设置一定要买的和一定不买的股票列表
    prefer_stock_list, bad_stock_list = [], []
    g.quantlib.fun_set_var(context, "prefer_stock_list", prefer_stock_list)
    g.quantlib.fun_set_var(context, "bad_stock_list", bad_stock_list)

    #补仓步长：-3%，-5%，-8%，-12%
    g.quantlib.fun_set_var(context, "buy_pace", -0.12)
    #空仓步长：5%，10%，15%，20%
    g.quantlib.fun_set_var(context, "sold_pace", 0.2)

def initialize(context):
    #用沪深 300 做回报基准
    set_benchmark('000300.XSHG')
    # 滑点、真实价格
    set_slippage(FixedSlippage(0.000))
    set_option('use_real_price', True)

    # 关闭部分log
    # log.set_level('order', 'error')
    
    setup_var(context);

def after_code_changed(context):
    setup_var(context);

def before_trading_start(context):
    # 记录盘前收益
    context.returns = {}
    context.returns['algo_before_returns'] = context.portfolio.returns

def handle_data(context, data):
    g.GP.gross_profit_trade(context, data)
    # g.GT.grid_trade(context,data)
    
#～～～～～～～～～～～～～选股方法～～～～～～～～～～～～～～～～#
#====================质量因子交易法=======================#
class Gross_Profitability_lib():

    def gross_profit_trade(self, context, data):
        # 检查是否需要调仓
        rebalance_flag, context.position_price, context.hold_periods = \
            g.quantlib.fun_needRebalance(context, 'GP algo ', context.stock_list, context.position_price, \
                context.hold_periods, context.hold_cycle, context.stop_loss_delt, context.stop_win_delt)

        statsDate = context.current_dt.date()
        trade_style = False    # True 会交易进行类似 100股的买卖，False 则只有在仓位变动 >25% 的时候，才产生交易
        if rebalance_flag:
            # 选取股票
            prefer_stock_list = context.prefer_stock_list
            bad_stock_list = context.bad_stock_list
            GP_stock_list = self.fun_get_stock_list(context, 5, statsDate)
            stock_list = list(set(prefer_stock_list + GP_stock_list) - set(bad_stock_list))
            # 分配仓位
            position_ratio = g.quantlib.fun_assetAllocationSystem(stock_list, statsDate)
            log.info("Current positions:",context.position_price);
            log.info("Adjust to positions:",position_ratio);

            trade_style = True
            context.stock_list = position_ratio.keys()

            # 更新待购价格
            context.position_price = g.quantlib.fun_update_positions_price(position_ratio)
            # 卖掉已有且不在待购清单里的股票
            for stock in context.portfolio.positions.keys():
                if stock not in position_ratio:
                    position_ratio[stock] = 0
            context.position_ratio = position_ratio

            # 每次调仓清空bad_stock_list
            g.quantlib.fun_set_var(context, "bad_stock_list", [])

        # 调仓，执行交易
        g.quantlib.fun_do_trade(context, context.position_ratio, trade_style)

    def fun_get_stock_list(self, context, hold_number, statsDate=None):
        df = get_fundamentals(
            query(
                valuation.code, 
                valuation.market_cap, 
                valuation.pe_ratio, 
                valuation.ps_ratio, 
                valuation.pb_ratio
            ).filter(
                valuation.code.in_(context.fund)
            )
        )

        # 1) 总市值全市场从大到小前80%
        fCap = df.sort(['market_cap'], ascending=[False])
        fCap = fCap.reset_index(drop = True)
        fCap = fCap[0:int(len(fCap)*0.8)]
        sListCap = list(fCap['code'])

        # 2）市盈率全市场从小到大前40%（剔除市盈率为负的股票）
        fPE = df.sort(['pe_ratio'], ascending=[True])
        fPE = fPE.reset_index(drop = True)
        fPE = fPE[fPE.pe_ratio > 0]
        fPE = fPE.reset_index(drop = True)
        fPE = fPE[0:int(len(fPE)*0.4)]
        sListPE = list(fPE['code'])

        # 3）市净率全市场从小到大前40%（剔除pb为负的股票）
        fPB = df.sort(['pb_ratio'], ascending=[True])
        fPB = fPB.reset_index(drop = True)
        fPB = fPB[fPB.pb_ratio > 0]
        fPB = fPB.reset_index(drop = True)
        fPB = fPB[0:int(len(fPB)*0.4)]
        sListPB = list(fPB['code'])

        # 4）市收率小于2.5
        fPS = df[df.ps_ratio < 2.5]
        sListPS = list(fPS['code'])

        # 5）同时满足上述3条的股票，按照股息率从大到小排序，选出股息率最高的 n 只股票
        good_stocks = list(set(sListCap) & set(sListPE) & set(sListPS) & set(sListPB))
        #print len(good_stocks)
        GP_ratio = {}

        df = get_fundamentals(
            query(income.code, income.total_operating_revenue, income.total_operating_cost, balance.total_assets),
            date = statsDate - dt.timedelta(1)
        )

        df = df.fillna(value = 0)
        df = df[df.total_operating_revenue > 0]
        df = df.reset_index(drop = True)
        df = df[df.total_assets > 0]
        df = df.reset_index(drop = True)
        df = df[df.code.isin(good_stocks)]
        df = df.reset_index(drop = True)
        df['GP'] = 1.0*(df['total_operating_revenue'] - df['total_operating_cost']) / df['total_assets']

        df = df.drop(['total_assets', 'total_operating_revenue', 'total_operating_cost'], axis=1)
        df = df.sort(columns='GP', ascending=False)
        #print df.head(10)
        stock_list = list(df['code'])

        positions_list = context.portfolio.positions.keys()
        stock_list = g.quantlib.unpaused(stock_list, positions_list)
        stock_list = g.quantlib.remove_st(stock_list, statsDate)

        stock_list = stock_list[:hold_number*10]
        stock_list = g.quantlib.remove_limit_up(stock_list, positions_list)

        print stock_list

        return stock_list[:hold_number]

#～～～～～～～～～～～～～选股方法～～～～～～～～～～～～～～～～#


#～～～～～～～～～～～～～ 择时方法 ～～～～～～～～～～～～～#

#====================== 网格交易法 ======================#
class Grid_Trade_lib():

    def grid_trade(self, context, data):
        self.update_initial_position(context, data)
        
        # 指数止损，前一天跌幅大于3%
        if g.quantlib.conduct_nday_stoploss(context, '000300.XSHG', 2,-0.03):
            return
        for stock in context.portfolio.positions.keys():
            # 累计下跌20%
            if g.quantlib.conduct_accumulate_stoploss(context,data,stock,-0.2):
                continue
            #1.连续5日下跌，不操作
            if g.quantlib.is_fall_nday(5,stock):
                continue

            # 当波动率大于5时才使用网格
            # if variance(stock) > 1:
            #     continue
           
            log.info("%s variance:%s",stock,self.variance(stock))

            
            self.setup_position(context,data,stock,g.quantlib.fun_get_var(context, "buy_pace", -0.05))
            self.setup_position(context,data,stock,g.quantlib.fun_get_var(context, "sold_pace", 0.1))

    # 计算波动率
    def variance(self, security_code):
        hist1 = attribute_history(security_code, 180, '1d', 'close',df=False)
        narray=np.array(hist1['close'])
        sum1=narray.sum()
        narray2=narray*narray
        sum2=narray2.sum()
        N = len(hist1['close'])
        mean=sum1/N
        var=sum2/N-mean**2
        return var

    def setup_position(self, context,data,stock,bench):
        bottom_position = g.grid_initial_position[stock]
        
        if bottom_position is None:
            return
        
        available_cash = context.portfolio.available_cash
        current_price = data[stock].price
        amount = context.portfolio.positions[stock].total_amount
        current_value = current_price*amount
        bottom_value = bottom_position["initial_price"] * bottom_position["initial_amount"]
        unit_value = bottom_value/10
        bottom_price = bottom_position["initial_price"]
        returns = (current_price-bottom_price)/bottom_price

        grid_result = self.adjust_position(stock, bottom_position["grid_position"], current_value, unit_value, returns, bench)

        if grid_result is not None:
            (target_value, to_position) = grid_result
            order_target_value(stock,target_value)
            g.grid_initial_position[stock]["grid_position"] = to_position

    def adjust_position(self, stock, cur_position, current_value, unit_value, returns, bench):
        if returns*bench <=0:
            return;
        is_buy = returns < 0
        to_position = math.floor(returns/bench)
        to_position = 4 if to_position >4 else to_position
        to_position = int(to_position * (bench/abs(bench)) )
        
        buy_grid=[10,11,13,16,20]
        sold_grid=[10,6,3,1,0]
        abs_to_position = abs(to_position);
        
        grid_value = (buy_grid[abs_to_position] if is_buy else sold_grid[abs_to_position])
        target_value =  grid_value * unit_value;
        target_return = abs_to_position*bench
        is_trade = (returns < target_return) if is_buy else (returns > target_return)
        is_trade = (current_value < target_value) and is_trade
        is_trade = (cur_position!=to_position) and cur_position!=to_position
        
        if is_trade:
            log.info("Start grid trade for %s", stock)
            log.info("Stock %s:Current grid position:%s, Current return:%s,Bench:%s,Current value:%s,Unit value:%s", stock, cur_position, returns, bench, current_value, unit_value)
            log.info("Start trade, adjust hold value to %s=%s*%s", target_value, grid_value, unit_value)
            return (target_value, to_position)
        else:
            return None

    def remove_sold_positions(self, stock):
        g.grid_initial_position.pop(stock, None)
        log.info("remove initial position:{}".format(stock))

    # setup initial position and remove sold positions
    def update_initial_position(self, context,data):
        # initial setup
        if hasattr(g, "grid_initial_position") is not True:
            log.info("setup grid trade initial position")
            g.grid_initial_position = {}
            
        # add new positions
        for stock in context.portfolio.positions.keys():
            if stock not in g.grid_initial_position.keys():
                initial_price = context.portfolio.positions[stock].price;
                initial_amount = context.portfolio.positions[stock].total_amount;
                log.info("add initial position:{} as price:{}, amount{}".format(stock, initial_price, initial_amount))
                g.grid_initial_position[stock] = {"initial_price": initial_price, "initial_amount": initial_amount, "grid_position": 0}
        
        # remove not exist positions
        for stock in g.grid_initial_position.keys():
            if stock not in context.portfolio.positions.keys():
                self.remove_sold_positions(stock)

#～～～～～～～～～～～～～ 择时方法 ～～～～～～～～～～～～～# 


#~=~=~=~=~=~=~=~=~=~=~=~= 基本函数库 ~=~=~=~=~=~=~=~=~=~=~=~=#
class quantlib():

    def fun_set_var(self, context, var_name, var_value):
        if var_name not in dir(context):
            setattr(context, var_name, var_value)

    def fun_get_var(self, context, var_name, default_value):
        if var_name not in dir(context):
            return default_value
        else:
            return context[var_name]

    def fun_check_price(self, context, algo_name, stock_list, position_price, stop_loss_delt, stop_win_delt):
        flag = False
        if stock_list:
            h = history(1, '1d', 'close', stock_list, df=False)
            for stock in stock_list:
                curPrice = h[stock][0]
                if stock not in position_price:
                    position_price[stock] = curPrice
                oldPrice = position_price[stock]
                if oldPrice != 0:
                    deltaprice = curPrice - oldPrice

                    if deltaprice > 0 and abs(deltaprice / oldPrice) > stop_win_delt:
                        # 上涨超过指定百分比
                        flag = True
                    if deltaprice < 0 and abs(deltaprice / oldPrice) > stop_loss_delt:
                        # 下跌超过指定百分比
                        # 加入到禁止股票列表，调仓时会卖出
                        bad_stock_list = context.bad_stock_list
                        bad_stock_list.append(stock)
                        self.fun_set_var(context, "bad_stock_list", bad_stock_list)
                        flag = True
                    
                    if flag:
                        log.info("%s need adjust position: %s current price:%s / original price: %s", algo_name, stock, str(curPrice), str(oldPrice))
        
        return flag, position_price

    # 调仓入口
    def fun_needRebalance(self, context, algo_name, stock_list, position_price, hold_periods, hold_cycle, stop_loss_delt, stop_win_delt):
        # log.info("%s adjust position window remain %s days", algo_name, str(hold_periods))
        rebalance_flag = False
        
        stocks_count = len(stock_list)
        
        if stocks_count == 0:
            log.info("%s need adjust position, cuz hold num = 0", algo_name)
            rebalance_flag = True
        elif hold_periods == 0:
            log.info("%s need adjust position, cuz hold remain day = 0", algo_name)
            rebalance_flag = True
        if not rebalance_flag:
            rebalance_flag, position_price = self.fun_check_price(context, algo_name, stock_list, position_price, stop_loss_delt, stop_win_delt)
        
        if rebalance_flag:
            hold_periods = hold_cycle
        else:
            hold_periods -= 1

        return rebalance_flag, position_price, hold_periods

    # 更新持有股票的价格，每次调仓后跑一次
    def fun_update_positions_price(self, ratio):
        position_price = {}
        if ratio:
            h = history(1, '1m', 'close', ratio.keys(), df=False)
            for stock in ratio.keys():
                if ratio[stock] > 0:
                    position_price[stock] = round(h[stock][0], 3)
        return position_price

    # TODO 补全各种配仓方案
    def fun_assetAllocationSystem(self, stock_list, statsDate=None):
        def __fun_getEquity_ratio(__stocklist, type, limit_up=1.0, limit_low=0.0, statsDate=None):
            __ratio = {}
            if __stocklist:
                if type == 1: #风险平价 历史模拟法
                    __ratio = self.fun_calStockWeight_by_risk(1.96, __stocklist, limit_up, limit_low, statsDate)
                elif type == 2: #最小方差配仓
                    __ratio = self.fun_calStockWeight(__stocklist, limit_up, limit_low)
                elif type == 3: #马科维奇算法配仓
                    __ratio = self.fun_cal_Weight_by_Markowitz(__stocklist)
                elif type == 5: # 风险平价 方差-协方差法
                    __ratio = self.fun_calWeight_by_RiskParity(__stocklist, statsDate)
                else: #等权重
                    for stock in __stocklist:
                        __ratio[stock] = 1.0/len(__stocklist)

            return __ratio

        equity_ratio = __fun_getEquity_ratio(stock_list, 0, 1.0, 0.0, statsDate)

        return equity_ratio

    def fun_do_trade(self, context, trade_ratio, trade_style):

        def __fun_tradeStock(context, curPrice, stock, ratio, trade_style):
            total_value = context.portfolio.portfolio_value
            
            curValue = context.portfolio.positions[stock].total_amount * curPrice
            Quota = total_value * ratio
            if Quota:
                if abs(Quota - curValue) / Quota >= 0.25 or trade_style:
                    if Quota > curValue:
                        #if curPrice > context.portfolio.positions[stock].avg_cost:
                        self.fun_trade(context, stock, Quota)
                    else:
                        self.fun_trade(context, stock, Quota)
            else:
                if curValue > 0:
                    self.fun_trade(context, stock, Quota)
    
        trade_list = trade_ratio.keys()
        myholdstock = context.portfolio.positions.keys()
        stock_list = list(set(trade_list).union(set(myholdstock)))
        total_value = context.portfolio.portfolio_value
    
        # 已有仓位
        holdDict = {}
        h = history(1, '1d', 'close', stock_list, df=False)
        for stock in myholdstock:
            tmpW = round((context.portfolio.positions[stock].total_amount * h[stock])/total_value, 2)
            holdDict[stock] = float(tmpW)
    
        # 对已有仓位做排序
        tmpDict = {}
        for stock in holdDict:
            if stock in trade_ratio:
                tmpDict[stock] = round((trade_ratio[stock] - holdDict[stock]), 2)
        tradeOrder = sorted(tmpDict.items(), key=lambda d:d[1], reverse=False)

        # 交易已有仓位的股票，从减仓的开始，腾空现金
        _tmplist = []
        for idx in tradeOrder:
            stock = idx[0]
            __fun_tradeStock(context, h[stock][-1], stock, trade_ratio[stock], trade_style)
            _tmplist.append(stock)

        # 交易新股票
        for i in range(len(trade_list)):
            stock = trade_list[i]
            if len(_tmplist) != 0 :
                if stock not in _tmplist:
                    __fun_tradeStock(context, h[stock][-1], stock, trade_ratio[stock], trade_style)
            else:
                __fun_tradeStock(context, h[stock][-1], stock, trade_ratio[stock], trade_style)

    def unpaused(self, stock_list, positions_list):
        current_data = get_current_data()
        tmpList = []
        for stock in stock_list:
            if not current_data[stock].paused or stock in positions_list:
                tmpList.append(stock)
        return tmpList

    def remove_st(self, stock_list, statsDate):
        current_data = get_current_data()
        return [s for s in stock_list if not current_data[s].is_st]

    # 剔除涨停板的股票（如果没有持有的话）
    def remove_limit_up(self, stock_list, positions_list):
        h = history(1, '1m', 'close', stock_list, df=False, skip_paused=False, fq='pre')
        h2 = history(1, '1m', 'high_limit', stock_list, df=False, skip_paused=False, fq='pre')
        tmpList = []
        for stock in stock_list:
            if h[stock][0] < h2[stock][0] or stock in positions_list:
                tmpList.append(stock)

        return tmpList

    # 剔除上市时间较短的产品
    def fun_delNewShare(self, context, equity, deltaday):
        deltaDate = context.current_dt.date() - dt.timedelta(deltaday)
    
        tmpList = []
        for stock in equity:
            if get_security_info(stock).start_date < deltaDate:
                tmpList.append(stock)
    
        return tmpList

    def fun_trade(self, context, stock, value):
        self.fun_setCommission(context, stock)
        order_target_value(stock, value)

    def fun_setCommission(self, context, stock):
        set_order_cost(OrderCost(open_tax=0, close_tax=0.001, open_commission=0.0003, close_commission=0.0003, close_today_commission=0, min_commission=5), type='stock')
        

    #====================止损方法=======================#
    # 计算股票前n日收益率
    def security_return(self, days,security_code):
        hist1 = attribute_history(security_code, days + 1, '1d', 'close',df=False)
        security_returns = (hist1['close'][-1]-hist1['close'][0])/hist1['close'][0]
        return security_returns

    # 止损，根据前n日连续收益率
    def conduct_nday_stoploss(self, context,security_code,days,bench):
        if  self.security_return(days,security_code)<= bench:
            for stock in context.portfolio.positions.keys():
                order_target_value(stock,0)
                log.info("Sell %s for stoploss", stock)
            return True
        else:
            return False
            
    # 连续N日下跌
    def is_fall_nday(self, days,stock):
        his = history(days+1,'1d','close',[stock],df =False)
        cnt = 0
        for i in range(days):
            daily_returns = (his[stock][i+1] - his[stock][i])/his[stock][i]
            if daily_returns <0:
                cnt += 1
        if cnt == days:
            log.info("%s fall %d days", stock, days)
            return True
        else:
            return False

    # 止损百分比
    def is_change_percent(self, stock_pos,percent):
        avg_cost = stock_pos.avg_cost
        price = stock_pos.price
        return (price/avg_cost) <= (1-percent)

    # 计算股票累计收益率（从建仓至今）
    def security_accumulate_return(self, context,data,stock):
        current_price = data[stock].price
        cost = context.portfolio.positions[stock].avg_cost
        if cost != 0:
            return (current_price-cost)/cost
        else:
            return None
            
    # 个股止损，根据累计收益
    def conduct_accumulate_stoploss(self, context,data,stock,bench):
        if self.security_accumulate_return(context,data,stock) != None\
        and self.security_accumulate_return(context,data,stock) < bench:
            order_target_value(stock,0)
            log.info("Sell %s for stoploss", stock)
            return True
        else:
            return False

    # 比较现价与N日均价
    def compare_current_nmoveavg(self, data,stock,days,multi):
        current_price = data[stock].price
        moveavg = data[stock].mavg(days)
        if current_price > multi * moveavg:
            return True
        else:
            return False
    #====================止损方法=======================# 
