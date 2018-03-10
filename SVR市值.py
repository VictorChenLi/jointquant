# 克隆自聚宽文章：https://www.joinquant.com/post/10778
# 标题：【量化课堂】机器学习多因子策略
# 作者：JoinQuant量化课堂

# 克隆自聚宽文章：https://www.joinquant.com/post/10778
# 标题：【量化课堂】机器学习多因子策略
# 作者：JoinQuant量化课堂

import pandas as pd
import numpy as np
import math
from sklearn.svm import SVR  
from sklearn.model_selection import GridSearchCV  
from sklearn.model_selection import learning_curve
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import jqdata

def initialize(context):
    set_params()
    set_backtest()
    run_daily(trade, '14:50')
    
def set_params():
    g.days = 0
    g.refresh_rate = 10
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    g.stocknum = 10
    g.index2 = '000016.XSHG'  # 上证50指数
    g.index8 = '399333.XSHE'  # 中小板R指数
    g.index_growth_rate = 0.01    
    
def set_backtest():
    set_benchmark('000985.XSHG')
    set_option('use_real_price', True)
    log.set_level('order', 'error')

# 获取股票n日以来涨幅，根据当前价计算
# n 默认20日
def get_growth_rate(security, n=20):
    lc = get_close_price(security, n)
    #c = data[security].close
    c = get_close_price(security, 1, '1m')
    
    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %(security, n, lc, c))
        return 0

# 获取前n个单位时间当时的收盘价
def get_close_price(security, n, unit='1d'):
    return attribute_history(security, n, unit, ('close'), True)['close'][0]
        
# 清空卖出所有持仓
def clear_position(context):
    if context.portfolio.positions:
        log.info("==> 清仓，卖出所有股票")
        sell_list = list(context.portfolio.positions.keys())
        for stock in sell_list:
            order_target_value(stock, 0)
        
def trade(context):

    gr_index2 = get_growth_rate(g.index2)
    gr_index8 = get_growth_rate(g.index8)
    log.info("当前%s指数的20日涨幅 [%.2f%%]" %(get_security_info(g.index2).display_name, gr_index2*100))
    log.info("当前%s指数的20日涨幅 [%.2f%%]" %(get_security_info(g.index8).display_name, gr_index8*100))

    #gr_index8 = get_idx_growth_rate(context.previous_date)
    #log.info("当前自定义指数指数的20日涨幅 [%.2f%%]" %(gr_index8*100))
    
    if gr_index2 <= g.index_growth_rate and gr_index8 <= g.index_growth_rate:
    #if gr_index8 <= g.index_growth_rate:
        clear_position(context)
        g.days = 0
    else: #if  gr_index2 > g.index_growth_rate or ret_index8 > g.index_growth_rate:
        if g.days % g.refresh_rate == 0:
            sample = get_index_stocks('000985.XSHG', date = None)
            q = query(valuation.code, valuation.market_cap, balance.total_assets - balance.total_liability,
                      balance.total_assets / balance.total_liability, income.net_profit, income.net_profit + 1, 
                      indicator.inc_revenue_year_on_year, balance.development_expenditure).filter(valuation.code.in_(sample))
            df = get_fundamentals(q, date = None)
            df.columns = ['code', 'log_mcap', 'log_NC', 'LEV', 'NI_p', 'NI_n', 'g', 'log_RD']
            
            df['log_mcap'] = np.log(df['log_mcap'])
            df['log_NC'] = np.log(df['log_NC'])
            df['NI_p'] = np.log(np.abs(df['NI_p']))
            df['NI_n'] = np.log(np.abs(df['NI_n'][df['NI_n']<0]))
            df['log_RD'] = np.log(df['log_RD'])
            df.index = df.code.values
            del df['code']
            df = df.fillna(0)
            df[df>10000] = 10000
            df[df<-10000] = -10000
            industry_set = ['801010', '801020', '801030', '801040', '801050', '801080', '801110', '801120', '801130', 
                      '801140', '801150', '801160', '801170', '801180', '801200', '801210', '801230', '801710',
                      '801720', '801730', '801740', '801750', '801760', '801770', '801780', '801790', '801880','801890']
            
            for i in range(len(industry_set)):
                industry = get_industry_stocks(industry_set[i], date = None)
                s = pd.Series([0]*len(df), index=df.index)
                s[set(industry) & set(df.index)]=1
                df[industry_set[i]] = s
                
            X = df[['log_NC', 'LEV', 'NI_p', 'NI_n', 'g', 'log_RD','801010', '801020', '801030', '801040', '801050', 
                    '801080', '801110', '801120', '801130', '801140', '801150', '801160', '801170', '801180', '801200', 
                    '801210', '801230', '801710', '801720', '801730', '801740', '801750', '801760', '801770', '801780', 
                    '801790', '801880', '801890']]
            Y = df[['log_mcap']]
            X = X.fillna(0)
            Y = Y.fillna(0)
            
            svr = SVR(kernel='rbf', gamma=0.1) 
            model = svr.fit(X, Y)
            factor = Y - pd.DataFrame(svr.predict(X), index = Y.index, columns = ['log_mcap'])
            factor = factor.sort_index(by = 'log_mcap')
            stockset = list(factor.index[:10])
            sell_list = list(context.portfolio.positions.keys())
            for stock in sell_list:
                if stock not in stockset[:g.stocknum]:
                    stock_sell = stock
                    order_target_value(stock_sell, 0)
                
            if len(context.portfolio.positions) < g.stocknum:
                num = g.stocknum - len(context.portfolio.positions)
                cash = context.portfolio.cash/num
            else:
                cash = 0
                num = 0
            for stock in stockset[:g.stocknum]:
                if stock in sell_list:
                    pass
                else:
                    stock_buy = stock
                    order_target_value(stock_buy, cash)
                    num = num - 1
                    if num == 0:
                        break
            g.days += 1
        else:
            g.days = g.days + 1    
            