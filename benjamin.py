# 克隆自聚宽文章：https://www.joinquant.com/post/7111
# 标题：又一个价值投资策略~大家随意看看~
# 作者：我爱长颈鹿咕咕

# 导入聚宽函数库
import jqdata
from jqdata import gta

# 初始化函数，设定基准等等
def initialize(context):
    set_params()        #1设置策参数
    set_variables() #2设置中间变量

def set_params():
    g.tc=30  # 调仓频率
    
def set_variables():
    g.t=0              #记录回测运行的天数
    g.if_trade=False   #当天是否交易
    
def before_trading_start(context):
    if g.t%g.tc==0:
        #每g.tc天，交易一次行
        g.if_trade=True 
        g.all_stocks = list(get_all_securities(['stock']).index)
    g.t+=1
    
    
def handle_data(context, data):
    if g.if_trade==True:
        df = get_fundamentals(query(
             valuation.code,balance.total_current_assets,
             balance.total_current_liability,valuation.pe_ratio,
             valuation.pb_ratio,balance.total_liability
             ).filter(
             valuation.code.in_(g.all_stocks)
             ).order_by(
             valuation.code
             ))
        
        avg_pe=sum(df['pe_ratio'])/len(df['pe_ratio'])
        avg_pb=sum(df['pb_ratio'])/len(df['pb_ratio'])
        
        scores_=[]
        for i in df.index:
            scores=0
            if (df['pe_ratio'][i] < avg_pe):
                scores = scores + 1
            
            if (df['pb_ratio'][i] < avg_pb):
                scores = scores + 1
            
            if (df['total_current_assets'][i] >= 1.2 * df['total_current_liability'][i]):
                scores = scores + 1
            
            if (df['total_liability'][i] <= 1.5 * (df['total_current_assets'][i]-df['total_current_liability'][i])):
                scores = scores + 1
            scores_.append(scores)
        df['scores_1'] = scores_        
        
        selected_stock_1 = []
        for i in df.index:
            if (df['scores_1'][i] == 4):
                selected_stock_1.append(df['code'][i])
        
        df2 = pd.DataFrame(index=range(0,len(selected_stock_1)))
        df2['code']=selected_stock_1
        # print df2
        prev_time=[]         
        for i in range(1,6):
            prev_time.append(str(context.current_dt.year-i))    
        
        for i in prev_time:
            h = get_fundamentals(query(
                valuation.code,income.net_profit
                ).filter(
                valuation.code.in_(selected_stock_1)
                ).order_by(valuation.code),statDate=i)
            # print h
            df2[i] = h['net_profit']
        # print df2
        
        selected_stock_2=[]
        for i in df2.index:
            scores_2 = 0
            if (df2.iloc[i,1] >= 0):
                scores_2 = scores_2 +1
            if (df2.iloc[i,2] >= 0):
                scores_2 = scores_2 +1
            if (df2.iloc[i,3] >= 0):
                scores_2 = scores_2 +1
            if (df2.iloc[i,4] >= 0):
                scores_2 = scores_2 +1
            if (df2.iloc[i,5] >= 0):
                scores_2 = scores_2 +1
            if (scores_2 >=4 ):
                selected_stock_2.append(df2['code'][i])
        # print selected_stock_2
        
        df3 = pd.DataFrame(index=range(0,len(selected_stock_2)))
        df3['code']=selected_stock_2
        prev_time_2=[]         
        for i in range(1,5):
            prev_time_2.append(str(context.current_dt.year-i))    
        
        for i in prev_time_2:
            h = get_fundamentals(query(
                valuation.code,income.total_profit
                ).filter(
                valuation.code.in_(selected_stock_2)
                ).order_by(valuation.code),statDate=i)
            # print h
            df3[i] = h['total_profit']
        # print df3
        
        selected_stock_3=[]
        for i in df3.index:   
            if (df3.iloc[i,1] >= df3.iloc[i,4]*0.8):
                selected_stock_3.append(df['code'][i])
                
        # print selected_stock_3
       
        num=len(selected_stock_3)
        cash=context.portfolio.cash
        for i in g.all_stocks:
            if i in selected_stock_3:
                order_value(i,cash/num)
            if i not in selected_stock_3:
                order_target(i,0)
    g.if_trade=False