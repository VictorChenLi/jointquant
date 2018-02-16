'''
始终持有沪深300银行指数成分股中市净率最低的股份制银行，每周检查一次，
如果发现有新的股份制银行市净率低于原有的股票，则予以换仓。
'''

## 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    # 设定沪深300银行指数作为基准
    set_benchmark('399951.XSHE')
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True) 
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, \
                             open_commission=0.0003, close_commission=0.0003,\
                             close_today_commission=0, min_commission=5), type='stock')
                             
    # 首次不考虑时间买入                         
    check_stocks(context)
    trade(context)
    
    run_daily(daily_check, time='open') #每日检查
    # 运行函数, 按周运行，在每周第一个交易日运行
    run_weekly(check_stocks, weekday=1, time='before_open') #选股
    run_weekly(trade, weekday=1, time='open') #交易
    
## 得到沪深300银行指数成分股,找到市净率最低的股票
def check_stocks(context):
    # 得到沪深300银行指数成分股
    g.stocks = get_index_stocks('399951.XSHE')

    # 查询股票的市净率，并按照市净率升序排序
    if len(g.stocks) > 0:
        g.df = get_fundamentals(
            query(
                valuation.code,
                valuation.pb_ratio
            ).filter(
                valuation.code.in_(g.stocks)
            ).order_by(
                valuation.pb_ratio.asc()
            )
        )

        # 找出最低市净率的一只股票
        g.code = g.df['code'][0]

## 交易
def trade(context):
    if len(g.stocks) > 0:
        code = g.code
        # 如持仓股票不是最低市净率的股票，则卖出
        for stock in context.portfolio.positions.keys():
            if stock != code:
                order_target(stock,0)
        
        # 持仓该股票
        if len(context.portfolio.positions) > 0:
            return
        else:
            order_value(code, context.portfolio.inout_cash*0.9)

def handle_data(context, data):
    grid_trade(context,data) #网格交易
    pass            

#====================止损方法=======================#
# 计算股票前n日收益率
def security_return(days,security_code):
    hist1 = attribute_history(security_code, days + 1, '1d', 'close',df=False)
    security_returns = (hist1['close'][-1]-hist1['close'][0])/hist1['close'][0]
    return security_returns

# 止损，根据前n日连续收益率
def conduct_nday_stoploss(context,security_code,days,bench):
    if  security_return(days,security_code)<= bench:
        for stock in context.portfolio.positions.keys():
            order_target_value(stock,0)
            log.info("Sell %s for stoploss", stock)
        return True
    else:
        return False
        
# 连续N日下跌
def is_fall_nday(days,stock):
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
def is_change_percent(stock_pos,percent):
    avg_cost = stock_pos.avg_cost
    price = stock_pos.price
    return (price/avg_cost) <= (1-percent)

# 计算股票累计收益率（从建仓至今）
def security_accumulate_return(context,data,stock):
    current_price = data[stock].price
    cost = context.portfolio.positions[stock].avg_cost
    if cost != 0:
        return (current_price-cost)/cost
    else:
        return None
        
# 个股止损，根据累计收益
def conduct_accumulate_stoploss(context,data,stock,bench):
    if security_accumulate_return(context,data,stock) != None\
    and security_accumulate_return(context,data,stock) < bench:
        order_target_value(stock,0)
        log.info("Sell %s for stoploss", stock)
        return True
    else:
        return False

# 比较现价与N日均价
def compare_current_nmoveavg(data,stock,days,multi):
    current_price = data[stock].price
    moveavg = data[stock].mavg(days)
    if current_price > multi * moveavg:
        return True
    else:
        return False
#====================止损方法=======================# 
    
def daily_check(context):
    for stock in context.portfolio.positions.keys():
        # 如果单只股票跌幅一共超过10%，则止损
        if is_change_percent(context.portfolio.positions[stock],0.10) is True:
            log.info("Stock {} is fall more than 10%, sell the stock".format(stock))
            order_target_value(stock, 0)

#====================交易方法=======================#
# 网格交易法
def grid_trade(context, data):
    update_initial_position(context, data)
    
    # 指数止损，前一天跌幅大于3%
    if conduct_nday_stoploss(context, '000300.XSHG', 2,-0.03):
        return
    for stock in context.portfolio.positions.keys():
        # 累计下跌20%
        if conduct_accumulate_stoploss(context,data,stock,-0.2):
            continue
        #1.连续5日下跌，不操作
        if is_fall_nday(5,stock):
            continue

        # 当波动率大于5时才使用网格
        if variance(stock) > 1:
            continue
       
        log.info("\n%s variance:%s",stock,variance(stock))

        #补仓步长：-3%，-5%，-8%，-12%
        setup_position(context,data,stock,context.buy_pace)
        #空仓步长：5%，10%，15%，20%
        setup_position(context,data,stock,context.sold_pace)

# 计算波动率
def variance(security_code):
    hist1 = attribute_history(security_code, 180, '1d', 'close',df=False)
    narray=np.array(hist1['close'])
    sum1=narray.sum()
    narray2=narray*narray
    sum2=narray2.sum()
    N = len(hist1['close'])
    mean=sum1/N
    var=sum2/N-mean**2
    return var

def setup_position(context,data,stock,bench):
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

    grid_result = adjust_position(stock, bottom_position["grid_position"], current_value, unit_value, returns, bench)

    if grid_result is not None:
        (target_value, to_position) = grid_result
        order_target_value(stock,target_value)
        g.grid_initial_position[stock]["grid_position"] = to_position

def adjust_position(stock, cur_position, current_value, unit_value, returns, bench):
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

def remove_sold_positions(stock):
    g.grid_initial_position.pop(stock, None)
    log.info("remove initial position:{}".format(stock))

# setup initial position and remove sold positions
def update_initial_position(context,data):
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
            remove_sold_positions(stock)
#====================交易方法=======================# 
    