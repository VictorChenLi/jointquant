#布林策略
def bolling(cash,paused,current_price,num,days):
    #取得股票的收盘价信息
    price=attribute_history(security,num+days,'1d',('close','open'),skip_paused=True)
    #创建一个num*days的二维数组来保存收盘价数据
    price_array=np.arange(num*days).reshape(num,days)
    for i in range(0,num):
        for j in range(0,days):
            price_array[i][j]=price['close'][i+j]
    #创建一个数组来保存中轨信息
    mid=np.arange(num)
    #创建一个数组来保存标准差
    std=np.arange(num)
    for i in range(0,num):
        mid[i]=np.mean(price_array[i])
        std[i]=np.std(price_array[i])
    #用up来保存昨日的上轨线
    up=mid[num-1]+2*std
    #用down来保存昨日的下轨线
    down=mid[num-1]-2*std
    #用一个列表来保存每天是开口还是收口
    #如果一天的标准差不比前一天小，则在open列表里记录
    #True,反之记录False,在close列表里记录False,反之
    #记录False
    open=[]
    close=[]
    for i in range(0,num-1):
        if std[i]>std[i+1]:
            close.append('True')
            open.append('False')
        else:
            open.append('True')
            close.append('False')
    #如果连续num天开口
    if 'False' not in open:
        #如果当前价格超过昨日的上轨
        if current_price>mid[num-1]+2*std[num-1]:
            #计算可以买多少股票
            num_of_shares=int(cash/current_price)
            #如果可以买的数量超过0并且股票未停牌
            if num_of_shares>0 and paused==False:
                #购买股票
                order(security,+num_of_shares)
        #如果当前价格跌破了昨日的下轨
        elif current_price<mid[num-1]-2*std[num-1]:
            #如果股票未停牌
            if paused==False:
                #将股票卖空
                order_target(security,0)
    #如果连续num天收口，则股价超过上轨时卖，跌破
    #下轨时买
    if 'False' not in close:
        if current_price>mid[num-1]+2*std[num-1]:
            if paused==False:
                order_target(security,0)
        elif current_price<mid[num-1]-2*std[num-1]:
            num_of_shares=int(cash/current_price)
            if num_of_shares>0 and paused==False:
                order(security,+num_of_shares)