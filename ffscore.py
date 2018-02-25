class FFScore_lib():

    def __init__(self, _period = '1d'):
        pass

    def algo(self, context, algo_ratio, portfolio_value):
        '''
        FFScore algorithms
        输入参数：FFScore_ratio, protfolio_value
        输出参数：FFScore_trade_ratio
        自有类  : FFScore_lib
        调用类  : quantlib
        '''
        # 调仓
        statsDate = context.current_dt.date()
        # 取得待购列表
        stock_list = self.fun_get_stock_list(context, statsDate)

        # 分配仓位
        equity_ratio, bonds_ratio = g.quantlib.fun_assetAllocationSystem(stock_list, context.moneyfund, statsDate)

        # 根据预设的风险敞口，计算交易时的比例
        trade_ratio = g.quantlib.fun_calPosition(equity_ratio, bonds_ratio, algo_ratio, context.moneyfund, portfolio_value, statsDate)

        return trade_ratio

    def fun_get_stock_list(self, context, statsDate):
        def __cal_FFScore(stock_list, FFScore, new_list):
            for stock in stock_list:
                if stock in new_list:
                    if stock in FFScore:
                        FFScore[stock] += 1
                    else:
                        FFScore[stock] = 1
                elif stock not in FFScore:
                    FFScore[stock] = 0
            return FFScore

        df = get_fundamentals(
            query(valuation.code, valuation.pb_ratio),
            date = statsDate - dt.timedelta(1)
        )
        # 1）市净率全市场从小到大前20%（剔除市净率为负的股票）
        df = df.sort(['pb_ratio'], ascending=[True])
        df = df.reset_index(drop = True)
        df = df[df.pb_ratio > 0]
        df = df.reset_index(drop = True)
        df = df[0:int(len(df)*0.05)]  #股票太多，所以变更了比例
        stock_list = list(df['code'])

        #2) 盈利水平打分
        # 2.1 资产收益率（ROE）：收益率为正数时ROE=1，否则为0。
        df = get_fundamentals(
            query(indicator.code, indicator.roe),
            date = statsDate - dt.timedelta(1)
        )
        df = df[df.code.isin(stock_list)]
        df = df.reset_index(drop = True)
        df = df[df.roe > 0]
        df = df.reset_index(drop=True)
        list_roe = list(df['code'])

        FFScore = {}
        FFScore = __cal_FFScore(stock_list, FFScore, list_roe)
        
        #2.2 资产收益率变化（△ROA）：当期最新可得财务报告的ROA同比的变化。变化为正数时△ROA=1，否则为0。
        df = get_fundamentals(
            query(indicator.code, indicator.roa),
            date = statsDate - dt.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(indicator.code, indicator.roa),
            date = statsDate - dt.timedelta(365)
        )
        df = df[df.code.isin(stock_list)]
        df = df.reset_index(drop = True)
        df.index = list(df['code'])
        df = df.drop(['code'], axis=1)
        dict1 = df.to_dict()['roa']

        df2 = df2[df2.code.isin(stock_list)]
        df2 = df2.reset_index(drop = True)
        df2.index = list(df2['code'])
        df2 = df2.drop(['code'], axis=1)
        dict2 = df2.to_dict()['roa']
        
        tmpList = []
        for stock in dict1.keys():
            if stock in dict2:
                if dict1[stock] > dict2[stock]:
                    tmpList.append(stock)
        FFScore = __cal_FFScore(stock_list, FFScore, tmpList)

        # 3)财务杠杆和流动性
        # 3.1 杠杆变化（△LEVER）：杠杆通过非流动负债合计除以非流动资产合计计算，杠杆变化为当期最新可得财务报告的杠杆同比的变化。变化为负数时△LEVER=1，否则为0。
        df = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - dt.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - dt.timedelta(365)
        )
        
        df3 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - dt.timedelta(730)
        )

        df['total_non_current_assets_before'] = df2['total_non_current_assets']
        df = df.dropna()
        df = df[df.code.isin(stock_list)]
        df['LEVER'] = 2.0*df['total_non_current_liability'] / (df['total_non_current_assets'] + df['total_non_current_assets_before'])
        df.index = list(df['code'])
        df = df.drop(['code', 'total_non_current_assets', 'total_non_current_liability', 'total_non_current_assets_before'], axis=1)
        dict1 = df.to_dict()['LEVER']

        df2['total_non_current_assets_before'] = df3['total_non_current_assets']
        df2 = df2.dropna()
        df2 = df2[df2.code.isin(stock_list)]
        df2['LEVER'] = 2.0*df2['total_non_current_liability'] / (df2['total_non_current_assets'] + df2['total_non_current_assets_before'])
        df2.index = list(df2['code'])
        df2 = df2.drop(['code', 'total_non_current_assets', 'total_non_current_liability', 'total_non_current_assets_before'], axis=1)
        dict2 = df2.to_dict()['LEVER']

        tmpList = []
        for stock in dict1.keys():
            if stock in dict2:
                if dict1[stock] < dict2[stock]:
                    tmpList.append(stock)
        FFScore = __cal_FFScore(stock_list, FFScore, tmpList)
        '''
        df = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - dt.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - dt.timedelta(365)
        )

        df = df[df.code.isin(stock_list)]
        df['LEVER'] = df['total_non_current_liability'] / df['total_non_current_assets']
        df.index = list(df['code'])
        df = df.drop(['code', 'total_non_current_assets', 'total_non_current_liability'], axis=1)
        dict1 = df.to_dict()['LEVER']

        df2 = df2[df2.code.isin(stock_list)]
        df2['LEVER'] = df2['total_non_current_liability'] / df2['total_non_current_assets']
        df2.index = list(df2['code'])
        df2 = df2.drop(['code', 'total_non_current_assets', 'total_non_current_liability'], axis=1)
        dict2 = df2.to_dict()['LEVER']

        tmpList = []
        for stock in dict1.keys():
            if stock in dict2:
                if dict1[stock] < dict2[stock]:
                    tmpList.append(stock)
        FFScore = __cal_FFScore(stock_list, FFScore, tmpList)
        '''
        # 4）运营效率
        # 4.1 流动资产周转率变化（△CATURN）： 流动资产周转率变化为当期最新可得财务报告的资产周转率同比的变化。变化为正数时△CATURN =1，否则为0。
        # 主营业务收入与流动资产的比例来反映流动资产的周转速度，来衡量企业在生产运营上对流动资产的利用效率。
        df = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - dt.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - dt.timedelta(365)
        )

        df3 = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - dt.timedelta(730)
        )

        df['total_current_assets_before'] = df2['total_current_assets']
        df = df.dropna()
        df = df[df.code.isin(stock_list)]
        df['CATURN'] = (df['total_operating_revenue'] - df['non_operating_revenue']) / (df['total_current_assets'] + df['total_current_assets_before'])
        df.index = list(df['code'])
        df = df.drop(['code', 'total_current_assets', 'total_operating_revenue', 'non_operating_revenue', 'total_current_assets_before'], axis=1)
        dict1 = df.to_dict()['CATURN']

        df2['total_current_assets_before'] = df3['total_current_assets']
        df2 = df2.dropna()
        df2 = df2[df2.code.isin(stock_list)]
        df2['CATURN'] = (df2['total_operating_revenue'] - df2['non_operating_revenue']) / (df2['total_current_assets'] + df2['total_current_assets_before'])
        df2.index = list(df2['code'])
        df2 = df2.drop(['code', 'total_current_assets', 'total_operating_revenue', 'non_operating_revenue', 'total_current_assets_before'], axis=1)
        dict2 = df2.to_dict()['CATURN']
        
        tmpList = []
        for stock in dict1.keys():
            if stock in dict2:
                if dict1[stock] > dict2[stock]:
                    tmpList.append(stock)
        FFScore = __cal_FFScore(stock_list, FFScore, tmpList)

        # 4.2 资产周转率变化（△TURN）： 资产周转率通过总资产周转率除以平均资产总值计算，资产周转率变化为当期最新可得财务报告的资产周转率同比的变化。变化为正数时△TURN =1，否则为0
        df = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, balance.total_current_assets, balance.total_non_current_assets),
            date = statsDate - dt.timedelta(1)
        )
        df2 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, balance.total_current_assets, balance.total_non_current_assets),
            date = statsDate - dt.timedelta(365)
        )
        df3 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, balance.total_current_assets, balance.total_non_current_assets),
            date = statsDate - dt.timedelta(730)
        )

        df['total_assets'] = df['total_current_assets'] + df['total_non_current_assets']
        df2['total_assets'] = df2['total_current_assets'] + df2['total_non_current_assets']
        df3['total_assets'] = df3['total_current_assets'] + df3['total_non_current_assets']
        df['total_assets_before'] = df2['total_assets']
        df2['total_assets_before'] = df3['total_assets']
        df = df.dropna()
        df2 = df2.dropna()

        df = df[df.code.isin(stock_list)]
        df['TURN'] = (df['total_operating_revenue'] - df['non_operating_revenue']) / (df['total_assets'] + df['total_assets_before'])
        df.index = list(df['code'])
        df = df.drop(['code', 'total_operating_revenue', 'non_operating_revenue', 'total_current_assets', 'total_non_current_assets', 'total_assets', 'total_assets_before'], axis=1)
        dict1 = df.to_dict()['TURN']

        df2 = df2[df2.code.isin(stock_list)]
        df2['TURN'] = (df2['total_operating_revenue'] - df2['non_operating_revenue']) / (df2['total_assets'] + df2['total_assets_before'])
        df2.index = list(df2['code'])
        df2 = df2.drop(['code', 'total_operating_revenue', 'non_operating_revenue', 'total_current_assets', 'total_non_current_assets', 'total_assets', 'total_assets_before'], axis=1)
        dict2 = df2.to_dict()['TURN']

        tmpList = []
        for stock in dict1.keys():
            if stock in dict2:
                if dict1[stock] > dict2[stock]:
                    tmpList.append(stock)
        FFScore = __cal_FFScore(stock_list, FFScore, tmpList)

        stock_list = []
        for stock in FFScore.keys():
            if FFScore[stock] == 5:
                stock_list.append(stock)

        stock_list = g.quantlib.unpaused(stock_list)
        stock_list = g.quantlib.remove_st(stock_list, statsDate)

        return stock_list