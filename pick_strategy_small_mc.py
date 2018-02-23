def pick_strategy_small_mc():
    g.strategy_memo = '小市值多因子选股'
    buy_count = 3

    pick_config = [
        # FD_Factor 第一个参数为因子，min=为最小值 max为最大值，=None则不限，默认都为None。min,max都写则为区间
        [True, '', '小市值选股', Pick_financial_data, {
            'factors': [
                # FD_Factor('valuation.circulating_market_cap', min=0, max=100)  # 流通市值0~100亿
                FD_Factor('valuation.pe_ratio', min=0, max=200)  # pe > 0
                , FD_Factor('indicator.eps', min=0)  # eps > 0
                # , FD_Factor('indicator.inc_revenue_year_on_year', min=20)
                # , FD_Factor('indicator.inc_net_profit_year_on_year', min=20)
                # ,FD_Factor('indicator.roe',min=1,max=50) # roe
            ],
            'order_by': 'valuation.circulating_market_cap',  # 按流通市值排序
            'sort': SortType.asc,  # 从小到大排序
            'limit': 200  # 只取前200只
        }],
        [True, '', '过滤创业板', Filter_gem, {}],
        [True, '', '过滤ST,停牌,涨跌停股票', Filter_common, {}],
        [True, '', '权重排序', SortRules, {
            'config': [
                [True, '', '流通市值排序', Sort_financial_data, {
                    'factor': 'valuation.circulating_market_cap',
                    'sort': SortType.asc
                    , 'weight': 40}],
                [True, '', '按当前价排序', Sort_price, {
                    'sort': SortType.asc
                    , 'weight': 20}],
                [True, '20growth', '20日涨幅排序', Sort_growth_rate, {
                    'sort': SortType.asc
                    , 'weight': 10
                    , 'day': 20}],
                [True, '60growth', '60日涨幅排序', Sort_growth_rate, {
                    'sort': SortType.asc
                    , 'weight': 10
                    , 'day': 60}],
                [True, '', '按换手率排序', Sort_turnover_ratio, {
                    'sort': SortType.asc
                    , 'weight': 10}],
            ]}
        ],
        [True, '', '获取最终选股数', Filter_buy_count, {
            'buy_count': buy_count  # 最终入选股票数
        }],
    ]
    pick_new = [
        [True, '_pick_stocks_', '选股', Pick_stocks, {
            'config': pick_config,
            'day_only_run_one': True
        }]
    ]

    return pick_new