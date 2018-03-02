def pick_strategy_gross_profit(buy_count):
    g.strategy_memo = '长线价值投资'

    pick_config = [
        [True, '', '低估价值选股', Underestimate_value_pick, {}],
        # [True, '', '布林线选股', Bolling_pick, {}],
        # [True, '', '股息率选股', Dividend_yield_pick, {}],
        [True, '', '过滤创业板', Filter_gem, {}],
        [True, '', '过滤ST,停牌,涨跌停股票', Filter_common, {}],
        [True, '', '权重排序', SortRules, {
            'config': [
                [True, '', 'FFScore价值打分', FFScore_value, {
                    'sort': SortType.desc
                    , 'weight': 50}],
                [True, '', '首席质量因子排序', Sort_gross_profit, {
                    'sort': SortType.desc,
                    'weight': 50}],
            ]}
        ],
        # [True, '', '低估价值选股', Underestimate_value_pick, {}],
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