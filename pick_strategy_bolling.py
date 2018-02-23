#布林策略
def pick_strategy_bolling(buy_count):
    g.strategy_memo = '布林线选股'

    pick_config = [
        [True, '', '过滤创业板', Filter_gem, {}],
        [True, '', '过滤ST,停牌,涨跌停股票', Filter_common, {}],
        [True, '', '布林线选股', Bolling_pick, {}],
        [True, '', '权重排序', SortRules, {
            'config': [
                [True, '20volumn', '20日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 50
                    , 'day': 20}],
                [True, '60volumn', '60日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 50
                    , 'day': 60}],
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