# 克隆自聚宽文章：https://www.joinquant.com/post/6406
# 标题：面向对象策略框架升级版: 多因子选股+多因子权重排序示例策略。
# 作者：晚起的小虫

'''
多因子小市值策略示例
    经典二八择时
    多财务因子选股票池+其它过滤股票池
    对股票池多因子权重排序选择买股列表
    固定周期调仓
    固定买N只股，卖不在买股列表的股票。

by 晚起的小虫
'''
import numpy as np
import pandas as pd
import talib
from prettytable import PrettyTable
import types
import urllib2
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import datetime
import enum

try:
    import shipane_sdk
    from trader_sync import *
except:
    log.error("加载 shipane_sdk和trader_sync失败")
    pass


# 不同步的白名单，主要用于实盘易同步持仓时，不同步中的新股，需把新股代码添加到这里。
# 可把while_list另外放到研究的一个py文件里
def while_list():
    return ['000001.XSHE']


# ==================================策略配置==============================================
def select_strategy(context):
    g.strategy_memo = '多因子小市值策略'
    # **** 这里定义log输出的类类型,重要，一定要写。假如有需要自定义log，可更改这个变量
    g.log_type = Rule_loger
    # 判断是运行回测还是运行模拟
    g.is_sim_trade = context.run_params.type == 'sim_trade'
    index2 = '000016.XSHG'  # 大盘指数
    index8 = '399333.XSHE'  # 小盘指数
    buy_count = 3

    ''' ---------------------配置 调仓条件判断规则-----------------------'''
    # 调仓条件判断
    adjust_condition_config = [
        [True, '_time_c_', '调仓时间', Time_condition, {
            'times': [[14, 50]],  # 调仓时间列表，二维数组，可指定多个时间点
        }],
        [True, '_Stop_loss_by_price_', '指数最高低价比值止损器', Stop_loss_by_price, {
            'index': '000001.XSHG',  # 使用的指数,默认 '000001.XSHG'
            'day_count': 160,  # 可选 取day_count天内的最高价，最低价。默认160
            'multiple': 2.2  # 可选 最高价为最低价的multiple倍时，触 发清仓
        }],
        [True, '', '多指数20日涨幅止损器', Mul_index_stop_loss, {
            'indexs': [index2, index8],
            'min_rate': 0.005
        }],
        [True, '', '调仓日计数器', Period_condition, {
            'period': 3,  # 调仓频率,日
        }],
    ]
    adjust_condition_config = [
        [True, '_adjust_condition_', '调仓执行条件的判断规则组合', Group_rules, {
            'config': adjust_condition_config
        }]
    ]

    ''' --------------------------配置 选股规则----------------- '''
    pick_config = [
        # 测试的多因子选股,所选因子只作为示例。
        # 选用的财务数据参考 https://www.joinquant.com/data/dict/fundamentals
        # 传入参数的财务因子需为字符串，原因是直接传入如 indicator.eps 会存在序列化问题。
        # FD_Factor 第一个参数为因子，min=为最小值 max为最大值，=None则不限，默认都为None。min,max都写则为区间
        [True, '', '多因子选股票池', Pick_financial_data, {
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
        [True, '', '首席质量因子', Filter_financial_data, {
            'filters': [
                FD_Filter('valuation.pe_ratio',sort=SortType.desc, percent=80),
            ]
        }],
        [True, '', '过滤创业板', Filter_gem, {}],
        [True, '', '过滤ST,停牌,涨跌停股票', Filter_common, {}],
        [True, '', '权重排序', SortRules, {
            'config': [
                [True, '', '流通市值排序', Sort_financial_data, {
                    'factor': 'valuation.circulating_market_cap',
                    'sort': SortType.asc
                    , 'weight': 100}],
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
        [True, '_pick_stocks_', '选股', Pick_stocks2, {
            'config': pick_config,
            'day_only_run_one': True
        }]
    ]

    ''' --------------------------配置 4 调仓规则------------------ '''
    # # 通达信持仓字段不同名校正
    col_names = {'可用': u'可用', '市值': u'参考市值', '证券名称': u'证券名称', '资产': u'资产'
        , '证券代码': u'证券代码', '证券数量': u'证券数量', '可卖数量': u'可卖数量', '当前价': u'当前价', '成本价': u'成本价'
                 }
    adjust_position_config = [
        [True, '', '卖出股票', Sell_stocks, {}],
        [True, '', '买入股票', Buy_stocks, {
            'buy_count': buy_count  # 最终买入股票数
        }],
        [True, '_Show_postion_adjust_', '显示买卖的股票', Show_postion_adjust, {}],
        #实盘易同步持仓，把虚拟盘同步到实盘
        # [g.is_sim_trade, '_Shipane_manager_', '实盘易操作', Shipane_manager, {
        #     'host':'111.111.111.111',   # 实盘易IP
        #     'port':8888,    # 实盘易端口
        #     'key':'',   # 实盘易Key
        #     'client':'title:guangfa', # 实盘易client
        #     'strong_op':False,   # 强力同步模式，开启会强行同步两次。
        #     'col_names':col_names, # 指定实盘易返回的持仓字段映射
        #     'cost':context.portfolio.starting_cash, # 实盘的初始资金
        #     'get_white_list_func':while_list, # 不同步的白名单
        #     'sync_scale': 1,  # 实盘资金/模拟盘资金比例，建议1为好
        #     'log_level': ['debug', 'waring', 'error'],  # 实盘易日志输出级别
        #     'sync_with_change': True,  # 是否指定只有发生了股票操作时才进行同步 , 这里重要，避免无效同步！！！！
        # }],
        # # 模拟盘调仓邮件通知，暂时只试过QQ邮箱，其它邮箱不知道是否支持
        # [g.is_sim_trade, '_new_Email_notice_', '调仓邮件通知执行器', Email_notice, {
        #     'user': '123456@qq.com',    # QQmail
        #     'password': '123459486',    # QQmail密码
        #     'tos': ["接收者1<123456@qq.com>"], # 接收人Email地址，可多个
        #     'sender': '聚宽模拟盘',  # 发送人名称
        #     'strategy_name': g.strategy_memo, # 策略名称
        #     'send_with_change': False,   # 持仓有变化时才发送
        # }],
    ]
    adjust_position_config = [
        [True, '_Adjust_position_', '调仓执行规则组合', Adjust_position, {
            'config': adjust_position_config
        }]
    ]

    ''' --------------------------配置 辅助规则------------------ '''
    # 优先辅助规则，每分钟优先执行handle_data
    common_config_list = [
        [True, '', '设置系统参数', Set_sys_params, {
            'benchmark': '000300.XSHG'  # 指定基准为次新股指
        }],
        [True, '', '手续费设置器', Set_slip_fee, {}],
        [True, '', '持仓信息打印器', Show_position, {}],
        [True, '', '统计执行器', Stat, {}],
        # 用实盘易官方的同步API实现的同步，
        # 实盘配置参考 https://github.com/sinall/ShiPanE-Python-SDK#id15
        # [g.is_sim_trade,'','实盘易官方API同步',Shipane_Sync,{
        #     'manager':'manager-1'}],
        
        
        # [g.is_sim_trade, '_Purchase_new_stocks_', '实盘易申购新股', Purchase_new_stocks, {
        #     'times': [[11, 24]],
        #     'host':'111.111.111.111',   # 实盘易IP
        #     'port':8888,    # 实盘易端口
        #     'key':'',   # 实盘易Key
        #     'clients': ['title:zhaoshang', 'title:guolian'] # 实盘易client列表,即一个规则支持同一个实盘易下的多个帐号同时打新
        # }],
    ]
    common_config = [
        [True, '_other_pre_', '预先处理的辅助规则', Group_rules, {
            'config': common_config_list
        }]
    ]
    # 组合成一个总的策略
    g.main_config = (common_config
                     + adjust_condition_config
                     + pick_new
                     + adjust_position_config)


# ===================================聚宽调用==============================================
def initialize(context):
    # 策略配置
    select_strategy(context)
    # 创建策略组合
    g.main = Strategy_Group({'config': g.main_config
                                , 'g_class': Global_variable
                                , 'memo': g.strategy_memo
                                , 'name': '_main_'})
    g.main.initialize(context)

    # 打印规则参数
    g.main.log.info(g.main.show_strategy())


# 按分钟回测
def handle_data(context, data):
    # 保存context到全局变量量，主要是为了方便规则器在一些没有context的参数的函数里使用。
    g.main.g.context = context
    # 执行策略
    g.main.handle_data(context, data)


# 开盘
def before_trading_start(context):
    log.info("==========================================================================")
    g.main.g.context = context
    g.main.before_trading_start(context)


# 收盘
def after_trading_end(context):
    g.main.g.context = context
    g.main.after_trading_end(context)
    g.main.g.context = None


# 进程启动(一天一次)
def process_initialize(context):
    try:
        g.main.g.context = context
        g.main.process_initialize(context)
    except:
        pass


# 这里示例进行模拟更改回测时，如何调整策略,基本通用代码。
def after_code_changed(context):
    try:
        g.main
    except:
        print '更新代码->原先不是OO策略，重新调用initialize(context)。'
        initialize(context)
        return

    try:
        print '=> 更新代码'
        select_strategy(context)
        g.main.g.context = context
        g.main.update_params(context, {'config': g.main_config})
        g.main.after_code_changed(context)
        g.main.log.info(g.main.show_strategy())
    except Exception as e:
        # log.error('更新代码失败:' + str(e) + '\n重新创建策略')
        # initialize(context)
        pass


'''=================================基础类======================================='''


# '''----------------------------共同参数类-----------------------------------
# 1.考虑到规则的信息互通，完全分离也会增加很大的通讯量。适当的约定好的全局变量，可以增加灵活性。
# 2.因共同约定，也不影响代码重用性。
# 3.假如需要更多的共同参数。可以从全局变量类中继承一个新类并添加新的变量，并赋于所有的规则类。
#     如此达到代码重用与策略差异的解决方案。
# '''
class Rule_loger(object):
    def __init__(self, msg_header):
        try:
            self._owner_msg = msg_header + ':'
        except:
            self._owner_msg = '未知规则:'

    def debug(self, msg, *args, **kwargs):
        log.debug(self._owner_msg + msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        log.info(self._owner_msg + msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        log.warn(self._owner_msg + msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        log.error(self._owner_msg + msg, *args, **kwargs)


class Global_variable(object):
    context = None
    _owner = None
    stock_pindexs = [0]  # 指示是属于股票性质的子仓列表
    op_pindexs = [0]  # 提示当前操作的股票子仓Id
    buy_stocks = []  # 选股列表
    sell_stocks = []  # 卖出的股票列表
    # 以下参数需配置  Run_Status_Recorder 规则进行记录。
    is_empty_position = True  # True表示为空仓,False表示为持仓。
    run_day = 0  # 运行天数，持仓天数为正，空仓天数为负
    position_record = [False]  # 持仓空仓记录表。True表示持仓，False表示空仓。一天一个。

    def __init__(self, owner):
        self._owner = owner

    ''' ==============================持仓操作函数，共用================================'''

    # 开仓，买入指定价值的证券
    # 报单成功并成交（包括全部成交或部分成交，此时成交量大于0），返回True
    # 报单失败或者报单成功但被取消（此时成交量等于0），返回False
    # 报单成功，触发所有规则的when_buy_stock函数
    def open_position(self, sender, security, value, pindex=0):
        cur_price = get_close_price(security, 1, '1m')
        if math.isnan(cur_price):
            return False
        # 通过当前价，四乘五入的计算要买的股票数。
        amount = int(round(value / cur_price / 100) * 100)
        new_value = amount * cur_price

        order = order_target_value(security, new_value, pindex=pindex)
        if order != None and order.filled > 0:
            # 订单成功，则调用规则的买股事件 。（注：这里只适合市价，挂价单不适合这样处理）
            self._owner.on_buy_stock(security, order, pindex)
            return True
        return False

    # 按指定股数下单
    def order(self, sender, security, amount, pindex=0):
        cur_price = get_close_price(security, 1, '1m')
        if math.isnan(cur_price):
            return False
        position = self.context.portfolio.long_positions[security] if self.context is not None else None
        _order = order(security, amount, pindex=pindex)
        if _order != None and _order.filled > 0:
            # 订单成功，则调用规则的买股事件 。（注：这里只适合市价，挂价单不适合这样处理）
            if amount > 0:
                self._owner.on_buy_stock(security, _order, pindex)
            elif position is not None:
                self._owner.on_sell_stock(position, _order, pindex)
            return _order
        return _order

    # 平仓，卖出指定持仓
    # 平仓成功并全部成交，返回True
    # 报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
    # 报单成功，触发所有规则的when_sell_stock函数
    def close_position(self, sender, position, is_normal=True, pindex=0):
        security = position.security
        order = order_target_value(security, 0, pindex=pindex)  # 可能会因停牌失败
        if order != None:
            if order.filled > 0:
                self._owner.on_sell_stock(position, order, is_normal, pindex)
                if security not in self.sell_stocks:
                    self.sell_stocks.append(security)
                return True
        return False

    # 清空卖出所有持仓
    # 清仓时，调用所有规则的 when_clear_position
    def clear_position(self, sender, context, pindexs=[0]):
        pindexs = self._owner.before_clear_position(context, pindexs)
        # 对传入的子仓集合进行遍历清仓
        for pindex in pindexs:
            if context.portfolio.long_positions:
                sender.log.info(("[%d]==> 清仓，卖出所有股票") % (pindex))
                for stock in context.portfolio.long_positions.keys():
                    position = context.portfolio.long_positions[stock]
                    self.close_position(sender, position, False, pindex)
        # 调用规则器的清仓事件
        self._owner.on_clear_position(context, pindexs)

    # 通过对象名 获取对象
    def get_obj_by_name(self, name):
        return self._owner.get_obj_by_name(name)

    # 调用外部的on_log额外扩展事件
    def on_log(sender, msg, msg_type):
        pass

    # 获取当前运行持续天数，持仓返回正，空仓返回负，ignore_count为是否忽略持仓过程中突然空仓的天数也认为是持仓。或者空仓时相反。
    def get_run_day_count(self, ignore_count=1):
        if ignore_count == 0:
            return self.run_day

        prs = self.position_record
        false_count = 0
        init = prs[-1]
        count = 1
        for i in range(2, len(prs)):
            if prs[-i] != init:
                false_count += 1  # 失败个数+1
                if false_count > ignore_count:  # 连续不对超过 忽略噪音数。
                    if count < ignore_count:  # 如果统计的个数不足ignore_count不符，则应进行统计True或False反转
                        init = not init  # 反转
                        count += false_count  # 把统计失败的认为正常的加进去
                        false_count = 0  # 失败计数清0
                    else:
                        break
            else:
                count += 1  # 正常计数+1
                if false_count > 0:  # 存在被忽略的噪音数则累回来，认为是正常的
                    count += false_count
                    false_count = 0
        return count if init else -count  # 统计结束，返回结果。init为True返回正数，为False返回负数。


# ''' ==============================规则基类================================'''
# 指定规则的类型，在Strategy_Group的 handle_data里起作用
class Rule_Level(enum.Enum):
    Normal = 0  # 普通规则
    Prior = 1  # 优先执行的规则
    Finally = 2  # 在执行完其它规则后，必需执行的规则


# ''' ==============================规则基类================================'''
class Rule(object):
    g = None  # 所属的策略全局变量
    name = ''  # obj名，可以通过该名字查找到
    memo = ''  # 默认描述
    log = None
    # 执行是否需要退出执行序列动作，用于Group_Rule默认来判断中扯执行。
    is_to_return = False
    @property
    def level(self):
        return self._params.get('level', Rule_Level.Normal)

    def __init__(self, params):
        self._params = params.copy()
        # self.g = None  # 所属的策略全局变量
        # self.name = ''  # obj名，可以通过该名字查找到
        # self.memo = ''  # 默认描述
        # self.log = None
        # # 执行是否需要退出执行序列动作，用于Group_Rule默认来判断中扯执行。
        # self.is_to_return = False
        pass

    # 更改参数
    def update_params(self, context, params):
        self._params = params.copy()
        pass

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        pass

    def before_trading_start(self, context):
        self.is_to_return = False
        pass

    def after_trading_end(self, context):
        self.is_to_return = False
        pass

    def process_initialize(self, context):
        pass

    def after_code_changed(self, context):
        pass

    @property
    def to_return(self):
        return self.is_to_return

    # 卖出股票时调用的函数
    # price为当前价，amount为发生的股票数,is_normail正常规则卖出为True，止损卖出为False
    def on_sell_stock(self, position, order, is_normal, pindex=0):
        pass

    # 买入股票时调用的函数
    # price为当前价，amount为发生的股票数
    def on_buy_stock(self, stock, order, pindex=0):
        pass

    # 清仓前调用。
    def before_clear_position(self, context, pindexs=[0]):
        return pindexs

    # 清仓时调用的函数
    def on_clear_position(self, context, pindexs=[0]):
        pass

    # handle_data没有执行完 退出时。
    def on_handle_data_exit(self, context, data):
        pass

    # record副曲线
    def record(self, **kwargs):
        if self._params.get('record', False):
            record(**kwargs)

    def set_g(self, g):
        self.g = g

    def __str__(self):
        return self.memo


# ''' ==============================策略组合器================================'''
# 通过此类或此类的子类，来规整集合其它规则。可嵌套，实现规则树，实现多策略组合。
class Group_rules(Rule):
    rules = []
    # 规则配置list下标描述变量。提高可读性与未来添加更多规则配置。
    cs_enabled, cs_name, cs_memo, cs_class_type, cs_param = range(5)

    def __init__(self, params):
        Rule.__init__(self, params)
        self.config = params.get('config', [])
        pass

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.config = params.get('config', self.config)

    def initialize(self, context):
        # 创建规则
        self.rules = self.create_rules(self.config)
        for rule in self.rules:
            rule.initialize(context)
        pass

    def handle_data(self, context, data):
        for rule in self.rules:
            rule.handle_data(context, data)
            if rule.to_return:
                self.is_to_return = True
                return
        self.is_to_return = False
        pass

    def before_trading_start(self, context):
        Rule.before_trading_start(self, context)
        for rule in self.rules:
            rule.before_trading_start(context)
        pass

    def after_trading_end(self, context):
        Rule.after_code_changed(self, context)
        for rule in self.rules:
            rule.after_trading_end(context)
        pass

    def process_initialize(self, context):
        Rule.process_initialize(self, context)
        for rule in self.rules:
            rule.process_initialize(context)
        pass

    def after_code_changed(self, context):
        # 重整所有规则
        # print self.config
        self.rules = self.check_chang(context, self.rules, self.config)
        # for rule in self.rules:
        #     rule.after_code_changed(context)

        pass

    # 检测新旧规则配置之间的变化。
    def check_chang(self, context, rules, config):
        nl = []
        for c in config:
            # 按顺序循环处理新规则
            if not c[self.cs_enabled]:  # 不使用则跳过
                continue
            # print c[self.cs_memo]
            # 查找旧规则是否存在
            find_old = None
            for old_r in rules:
                if old_r.__class__ == c[self.cs_class_type] and old_r.name == c[self.cs_name]:
                    find_old = old_r
                    break
            if find_old is not None:
                # 旧规则存在则添加到新列表中,并调用规则的更新函数，更新参数。
                nl.append(find_old)
                find_old.memo = c[self.cs_memo]
                find_old.log = g.log_type(c[self.cs_memo])
                find_old.update_params(context, c[self.cs_param])
                find_old.after_code_changed(context)
            else:
                # 旧规则不存在，则创建并添加
                new_r = self.create_rule(c[self.cs_class_type], c[self.cs_param], c[self.cs_name], c[self.cs_memo])
                nl.append(new_r)
                # 调用初始化时该执行的函数
                new_r.initialize(context)
        return nl

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        for rule in self.rules:
            rule.on_sell_stock(position, order, is_normal, new_pindex)

    # 清仓前调用。
    def before_clear_position(self, context, pindexs=[0]):
        for rule in self.rules:
            pindexs = rule.before_clear_position(context, pindexs)
        return pindexs

    def on_buy_stock(self, stock, order, pindex=0):
        for rule in self.rules:
            rule.on_buy_stock(stock, order, pindex)

    def on_clear_position(self, context, pindexs=[0]):
        for rule in self.rules:
            rule.on_clear_position(context, pindexs)

    def before_adjust_start(self, context, data):
        for rule in self.rules:
            rule.before_adjust_start(context, data)

    def after_adjust_end(self, context, data):
        for rule in self.rules:
            rule.after_adjust_end(context, data)

    # 创建一个规则执行器，并初始化一些通用事件
    def create_rule(self, class_type, params, name, memo):
        obj = class_type(params)
        # obj.g = self.g
        obj.set_g(self.g)
        obj.name = name
        obj.memo = memo
        obj.log = g.log_type(obj.memo)
        # print g.log_type,obj.memo
        return obj

    # 根据规则配置创建规则执行器
    def create_rules(self, config):
        # config里 0.是否启用，1.描述，2.规则实现类名，3.规则传递参数(dict)]
        return [self.create_rule(c[self.cs_class_type], c[self.cs_param], c[self.cs_name], c[self.cs_memo]) for c in
                config if c[self.cs_enabled]]

    # 显示规则组合，嵌套规则组合递归显示
    def show_strategy(self, level_str=''):
        s = '\n' + level_str + str(self)
        level_str = '    ' + level_str
        for i, r in enumerate(self.rules):
            if isinstance(r, Group_rules):
                s += r.show_strategy('%s%d.' % (level_str, i + 1))
            else:
                s += '\n' + '%s%d. %s' % (level_str, i + 1, str(r))
        return s

    # 通过name查找obj实现
    def get_obj_by_name(self, name):
        if name == self.name:
            return self

        f = None
        for rule in self.rules:
            if isinstance(rule, Group_rules):
                f = rule.get_obj_by_name(name)
                if f != None:
                    return f
            elif rule.name == name:
                return rule
        return f

    def __str__(self):
        return self.memo  # 返回默认的描述


# 策略组合器
class Strategy_Group(Group_rules):
    def initialize(self, context):
        self.g = self._params.get('g_class', Global_variable)(self)
        self.memo = self._params.get('memo', self.memo)
        self.name = self._params.get('name', self.name)
        self.log = g.log_type(self.memo)
        self.g.context = context
        Group_rules.initialize(self, context)

    def handle_data_level(self, context, data, level):
        for rule in self.rules:
            if rule.level != level:
                continue
            rule.handle_data(context, data)
            if rule.to_return and not isinstance(rule, Strategy_Group):  # 这里新增控制，假如是其它策略组合器要求退出的话，不退出。
                self.is_to_return = True
                return
        self.is_to_return = False
        pass

    def handle_data(self, context, data):
        self.handle_data_level(context, data, Rule_Level.Prior)
        self.handle_data_level(context, data, Rule_Level.Normal)
        self.handle_data_level(context, data, Rule_Level.Finally)

    # 重载 set_g函数,self.g不再被外部修改
    def set_g(self, g):
        if self.g is None:
            self.g = g


'''==================================调仓条件相关规则========================================='''


# '''===========带权重的退出判断基类==========='''
class Weight_Base(Rule):
    @property
    def weight(self):
        return self._params.get('weight', 1)


# '''-------------------------调仓时间控制器-----------------------'''
class Time_condition(Weight_Base):
    def __init__(self, params):
        Weight_Base.__init__(self, params)
        # 配置调仓时间 times为二维数组，示例[[10,30],[14,30]] 表示 10:30和14：30分调仓
        self.times = params.get('times', [])

    def update_params(self, context, params):
        Weight_Base.update_params(self, context, params)
        self.times = params.get('times', self.times)
        pass

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        self.is_to_return = not [hour, minute] in self.times
        pass

    def __str__(self):
        return '调仓时间控制器: [调仓时间: %s ]' % (
            str(['%d:%d' % (x[0], x[1]) for x in self.times]))


# '''-------------------------调仓日计数器-----------------------'''
class Period_condition(Weight_Base):
    def __init__(self, params):
        Weight_Base.__init__(self, params)
        # 调仓日计数器，单位：日
        self.period = params.get('period', 3)
        self.day_count = 0

    def update_params(self, context, params):
        Weight_Base.update_params(self, context, params)
        self.period = params.get('period', self.period)

    def handle_data(self, context, data):
        self.log.info("调仓日计数 [%d]" % (self.day_count))
        self.is_to_return = self.day_count % self.period != 0
        self.day_count += 1
        pass

    def on_sell_stock(self, position, order, is_normal, pindex=0):
        if not is_normal:
            # 个股止损止盈时，即非正常卖股时，重置计数，原策略是这么写的
            self.day_count = 0
        pass

    # 清仓时调用的函数
    def on_clear_position(self, context, new_pindexs=[0]):
        self.day_count = 0
        pass

    def __str__(self):
        return '调仓日计数器:[调仓频率: %d日] [调仓日计数 %d]' % (
            self.period, self.day_count)


class Stop_loss_by_price(Rule):
    def __init__(self, params):
        self.index = params.get('index', '000001.XSHG')
        self.day_count = params.get('day_count', 160)
        self.multiple = params.get('multiple', 2.2)
        self.is_day_stop_loss_by_price = False

    def update_params(self, context, params):
        self.index = params.get('index', self.index)
        self.day_count = params.get('day_count', self.day_count)
        self.multiple = params.get('multiple', self.multiple)

    def handle_data(self, context, data):
        # 大盘指数前130日内最高价超过最低价2倍，则清仓止损
        # 基于历史数据判定，因此若状态满足，则当天都不会变化
        # 增加此止损，回撤降低，收益降低

        if not self.is_day_stop_loss_by_price:
            h = attribute_history(self.index, self.day_count, unit='1d', fields=('close', 'high', 'low'),
                                  skip_paused=True)
            low_price_130 = h.low.min()
            high_price_130 = h.high.max()
            if high_price_130 > self.multiple * low_price_130 and h['close'][-1] < h['close'][-4] * 1 and h['close'][
                -1] > h['close'][-100]:
                # 当日第一次输出日志
                self.log.info("==> 大盘止损，%s指数前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    get_security_info(self.index).display_name, high_price_130, low_price_130))
                self.is_day_stop_loss_by_price = True

        if self.is_day_stop_loss_by_price:
            self.g.clear_position(self, context, self.g.op_pindexs)
        self.is_to_return = self.is_day_stop_loss_by_price

    def before_trading_start(self, context):
        self.is_day_stop_loss_by_price = False
        pass

    def __str__(self):
        return '大盘高低价比例止损器:[指数: %s] [参数: %s日内最高最低价: %s倍] [当前状态: %s]' % (
            self.index, self.day_count, self.multiple, self.is_day_stop_loss_by_price)


# '''-------------多指数N日涨幅止损------------'''
class Mul_index_stop_loss(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        self._indexs = params.get('indexs', [])
        self._min_rate = params.get('min_rate', 0.01)
        self._n = params.get('n', 20)

    def update_params(self, context, params):
        Rule.__init__(self, params)
        self._indexs = params.get('indexs', [])
        self._min_rate = params.get('min_rate', 0.01)
        self._n = params.get('n', 20)

    def handle_data(self, context, data):
        self.is_to_return = False
        r = []
        for index in self._indexs:
            gr_index = get_growth_rate(index, self._n)
            self.log.info('%s %d日涨幅  %.2f%%' % (show_stock(index), self._n, gr_index * 100))
            r.append(gr_index > self._min_rate)
        if sum(r) == 0:
            self.log.warn('不符合持仓条件，清仓')
            self.g.clear_position(self, context, self.g.op_pindexs)
            self.is_to_return = True

    def after_trading_end(self, context):
        Rule.after_trading_end(self, context)
        for index in self._indexs:
            gr_index = get_growth_rate(index, self._n - 1)
            self.log.info('%s %d日涨幅  %.2f%% ' % (show_stock(index), self._n - 1, gr_index * 100))

    def __str__(self):
        return '多指数20日涨幅损器[指数:%s] [涨幅:%.2f%%]' % (str(self._indexs), self._min_rate * 100)


'''=========================选股规则相关==================================='''


# '''==============================选股 query过滤器基类=============================='''
class Filter_query(Rule):
    def filter(self, context, data, q):
        return None


# '''==============================选股 stock_list过滤器基类=============================='''
class Filter_stock_list(Rule):
    def filter(self, context, data, stock_list):
        return None


# '''-----------------选股组合器2-----------------------'''
class Pick_stocks2(Group_rules):
    def __init__(self, params):
        Group_rules.__init__(self, params)
        self.has_run = False

    def handle_data(self, context, data):
        try:
            to_run_one = self._params.get('day_only_run_one', False)
        except:
            to_run_one = False
        if to_run_one and self.has_run:
            self.log.info('设置一天只选一次，跳过选股。')
            return

        q = None
        for rule in self.rules:
            if isinstance(rule, Filter_query):
                q = rule.filter(context, data, q)
        stock_list = list(get_fundamentals(q)['code']) if q != None else []
        for rule in self.rules:
            if isinstance(rule, Filter_stock_list):
                stock_list = rule.filter(context, data, stock_list)
        self.g.buy_stocks = stock_list
        if len(self.g.buy_stocks) > 5:
            tl = self.g.buy_stocks[0:5]
        else:
            tl = self.g.buy_stocks[:]
        self.log.info('选股:\n' + join_list(["[%s]" % (show_stock(x)) for x in tl], ' ', 10))
        self.has_run = True

    def before_trading_start(self, context):
        self.has_run = False

    def __str__(self):
        return self.memo


# 选取财务数据的参数
# 使用示例 FD_Factor('valuation.market_cap',None,100) #先取市值小于100亿的股票
# 注：传入类型为 'valuation.market_cap'字符串而非 valuation.market_cap 是因 valuation.market_cap等存在序列化问题！！
# 具体传入field 参考  https://www.joinquant.com/data/dict/fundamentals
class FD_Factor(object):
    def __init__(self, factor, **kwargs):
        self.factor = factor
        self.min = kwargs.get('min', None)
        self.max = kwargs.get('max', None)


# 过滤财务数据参数
# 使用示例 FD_Filter('valuation.pe_ratio',sort=SortType.desc,percent=80) 选取市盈率最大的80%
class FD_Filter(object):
    def __init__(self, factor, **kwargs):
        self.factor = factor
        self.sort = kwargs.get('sort', SortType.asc)
        self.percent = kwargs.get('percent', None)


# 根据多字段财务数据一次选股，返回一个Query
class Pick_financial_data(Filter_query):
    def filter(self, context, data, q):
        if q is None:
            #             q = query(valuation,balance,cash_flow,income,indicator)
            q = query(valuation)

        for fd_param in self._params.get('factors', []):
            if not isinstance(fd_param, FD_Factor):
                continue
            if fd_param.min is None and fd_param.max is None:
                continue
            factor = eval(fd_param.factor)
            if fd_param.min is not None:
                q = q.filter(
                    factor > fd_param.min
                )
            if fd_param.max is not None:
                q = q.filter(
                    factor < fd_param.max
                )
        order_by = eval(self._params.get('order_by', None))
        sort_type = self._params.get('sort', SortType.asc)
        if order_by is not None:
            if sort_type == SortType.asc:
                q = q.order_by(order_by.asc())
            else:
                q = q.order_by(order_by.desc())

        limit = self._params.get('limit', None)
        if limit is not None:
            q = q.limit(limit)

        return q

    def __str__(self):
        s = ''
        for fd_param in self._params.get('factors', []):
            if not isinstance(fd_param, FD_Factor):
                continue
            if fd_param.min is None and fd_param.max is None:
                continue
            s += '\n\t\t\t\t---'
            if fd_param.min is not None and fd_param.max is not None:
                s += '[ %s < %s < %s ]' % (fd_param.min, fd_param.factor, fd_param.max)
            elif fd_param.min is not None:
                s += '[ %s < %s ]' % (fd_param.min, fd_param.factor)
            elif fd_param.max is not None:
                s += '[ %s > %s ]' % (fd_param.factor, fd_param.max)

        order_by = self._params.get('order_by', None)
        sort_type = self._params.get('sort', SortType.asc)
        if order_by is not None:
            s += '\n\t\t\t\t---'
            sort_type = '从小到大' if sort_type == SortType.asc else '从大到小'
            s += '[排序:%s %s]' % (order_by, sort_type)
        limit = self._params.get('limit', None)
        if limit is not None:
            s += '\n\t\t\t\t---'
            s += '[限制选股数:%s]' % (limit)
        return '多因子选股:' + s


# 根据财务数据对Stock_list进行过滤。返回符合条件的stock_list
class Filter_financial_data(Filter_stock_list):
    def filter(self, context, data, stock_list):
        for fd_param in self._params.get('filters', []):
            q = query(valuation).filter(
                valuation.code.in_(stock_list)
            )
            if not isinstance(fd_param, FD_Filter):
                continue
            if fd_param.sort is None and fd_param.percent is None:
                continue
            factor = eval(fd_param.factor)
            if fd_param.sort is not None:
                sort_type = fd_param.sort
                if sort_type == SortType.asc:
                    q = q.order_by(factor.asc())
                else:
                    q = q.order_by(factor.desc())
            stock_list = list(get_fundamentals(q)['code'])
            if fd_param.percent is not None:
                stock_list = stock_list[0 : int(len(stock_list) * (fd_param.percent)/100)]
        
        return stock_list

    def __str__(self):
        s = ''
        for fd_param in self._params.get('filters', []):
            factor = fd_param.factor
            sort_type = fd_param.sort
            s += '\n\t\t\t\t---'
            sort_type = '从小到大' if sort_type == SortType.asc else '从大到小'
            s += '[排序:%s %s]' % (factor, sort_type)
            percent = fd_param.percent
            if percent is not None:
                s += '\n\t\t\t\t---'
                s += '[选择前百分之:%s]' % (percent)

        return '多因子过滤:' + s


# '''------------------创业板过滤器-----------------'''
class Filter_gem(Filter_stock_list):
    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:3] != '300']

    def __str__(self):
        return '过滤创业板股票'


class Filter_common(Filter_stock_list):
    def __init__(self, params):
        self.filters = params.get('filters', ['st', 'high_limit', 'low_limit', 'pause'])

    def filter(self, context, data, stock_list):
        current_data = get_current_data()
        if 'st' in self.filters:
            stock_list = [stock for stock in stock_list
                          if not current_data[stock].is_st
                          and 'ST' not in current_data[stock].name
                          and '*' not in current_data[stock].name
                          and '退' not in current_data[stock].name]
        if 'high_limit' in self.filters:
            stock_list = [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                          or data[stock].close < data[stock].high_limit]
        if 'low_limit' in self.filters:
            stock_list = [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                          or data[stock].close > data[stock].low_limit]
        if 'pause' in self.filters:
            stock_list = [stock for stock in stock_list if not current_data[stock].paused]
        return stock_list

    def __str__(self):
        return '一般性股票过滤器:%s' % (str(self.filters))


import enum


# 因子排序类型
class SortType(enum.Enum):
    asc = 0  # 从小到大排序
    desc = 1  # 从大到小排序


# 价格因子排序选用的价格类型
class PriceType(enum.Enum):
    now = 0  # 当前价
    today_open = 1  # 开盘价
    pre_day_open = 2  # 昨日开盘价
    pre_day_close = 3  # 收盘价
    ma = 4  # N日均价


# 排序基本类 共用指定参数为 weight
class SortBase(Rule):
    @property
    def weight(self):
        return self._params.get('weight', 1)

    @property
    def is_asc(self):
        return self._params.get('sort', SortType.asc) == SortType.asc

    def _sort_type_str(self):
        return '从小到大' if self.is_asc else '从大到小'

    def sort(self, context, data, stock_list):
        return stock_list


# '''--多因子计算：每个规则产生一个排名，并根据排名和权重进行因子计算--'''
class SortRules(Group_rules, Filter_stock_list):
    def filter(self, context, data, stock_list):
        self.log.info(join_list([show_stock(stock) for stock in stock_list[:10]], ' ', 10))
        sorted_stocks = []
        total_weight = 0  # 总权重。
        for rule in self.rules:
            if isinstance(rule, SortBase):
                total_weight += rule.weight
        for rule in self.rules:
            if not isinstance(rule, SortBase):
                continue
            if rule.weight == 0:
                continue  # 过滤权重为0的排序规则，为以后批量自动调整权重作意外准备
            stocks = stock_list[:]  # 为防排序规则搞乱list，每次都重新复制一份
            # 获取规则排序
            tmp_stocks = rule.sort(context, data, stocks)
            rule.log.info(join_list([show_stock(stock) for stock in tmp_stocks[:10]], ' ', 10))

            for stock in stock_list:
                # 如果被评分器删除，则不增加到总评分里
                if stock not in tmp_stocks:
                    stock_list.remove(stock)

            sd = {}
            rule_weight = rule.weight * 1.0 / total_weight
            for i, stock in enumerate(tmp_stocks):
                sd[stock] = (i + 1) * rule_weight
            sorted_stocks.append(sd)
        result = []

        for stock in stock_list:
            total_score = 0
            for sd in sorted_stocks:
                score = sd.get(stock, 0)
                if score == 0:  # 如果评分为0 则直接不再统计其它的
                    total_score = 0
                    break
                else:
                    total_score += score
            if total_score != 0:
                result.append([stock, total_score])
        result = sorted(result, key=lambda x: x[1])
        # 仅返回股票列表 。
        return [stock for stock, score in result]

    def __str__(self):
        return '多因子权重排序器'


# 按N日增长率排序
# day 指定按几日增长率计算,默认为20
class Sort_growth_rate(SortBase):
    def sort(self, context, data, stock_list):
        day = self._params.get('day', 20)
        r = []
        for stock in stock_list:
            rate = get_growth_rate(stock, day)
            if rate != 0:
                r.append([stock, rate])
        r = sorted(r, key=lambda x: x[1], reverse=not self.is_asc)
        return [stock for stock, rate in r]

    def __str__(self):
        return '[权重: %s ] [排序: %s ] 按 %d 日涨幅排序' % (self.weight, self._sort_type_str(), self._params.get('day', 20))


class Sort_price(SortBase):
    def sort(self, context, data, stock_list):
        r = []
        price_type = self._params.get('price_type', PriceType.now)
        if price_type == PriceType.now:
            for stock in stock_list:
                close = data[stock].close
                r.append([stock, close])
        elif price_type == PriceType.today_open:
            curr_data = get_current_data()
            for stock in stock_list:
                r.append([stock, curr_data[stock].day_open])
        elif price_type == PriceType.pre_day_open:
            stock_data = history(count=1, unit='1d', field='open', security_list=stock_list, df=False, skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock][0]])
        elif price_type == PriceType.pre_day_close:
            stock_data = history(count=1, unit='1d', field='close', security_list=stock_list, df=False,
                                 skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock][0]])
        elif price_type == PriceType.ma:
            n = self._params.get('period', 20)
            stock_data = history(count=n, unit='1d', field='close', security_list=stock_list, df=False,
                                 skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock].mean()])

        r = sorted(r, key=lambda x: x[1], reverse=not self.is_asc)
        return [stock for stock, close in r]

    def __str__(self):
        s = '[权重: %s ] [排序: %s ] 按当 %s 价格排序' % (
            self.weight, self._sort_type_str(), str(self._params.get('price_type', PriceType.now)))
        if self._params.get('price_type', PriceType.now) == PriceType.ma:
            s += ' [%d 日均价]' % (self._params.get('period', 20))
        return s


# --- 按换手率排序 ---
class Sort_turnover_ratio(SortBase):
    def sort(self, context, data, stock_list):
        q = query(valuation.code, valuation.turnover_ratio).filter(
            valuation.code.in_(stock_list)
        )
        if self.is_asc:
            q = q.order_by(valuation.turnover_ratio.asc())
        else:
            q = q.order_by(valuation.turnover_ratio.desc())
        stock_list = list(get_fundamentals(q)['code'])
        return stock_list

    def __str__(self):
        return '[权重: %s ] [排序: %s ] 按换手率排序 ' % (self.weight, self._sort_type_str())


# --- 按财务数据排序 ---
class Sort_financial_data(SortBase):
    def sort(self, context, data, stock_list):
        factor = eval(self._params.get('factor', None))
        if factor is None:
            return stock_list
        q = query(valuation).filter(
            valuation.code.in_(stock_list)
        )
        if self.is_asc:
            q = q.order_by(factor.asc())
        else:
            q = q.order_by(factor.desc())
        stock_list = list(get_fundamentals(q)['code'])
        return stock_list

    def __str__(self):
        return '[权重: %s ] [排序: %s ] %s' % (self.weight, self._sort_type_str(), self.memo)


# '''------------------截取欲购股票数-----------------'''
class Filter_buy_count(Filter_stock_list):
    def __init__(self, params):
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        if len(stock_list) > self.buy_count:
            return stock_list[:self.buy_count]
        else:
            return stock_list

    def __str__(self):
        return '获取最终待购买股票数:[ %d ]' % (self.buy_count)


'''===================================调仓相关============================'''


# '''------------------------调仓规则组合器------------------------'''
# 主要是判断规则集合有没有 before_adjust_start 和 after_adjust_end 方法
class Adjust_position(Group_rules):
    # 重载，实现调用 before_adjust_start 和 after_adjust_end 方法
    def handle_data(self, context, data):
        for rule in self.rules:
            if isinstance(rule, Adjust_expand):
                rule.before_adjust_start(context, data)

        Group_rules.handle_data(self, context, data)
        for rule in self.rules:
            if isinstance(rule, Adjust_expand):
                rule.after_adjust_end(context, data)
        if self.is_to_return:
            return


# '''==============================调仓规则器基类=============================='''
# 需要 before_adjust_start和after_adjust_end的子类可继承
class Adjust_expand(Rule):
    def before_adjust_start(self, context, data):
        pass

    def after_adjust_end(self, context, data):
        pass


# '''---------------卖出股票规则--------------'''
class Sell_stocks(Rule):
    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 卖出不在待买股票列表中的股票
        # 对于因停牌等原因没有卖出的股票则继续持有
        for pindex in self.g.op_pindexs:
            for stock in context.portfolio.long_positions.keys():
                if stock not in buy_stocks:
                    position = context.portfolio.long_positions[stock]
                    self.g.close_position(self, position, True, pindex)

    def __str__(self):
        return '股票调仓卖出规则：卖出不在buy_stocks的股票'


# '''---------------买入股票规则--------------'''
class Buy_stocks(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.buy_count = params.get('buy_count', self.buy_count)

    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 买入股票
        # 始终保持持仓数目为g.buy_stock_count
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        for pindex in self.g.op_pindexs:
            position_count = len(context.portfolio.long_positions)
            if self.buy_count > position_count:
                value = context.portfolio.available_cash / (self.buy_count - position_count)
                for stock in buy_stocks:
                    if stock in self.g.sell_stocks:
                        continue
                    if context.portfolio.long_positions[stock].total_amount == 0:
                        if self.g.open_position(self, stock, value, pindex):
                            if len(context.portfolio.long_positions) == self.buy_count:
                                break
        pass

    def after_trading_end(self, context):
        self.g.sell_stocks = []

    def __str__(self):
        return '股票调仓买入规则：现金平分式买入股票达目标股票数'


# '''------------------股票买卖操作记录-----------------'''
class Op_stocks_record(Adjust_expand):
    def __init__(self, params):
        Adjust_expand.__init__(self, params)
        self.op_buy_stocks = []
        self.op_sell_stocks = []
        self.position_has_change = False

    def on_buy_stock(self, stock, order, new_pindex=0):
        self.position_has_change = True
        self.op_buy_stocks.append([stock, order.filled])

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        self.position_has_change = True
        self.op_sell_stocks.append([position.security, -order.filled])

    def after_adjust_end(self, context, data):
        self.op_buy_stocks = self.merge_op_list(self.op_buy_stocks)
        self.op_sell_stocks = self.merge_op_list(self.op_sell_stocks)

    def after_trading_end(self, context):
        self.op_buy_stocks = []
        self.op_sell_stocks = []
        self.position_has_change = False

    # 对同一只股票的多次操作，进行amount合并计算。
    def merge_op_list(self, op_list):
        s_list = list(set([x[0] for x in op_list]))
        return [[s, sum([x[1] for x in op_list if x[0] == s])] for s in s_list]


# '''------------------股票操作显示器-----------------'''
class Show_postion_adjust(Op_stocks_record):
    def after_adjust_end(self, context, data):
        # 调用父类方法
        Op_stocks_record.after_adjust_end(self, context, data)
        if len(self.g.buy_stocks) > 0:
            if len(self.g.buy_stocks) > 5:
                tl = self.g.buy_stocks[0:5]
            else:
                tl = self.g.buy_stocks[:]
            self.log.info('选股:\n' + join_list(["[%s]" % (show_stock(x)) for x in tl], ' ', 10))
        # 显示买卖日志
        if len(self.op_sell_stocks) > 0:
            self.log.info(
                '\n' + join_list(["卖出 %s : %d" % (show_stock(x[0]), x[1]) for x in self.op_sell_stocks], '\n', 1))
        if len(self.op_buy_stocks) > 0:
            self.log.info(
                '\n' + join_list(["买入 %s : %d" % (show_stock(x[0]), x[1]) for x in self.op_buy_stocks], '\n', 1))
        # 显示完就清除
        self.op_buy_stocks = []
        self.op_sell_stocks = []

    def __str__(self):
        return '显示调仓时买卖的股票'


'''==================================其它=============================='''


# '''---------------------------------系统参数一般性设置---------------------------------'''
class Set_sys_params(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        try:
            # 一律使用真实价格
            set_option('use_real_price', self._params.get('use_real_price', True))
        except:
            pass
        try:
            # 过滤log
            log.set_level(*(self._params.get('level', ['order', 'error'])))
        except:
            pass
        try:
            # 设置基准
            set_benchmark(self._params.get('benchmark', '000300.XSHG'))
        except:
            pass
            # set_benchmark('399006.XSHE')
            # set_slippage(FixedSlippage(0.04))

    def __str__(self):
        return '设置系统参数：[使用真实价格交易] [忽略order 的 log] [设置基准]'


# '''------------------设置手续费-----------------'''
# 根据不同的时间段设置滑点与手续费并且更新指数成分股
class Set_slip_fee(Rule):
    def before_trading_start(self, context):
        # 根据不同的时间段设置手续费
        dt = context.current_dt
        if dt > datetime.datetime(2013, 1, 1):
            set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))

        elif dt > datetime.datetime(2011, 1, 1):
            set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))

        elif dt > datetime.datetime(2009, 1, 1):
            set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
        else:
            set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))

    def __str__(self):
        return '根据时间设置不同的交易费率'


# '''------------------持仓信息打印器-----------------'''
class Show_position(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        self.op_sell_stocks = []
        self.op_buy_stocks = []

    def after_trading_end(self, context):
        self.log.info(self.__get_portfolio_info_text(context, self.g.op_pindexs))
        self.op_buy_stocks = []
        self.op_buy_stocks = []

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        self.op_sell_stocks.append([position.security, order.filled])
        pass

    def on_buy_stock(self, stock, order, new_pindex=0):
        self.op_buy_stocks.append([stock, order.filled])
        pass

    # # 调仓后调用用
    # def after_adjust_end(self,context,data):
    #     print self.__get_portfolio_info_text(context,self.g.op_pindexs)
    #     pass
    # ''' ------------------------------获取持仓信息，普通文本格式------------------------------------------'''
    def __get_portfolio_info_text(self, context, op_sfs=[0]):
        sub_str = ''
        table = PrettyTable(["仓号", "股票", "持仓", "当前价", "盈亏", "持仓比"])
        # table.padding_width = 1# One space between column edges and contents (default)
        for sf_id in self.g.stock_pindexs:
            cash = context.portfolio.cash
            p_value = context.portfolio.positions_value
            total_values = p_value + cash
            if sf_id in op_sfs:
                sf_id_str = str(sf_id) + ' *'
            else:
                sf_id_str = str(sf_id)
            new_stocks = [x[0] for x in self.op_buy_stocks]
            for stock in context.portfolio.long_positions.keys():
                position = context.portfolio.long_positions[stock]
                if sf_id in op_sfs and stock in new_stocks:
                    stock_str = show_stock(stock) + ' *'
                else:
                    stock_str = show_stock(stock)
                stock_raite = (position.total_amount * position.price) / total_values * 100
                table.add_row([sf_id_str,
                               stock_str,
                               position.total_amount,
                               position.price,
                               "%.2f%%" % ((position.price - position.avg_cost) / position.avg_cost * 100),
                               "%.2f%%" % (stock_raite)]
                              )
            if sf_id < len(self.g.stock_pindexs) - 1:
                table.add_row(['----', '---------------', '-----', '----', '-----', '-----'])
            sub_str += '[仓号: %d] [总值:%d] [持股数:%d] [仓位:%.2f%%] \n' % (sf_id,
                                                                     total_values,
                                                                     len(context.portfolio.long_positions)
                                                                     , p_value * 100 / (cash + p_value))
        if len(context.portfolio.positions) == 0:
            return '子仓详情:\n' + sub_str
        else:
            return '子仓详情:\n' + sub_str + str(table)

    def __str__(self):
        return '持仓信息打印'


# ''' ----------------------统计类----------------------------'''
class Stat(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        # 加载统计模块
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    def after_trading_end(self, context):
        # self.report(context)
        self.print_win_rate(context.current_dt.strftime("%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)

    def on_sell_stock(self, position, order, is_normal, pindex=0):
        if order.filled > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            self.watch(position.security, order.filled, position.avg_cost, position.price)

    def reset(self):
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    # 记录交易次数便于统计胜率
    # 卖出成功后针对卖出的量进行盈亏统计
    def watch(self, stock, sold_amount, avg_cost, cur_price):
        self.trade_total_count += 1
        current_value = sold_amount * cur_price
        cost = sold_amount * avg_cost

        percent = round((current_value - cost) / cost * 100, 2)
        if current_value > cost:
            self.trade_success_count += 1
            win = [stock, percent]
            self.statis['win'].append(win)
        else:
            loss = [stock, percent]
            self.statis['loss'].append(loss)

    def report(self, context):
        cash = context.portfolio.cash
        totol_value = context.portfolio.portfolio_value
        position = 1 - cash / totol_value
        self.log.info("收盘后持仓概况:%s" % str(list(context.portfolio.positions)))
        self.log.info("仓位概况:%.2f" % position)
        self.print_win_rate(context.current_dt.strftime("%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)

    # 打印胜率
    def print_win_rate(self, current_date, print_date, context):
        if str(current_date) == str(print_date):
            win_rate = 0
            if 0 < self.trade_total_count and 0 < self.trade_success_count:
                win_rate = round(self.trade_success_count / float(self.trade_total_count), 3)

            most_win = self.statis_most_win_percent()
            most_loss = self.statis_most_loss_percent()
            starting_cash = context.portfolio.starting_cash
            total_profit = self.statis_total_profit(context)
            if len(most_win) == 0 or len(most_loss) == 0:
                return

            s = '\n----------------------------绩效报表----------------------------'
            s += '\n交易次数: {0}, 盈利次数: {1}, 胜率: {2}'.format(self.trade_total_count, self.trade_success_count,
                                                          str(win_rate * 100) + str('%'))
            s += '\n单次盈利最高: {0}, 盈利比例: {1}%'.format(most_win['stock'], most_win['value'])
            s += '\n单次亏损最高: {0}, 亏损比例: {1}%'.format(most_loss['stock'], most_loss['value'])
            s += '\n总资产: {0}, 本金: {1}, 盈利: {2}, 盈亏比率：{3}%'.format(starting_cash + total_profit, starting_cash,
                                                                  total_profit, total_profit / starting_cash * 100)
            s += '\n---------------------------------------------------------------'
            self.log.info(s)

    # 统计单次盈利最高的股票
    def statis_most_win_percent(self):
        result = {}
        for statis in self.statis['win']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] > result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计单次亏损最高的股票
    def statis_most_loss_percent(self):
        result = {}
        for statis in self.statis['loss']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] < result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计总盈利金额
    def statis_total_profit(self, context):
        return context.portfolio.portfolio_value - context.portfolio.starting_cash

    def __str__(self):
        return '策略绩效统计'


'''===============================其它基础函数=================================='''


def get_growth_rate(security, n=20):
    '''
    获取股票n日以来涨幅，根据当前价(前1分钟的close）计算
    n 默认20日  
    :param security: 
    :param n: 
    :return: float
    '''
    lc = get_close_price(security, n)
    c = get_close_price(security, 1, '1m')

    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" % (security, n, lc, c))
        return 0


def get_close_price(security, n, unit='1d'):
    '''
    获取前n个单位时间当时的收盘价
    为防止取不到收盘价，试3遍
    :param security: 
    :param n: 
    :param unit: '1d'/'1m'
    :return: float
    '''
    cur_price = np.nan
    for i in range(3):
        cur_price = attribute_history(security, n, unit, 'close', True)['close'][0]
        if not math.isnan(cur_price):
            break
    return cur_price


# 获取一个对象的类名
def get_obj_class_name(obj):
    cn = str(obj.__class__)
    cn = cn[cn.find('.') + 1:]
    return cn[:cn.find("'")]


def show_stock(stock):
    '''
    获取股票代码的显示信息    
    :param stock: 股票代码，例如: '603822.XSHG'
    :return: str，例如：'603822 嘉澳环保'
    '''
    return "%s %s" % (stock[:6], get_security_info(stock).display_name)


def join_list(pl, connector=' ', step=5):
    '''
    将list组合为str,按分隔符和步长换行显示(List的成员必须为字符型)
    例如：['1','2','3','4'],'~',2  => '1~2\n3~4'
    :param pl: List
    :param connector: 分隔符，默认空格 
    :param step: 步长，默认5
    :return: str
    '''
    result = ''
    for i in range(len(pl)):
        result += pl[i]
        if (i + 1) % step == 0:
            result += '\n'
        else:
            result += connector
    return result


'''=================================实盘易相关================================='''


# '''-------------------实盘易对接 同步持仓-----------------------'''
class Shipane_manager(Op_stocks_record):
    def __init__(self, params):
        Op_stocks_record.__init__(self, params)
        try:
            log
            self._logger = shipane_sdk._Logger()
        except NameError:
            import logging
            self._logger = logging.getLogger()
        self.moni_trader = JoinQuantTrader()
        self.shipane_trader = ShipaneTrader(self._logger, **params)
        self.syncer = TraderSynchronizer(self._logger
                                         , self.moni_trader
                                         , self.shipane_trader
                                         , normalize_code=normalize_code
                                         , **params)
        self._cost = params.get('cost', 100000)
        self._source_trader_record = []
        self._dest_trader_record = []

    def update_params(self, context, params):
        Op_stocks_record.update_params(self, context, params)
        self._cost = params.get('cost', 100000)
        self.shipane_trader = ShipaneTrader(self._logger, **params)
        self.syncer = TraderSynchronizer(self._logger
                                         , self.moni_trader
                                         , self.shipane_trader
                                         , normalize_code=normalize_code
                                         , **params)

    def after_adjust_end(self, context, data):
        # 是否指定只在有发生调仓动作时进行调仓
        if self._params.get('sync_with_change', True):
            if self.position_has_change:
                self.syncer.execute(context, data)
        else:
            self.syncer.execute(context, data)
        self.position_has_change = False

    def on_clear_position(self, context, pindex=[0]):
        if self._params.get('sync_with_change', True):
            if self.position_has_change:
                self.syncer.execute(context, None)
        else:
            self.syncer.execute(context, None)
        self.position_has_change = False

    def after_trading_end(self, context):
        Op_stocks_record.after_trading_end(self, context)
        try:
            self.moni_trader.context = context
            self.shipane_trader.context = context
            # 记录模拟盘市值
            pf = self.moni_trader.portfolio
            self._source_trader_record.append([self.moni_trader.current_dt, pf.positions_value + pf.available_cash])
            # 记录实盘市值
            pf = self.shipane_trader.portfolio
            self._dest_trader_record.append([self.shipane_trader.current_dt, pf.positions_value + pf.available_cash])
            self._logger.info('[实盘管理器] 实盘涨幅统计:\n' + self.get_rate_str(self._dest_trader_record))
            self._logger.info('[实盘管理器] 实盘持仓统计:\n' + self._get_trader_portfolio_text(self.shipane_trader))
        except Exception as e:
            self._logger.error('[实盘管理器] 盘后数据处理错误!' + str(e))

    def get_rate_str(self, record):
        if len(record) > 1:
            if record[-2][1] == 0:
                return '穷鬼，你没钱，还统计啥'
            rate_total = (record[-1][1] - self._cost) / self._cost
            rate_today = (record[-1][1] - record[-2][1]) / record[-2][1]
            now = datetime.datetime.now()
            record_week = [x for x in record if (now - x[0]).days <= 7]
            rate_week = (record[-1][1] - record_week[0][1]) / record_week[0][1] if len(record_week) > 0 else 0
            record_mouth = [x for x in record if (now - x[0]).days <= 30]
            rate_mouth = (record[-1][1] - record_mouth[0][1]) / record_mouth[0][1] if len(record_mouth) > 0 else 0
            return '资产涨幅:[总:%.2f%%] [今日%.2f%%] [最近一周:%.2f%%] [最近30:%.2f%%]' % (
                rate_total * 100
                , rate_today * 100
                , rate_week * 100
                , rate_mouth * 100)
        else:
            return '数据不足'
        pass

    # 获取持仓信息，HTML格式
    def _get_trader_portfolio_html(self, trader):
        pf = trader.portfolio
        total_values = pf.positions_value + pf.available_cash
        position_str = "总资产: [ %d ]<br>市值: [ %d ]<br>现金   : [ %d ]<br>" % (
            total_values,
            pf.positions_value, pf.available_cash
        )
        position_str += "<table border=\"1\"><tr><th>股票代码</th><th>持仓</th><th>当前价</th><th>盈亏</th><th>持仓比</th></tr>"
        for position in pf.positions.values():
            stock = position.security
            if position.price - position.avg_cost > 0:
                tr_color = 'red'
            else:
                tr_color = 'green'
            stock_raite = (position.total_amount * position.price) / total_values * 100
            position_str += '<tr style="color:%s"><td> %s </td><td> %d </td><td> %.2f </td><td> %.2f%% </td><td> %.2f%%</td></tr>' % (
                tr_color,
                show_stock(normalize_code(stock)),
                position.total_amount, position.price,
                (position.price - position.avg_cost) / position.avg_cost * 100,
                stock_raite
            )

        return position_str + '</table>'

    # 获取持仓信息，普通文本格式
    def _get_trader_portfolio_text(self, trader):
        pf = trader.portfolio
        total_values = pf.positions_value + pf.available_cash
        position_str = "总资产 : [ %d ] 市值: [ %d ] 现金   : [ %d ]" % (
            total_values,
            pf.positions_value, pf.available_cash
        )

        table = PrettyTable(["股票", "持仓", "当前价", "盈亏", "持仓比"])
        for stock in pf.positions.keys():
            position = pf.positions[stock]
            if position.total_amount == 0:
                continue
            stock_str = show_stock(normalize_code(stock))
            stock_raite = (position.total_amount * position.price) / total_values * 100
            table.add_row([
                stock_str,
                position.total_amount,
                position.price,
                "%.2f%%" % ((position.price - position.avg_cost) / position.avg_cost * 100),
                "%.2f%%" % (stock_raite)]
            )
        return position_str + '\n' + str(table)

    def __str__(self):
        return '实盘管理类:[同步持仓] [实盘邮件] [实盘报表]'
        
# 基于实盘易官方同步SDK实现的同步
class Shipane_Sync(Rule):
    # 覆盖level方法，强制返回为Finally级别
    @property
    def level(self):
        return Rule_Level.Finally

    def initialize(self, context):
        self._manager = shipane_sdk.StrategyManager(context, self._params.get('manager', 'manager-1'))

    def process_initialize(self,context):
        self._manager = shipane_sdk.StrategyManager(context, self._params.get('manager','manager-1'))

    def handle_data(self, context, data):
        self._manager.sync(context)


# '''------------------------------通过实盘易申购新股----------------------'''
class Purchase_new_stocks(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        self.times = params.get('times', [[10, 00]])
        self.host = params.get('host', '')
        self.port = params.get('port', 8888)
        self.key = params.get('key', '')
        self.clients = params.get('clients', [])

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.times = params.get('times', [[10, 00]])
        self.host = params.get('host', '')
        self.port = params.get('port', 8888)
        self.key = params.get('key', '')
        self.clients = params.get('clients', [])

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        if not [hour, minute] in self.times:
            return
        try:
            import shipane_sdk
        except:
            pass
        shipane = shipane_sdk.Client(g.log_type(self.memo), key=self.key, host=self.host, port=self.port,
                                     show_info=False)
        for client_param in self.clients:
            shipane.purchase_new_stocks(client_param)

    def __str__(self):
        return '实盘易申购新股[time: %s host: %s:%d  key: %s client:%s] ' % (
            self.times, self.host, self.port, self.key, self.clients)
            
# '''------------------邮件通知器-----------------'''
class Email_notice(Op_stocks_record):
    def __init__(self, params):
        Op_stocks_record.__init__(self, params)
        self.user = params.get('user', '')
        self.password = params.get('password', '')
        self.tos = params.get('tos', '')
        self.sender_name = params.get('sender', '发送者')
        self.strategy_name = params.get('strategy_name', '策略1')
        self.str_old_portfolio = ''

    def update_params(self, context, params):
        Op_stocks_record.update_params(self, context, params)
        self.user = params.get('user', '')
        self.password = params.get('password', '')
        self.tos = params.get('tos', '')
        self.sender_name = params.get('sender', '发送者')
        self.strategy_name = params.get('strategy_name', '策略1')
        self.str_old_portfolio = ''
        try:
            Op_stocks_record.update_params(self, context, params)
        except:
            pass

    def before_adjust_start(self, context, data):
        Op_stocks_record.before_trading_start(self, context)
        self.str_old_portfolio = self.__get_portfolio_info_html(context)
        pass

    def after_adjust_end(self, context, data):
        Op_stocks_record.after_adjust_end(self, context, data)
        try:
            send_time = self._params.get('send_time', [])
        except:
            send_time = []
        if self._params.get('send_with_change', True) and not self.position_has_change:
            return
        if len(send_time) == 0 or [context.current_dt.hour, context.current_dt.minute] in send_time:
            self.__send_email('%s:调仓结果' % (self.strategy_name)
                              , self.__get_mail_text_before_adjust(context
                                                                   , ''
                                                                   , self.str_old_portfolio
                                                                   , self.op_sell_stocks
                                                                   , self.op_buy_stocks))
            self.position_has_change = False  # 发送完邮件，重置标记

    def after_trading_end(self, context):
        Op_stocks_record.after_trading_end(self, context)
        self.str_old_portfolio = ''

    def on_clear_position(self, context, new_pindexs=[0]):
        # 清仓通知
        self.op_buy_stocks = self.merge_op_list(self.op_buy_stocks)
        self.op_sell_stocks = self.merge_op_list(self.op_sell_stocks)
        if len(self.op_buy_stocks) > 0 or len(self.op_sell_stocks) > 0:
            self.__send_email('%s:清仓' % (self.strategy_name), '已触发清仓')
            self.op_buy_stocks = []
            self.op_sell_stocks = []
        pass

    # 发送邮件 subject 为邮件主题,content为邮件正文(当前默认为文本邮件)
    def __send_email(self, subject, text):
        # # 发送邮件
        username = self.user  # 你的邮箱账号
        password = self.password  # 你的邮箱授权码。一个16位字符串

        sender = '%s<%s>' % (self.sender_name, self.user)

        msg = MIMEText("<pre>" + text + "</pre>", 'html', 'utf-8')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['to'] = ';'.join(self.tos)
        msg['from'] = sender  # 自己的邮件地址

        server = smtplib.SMTP_SSL('smtp.qq.com')
        try:
            # server.connect() # ssl无需这条
            server.login(username, password)  # 登陆
            server.sendmail(sender, self.tos, msg.as_string())  # 发送
            self.log.info('邮件发送成功:' + subject)
        except:
            self.log.info('邮件发送失败:' + subject)
        server.quit()  # 结束

    def __get_mail_text_before_adjust(self, context, op_info, str_old_portfolio,
                                      to_sell_stocks, to_buy_stocks):
        # 获取又买又卖的股票，实质为调仓
        mailtext = context.current_dt.strftime("%Y-%m-%d %H:%M:%S")
        if len(self.g.buy_stocks) >= 5:
            mailtext += '<br>选股前5:<br>' + ''.join(['%s<br>' % (show_stock(x)) for x in self.g.buy_stocks[:5]])
            mailtext += '--------------------------------<br>'
        # mailtext += '<br><font color="blue">'+op_info+'</font><br>'
        if len(to_sell_stocks) + len(to_buy_stocks) == 0:
            mailtext += '<br><font size="5" color="red">* 无需调仓! *</font><br>'
            mailtext += '<br>当前持仓:<br>'
        else:
            #             mailtext += '<br>==> 调仓前持仓:<br>'+str_old_portfolio+"<br>==> 执行调仓<br>--------------------------------<br>"
            mailtext += '卖出股票:<br><font color="blue">'
            mailtext += ''.join(['%s %d<br>' % (show_stock(x[0]), x[1]) for x in to_sell_stocks])
            mailtext += '</font>--------------------------------<br>'
            mailtext += '买入股票:<br><font color="red">'
            mailtext += ''.join(['%s %d<br>' % (show_stock(x[0]), x[1]) for x in to_buy_stocks])
            mailtext += '</font>'
            mailtext += '<br>==> 调仓后持仓:<br>'
        mailtext += self.__get_portfolio_info_html(context)
        return mailtext

    def __get_portfolio_info_html(self, context):
        total_values = context.portfolio.positions_value + context.portfolio.cash
        position_str = "--------------------------------<br>"
        position_str += "总市值 : [ %d ]<br>持仓市值: [ %d ]<br>现金   : [ %d ]<br>" % (
            total_values,
            context.portfolio.positions_value, context.portfolio.cash
        )
        position_str += "<table border=\"1\"><tr><th>股票代码</th><th>持仓</th><th>当前价</th><th>盈亏</th><th>持仓比</th></tr>"
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.price - position.avg_cost > 0:
                tr_color = 'red'
            else:
                tr_color = 'green'
            stock_raite = (position.total_amount * position.price) / total_values * 100
            position_str += '<tr style="color:%s"><td> %s </td><td> %d </td><td> %.2f </td><td> %.2f%% </td><td> %.2f%%</td></tr>' % (
                tr_color,
                show_stock(stock),
                position.total_amount, position.price,
                (position.price - position.avg_cost) / position.avg_cost * 100,
                stock_raite
            )

        return position_str + '</table>'

    def __str__(self):
        return '调仓结果邮件通知:[发送人:%s] [接收人:%s]' % (self.sender_name, str(self.tos))
