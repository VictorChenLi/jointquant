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
import time
import enum
from jqdata import gta
from jqlib.technical_analysis import *

def pick_strategy(buy_count):
    g.strategy_memo = '首席质量因子'

    pick_config = [
        [True, '', '多因子范围选取器', Pick_financial_data, {
            'factors': [
                # FD_Factor('valuation.circulating_market_cap', min=0, max=100)  # 流通市值0~100亿
                FD_Factor('valuation.pe_ratio', min=0, max=200),  # 200 > pe > 0
                FD_Factor('valuation.pb_ratio', min=0),  # pb_ratio > 0
                FD_Factor('valuation.ps_ratio', max=2.5) # ps_ratio < 2.5
            ]
        }],
        [True, '', '多因子过滤器', Filter_financial_data, {
            'filters': [
                # FD_Filter('valuation.market_cap',sort=SortType.desc, percent=80),
                FD_Filter('valuation.pe_ratio',sort=SortType.asc, percent=40),
                FD_Filter('valuation.pb_ratio',sort=SortType.asc, percent=40),
            ]
        }],
        [True, '', '过滤创业板', Filter_gem, {}],
        [True, '', '过滤ST,停牌,涨跌停股票', Filter_common, {}],
        [True, '', '权重排序', SortRules, {
            'config': [
                [True, '', '流通市值排序', Sort_financial_data, {
                    'factor': 'valuation.circulating_market_cap',
                    'sort': SortType.asc
                    , 'weight': 50}],
                [True, '', '首席质量因子排序', Sort_gross_profit, {
                    'sort': SortType.desc,
                    'weight': 100}],
                [True, '20volumn', '20日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 10
                    , 'day': 20}],
                [True, '60volumn', '60日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 10
                    , 'day': 60}],
                [True, '120volumn', '120日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 10
                    , 'day': 120}],
                [True, '180volumn', '180日成交量排序', Sort_volumn, {
                    'sort': SortType.desc
                    , 'weight': 10
                    , 'day': 180}],
            ]}
        ],
        # [True, '', '低估价值选股', Underestimate_value_pick, {}],
        # [True, '', '布林线选股', Bolling_pick, {}],
        # [True, '', '股息率选股', Dividend_yield_pick, {}],
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

#白名单，股票均在这个list里面选取
def white_list():
    return get_index_stocks("399951.XSHE") + get_index_stocks("000300.XSHG") + get_index_stocks("000001.XSHG")
    # return get_index_stocks("000300.XSHG")


# ==================================策略配置==============================================
def select_strategy(context):
    buy_count = 5
    adjust_days = 7
    # **** 这里定义log输出的类类型,重要，一定要写。假如有需要自定义log，可更改这个变量
    g.log_type = Rule_loger
    # 判断是运行回测还是运行模拟
    g.is_sim_trade = context.run_params.type == 'sim_trade'
    index2 = '000016.XSHG'  # 大盘指数
    index8 = '399333.XSHE'  # 小盘指数

    ''' ---------------------配置 调仓条件判断规则-----------------------'''
    # 调仓条件判断
    adjust_condition_config = [
        [True, '_time_c_', '调仓时间', Time_condition, {
            'times': [[14, 50]],  # 调仓时间列表，二维数组，可指定多个时间点
        }],
        [True, '', '止损策略', Group_rules, {
            'config':[
                [True, '_Stop_loss_by_price_', '指数最高低价比值止损器', Stop_loss_by_price, {
                    'index': '000001.XSHG',  # 使用的指数,默认 '000001.XSHG'
                    'day_count': 160,  # 可选 取day_count天内的最高价，最低价。默认160
                    'multiple': 2.2  # 可选 最高价为最低价的multiple倍时，触 发清仓
                }],
                # [True, '', '多指数20日涨幅止损器', Mul_index_stop_loss, {
                #     'indexs': [index2, index8],
                #     'min_rate': 0.005
                # }],
                [True, '', '个股止损器', Stop_loss_win_for_single, {
                    # 止损止盈后是否保留当前的持股状态
                    'keep_position': False,
                    # 动态止盈和accumulate_win不能一起使用
                    'accumulate_loss':  -0.05,
                    # 'accumulate_win': 0.2,
                    # 'dynamic_stop_win': True,
                    # 'dynamic_threshod': 0.05,
                    # 'dynamic_sense': 0.1 # 0 < sense,越接近0越灵敏,止盈出局越快
                }],
            ]
        }],
        [True, '', '调仓日计数器', Period_condition, {
            'period': adjust_days,  # 调仓频率,日
        }],
    ]
    adjust_condition_config = [
        [True, '_adjust_condition_', '调仓执行条件的判断规则组合', Group_rules, {
            'config': adjust_condition_config
        }]
    ]

    ''' --------------------------配置 选股规则----------------- '''
    pick_new = pick_strategy(buy_count)

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
    except Exception as e:
        log.error(str(e))
        pass


# 这里示例进行模拟更改回测时，如何调整策略,基本通用代码。
def after_code_changed(context):
    try:
        g.main
    except Exception as e:
        log.error(str(e))
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
        log.error('更新代码失败:' + str(e) + '\n重新创建策略')
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
        except Exception as e:
            log.error(str(e))
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


class Stop_loss_win_for_single(Rule):
    def __init__(self, params):
        self.accumulate_loss = params.get('accumulate_loss', None)
        self.accumulate_win = params.get('accumulate_win', None)
        self.dynamic_stop_win = params.get('dynamic_stop_win', False)
        self.dynamic_threshod = params.get('dynamic_threshod', 0.1)
        self.dynamic_sense = params.get('dynamic_sense', 0.2)
        self.keep_position = params.get('keep_position', False)
        pass

    def update_params(self, context, params):
        self.accumulate_loss = params.get('accumulate_loss', self.accumulate_loss)
        self.accumulate_win = params.get('accumulate_win', self.accumulate_win)
        self.dynamic_stop_win = params.get('dynamic_stop_win', self.dynamic_stop_win)
        self.dynamic_threshod = params.get('dynamic_threshod', self.dynamic_threshod)
        self.dynamic_sense = params.get('dynamic_sense', self.dynamic_sense)
        self.keep_position = params.get('keep_position', self.keep_position)

    def caculate_return(self, context, price, stock):
        cost = context.portfolio.positions[stock].avg_cost
        if cost != 0:
            return (price-cost)/cost
        else:
            return None

    # 计算股票累计收益率（从建仓至今）
    def security_accumulate_return(self, context, data, stock):
        current_price = data[stock].price
        return self.caculate_return(context, current_price, stock)

    def get_dynamic_win_stop(self, context, data, stock):
        position = context.portfolio.positions[stock];
        delta = context.current_dt - position.init_time

        # 得到过去这些时间内每分钟最高的价格
        hist1 = get_price(stock, start_date=position.init_time, end_date=context.current_dt, frequency='1m', fields=["avg"], skip_paused=True)
        high_price = hist1['avg'].max();
        high_margin_return = self.caculate_return(context, high_price, stock)

        if high_margin_return != None and self.dynamic_threshod < high_margin_return:
            dynamic_stop_margin = pow(high_margin_return, self.dynamic_sense) * high_margin_return
            return dynamic_stop_margin
        else:
            return 0

    def handle_data(self, context, data):
        for stock in context.portfolio.positions.keys():
            accumulate_return = self.security_accumulate_return(context,data,stock);
            # 动态止盈
            if self.dynamic_stop_win:
                dynamic_stop_margin = self.get_dynamic_win_stop(context, data, stock)
                if accumulate_return > 0 and accumulate_return < dynamic_stop_margin:
                    position = context.portfolio.long_positions[stock]
                    # 平仓，并且 is_normal=False, 需要重新调仓
                    self.log.warn('{0} 该股累计涨幅超过动态止盈点{1}%, 目前为{2}%，执行平仓，并且重新开始调仓'.format(show_stock(position.security), dynamic_stop_margin*100, accumulate_return*100))
                    self.g.close_position(self, position, self.keep_position)

            # 静态止损止盈
            if accumulate_return != None \
            and ( (self.accumulate_loss !=None and accumulate_return < self.accumulate_loss) \
            or (self.dynamic_stop_win == False and self.accumulate_win !=None and accumulate_return > self.accumulate_win) ):
                    position = context.portfolio.long_positions[stock]
                    # 平仓，并且 is_normal=False, 需要重新调仓
                    self.log.warn('{0} 该股累计{1}超过{2}%，执行平仓，并且重新开始调仓'.format(show_stock(position.security), "涨幅" if accumulate_return > 0 else "跌幅", (self.accumulate_win if accumulate_return > 0 else self.accumulate_loss)*100))
                    self.g.close_position(self, position, self.keep_position)

    def __str__(self):
        s =  '个股止损器:'
        if self.dynamic_stop_win:
            s += '[动态止盈方案:启动阈值:{0}%, 灵敏度:{1}, 保持持仓:{2}]'.format(self.dynamic_threshod*100, self.dynamic_sense, self.keep_position)
        elif self.accumulate_win != None:
            s += '[参数: 止盈点为{0}%]'.format(self.accumulate_win * 100)
        if self.accumulate_loss != None:
            s += '[参数: 止损点为{0}%]'.format(self.accumulate_loss * 100)
        return s

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
            low_price = h.low.min()
            high_price = h.high.max()
            if high_price > self.multiple * low_price and h['close'][-1] < h['close'][-4] * 1 and h['close'][
                -1] > h['close'][-100]:
                # 当日第一次输出日志
                self.log.info("==> 大盘止损，%s指数前%s日内最高价超过最低价%s倍, 最高价: %f, 最低价: %f" % (
                    get_security_info(self.index).display_name, self.day_count, self.multiple, high_price, low_price))
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
        msg = ""
        for index in self._indexs:
            gr_index = get_growth_rate(index, self._n)
            msg += '%s %d日涨幅  %.2f%%\n' % (show_stock(index), self._n, gr_index * 100)
            r.append(gr_index > self._min_rate)
        if sum(r) == 0:
            self.log.warn(msg)
            self.log.warn('多指数%d日涨幅均小于%.2f%%, 不符合持仓条件，清仓')
            self.g.clear_position(self, context, self.g.op_pindexs)
            self.is_to_return = True

    def after_trading_end(self, context):
        Rule.after_trading_end(self, context)
        # for index in self._indexs:
        #     gr_index = get_growth_rate(index, self._n - 1)
        #     self.log.info('%s %d日涨幅  %.2f%% ' % (show_stock(index), self._n - 1, gr_index * 100))

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


# '''-----------------选股组合器-----------------------'''
class Pick_stocks(Group_rules):
    def __init__(self, params):
        Group_rules.__init__(self, params)
        self.has_run = False

    def handle_data(self, context, data):
        try:
            to_run_one = self._params.get('day_only_run_one', False)
        except Exception as e:
            log.error(str(e))
            to_run_one = False
        if to_run_one and self.has_run:
            self.log.info('设置一天只选一次，跳过选股。')
            return

        # 执行 filter query
        q = None
        for rule in self.rules:
            if isinstance(rule, Filter_query):
                q = rule.filter(context, data, q)
                print "执行了: %s" % rule
        stock_list = list(get_fundamentals(q)['code']) if q != None else white_list()
        stock_list = intersect(stock_list, white_list())

        print "选股得到%s只股票" % len(stock_list)

        # 执行 Filter_stock_list
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
            q = query(valuation,balance,cash_flow,income,indicator)
            # q = query(valuation)

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
        order_by = self._params.get('order_by', None)
        if order_by is not None:
            order_by = eval(order_by)
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
            s += '\n\t\t---'
            if fd_param.min is not None and fd_param.max is not None:
                s += '[ %s < %s < %s ]' % (fd_param.min, fd_param.factor, fd_param.max)
            elif fd_param.min is not None:
                s += '[ %s < %s ]' % (fd_param.min, fd_param.factor)
            elif fd_param.max is not None:
                s += '[ %s > %s ]' % (fd_param.factor, fd_param.max)

        order_by = self._params.get('order_by', None)
        sort_type = self._params.get('sort', SortType.asc)
        if order_by is not None:
            s += '\n\t\t---'
            sort_type = '从小到大' if sort_type == SortType.asc else '从大到小'
            s += '[排序:%s %s]' % (order_by, sort_type)
        limit = self._params.get('limit', None)
        if limit is not None:
            s += '\n\t\t---'
            s += '[限制选股数:%s]' % (limit)
        return '多因子选股:' + s


# 根据财务数据对Stock_list进行过滤。返回符合条件的stock_list
class Filter_financial_data(Filter_stock_list):
    def filter(self, context, data, stock_list):
        for fd_param in self._params.get('filters', []):
            q = query(valuation,balance,cash_flow,income,indicator).filter(
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
        
        print show_rule_execute_result(self, stock_list)

        return stock_list

    def __str__(self):
        s = ''
        for fd_param in self._params.get('filters', []):
            factor = fd_param.factor
            sort_type = fd_param.sort
            s += '\n\t\t---'
            sort_type = '从小到大' if sort_type == SortType.asc else '从大到小'
            s += '[排序:%s %s]' % (factor, sort_type)
            percent = fd_param.percent
            if percent is not None:
                s += '\n\t\t---'
                s += '[选择前百分之:%s]' % (percent)

        return '多因子过滤:' + s


# 股息率选股
class Dividend_yield_pick(Filter_stock_list):
    def __init__(self, params):
        pass

    def filter(self, context, data, stock_list):
        year = context.current_dt.year-1
        
        #将当前股票池转换为国泰安的6位股票池
        stocks_symbol=[]
        for s in stock_list:
            stocks_symbol.append(s[0:6])

            
        #如果知道前一年的分红，那么得到前一年的分红数据
        df1 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,#股票代码
                gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
                gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
            ).filter(
                gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
                gta.STK_DIVIDEND.DIVDENDYEAR == year,
               #且分红信息在上一年度
                gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol)
            )).dropna(axis=0)
        
        stocks_symbol_this_year=list(df1['SYMBOL'])
        
        #如果前一年的分红不知道，那么知道前两年的分红数据
        df2 = gta.run_query(query(
            gta.STK_DIVIDEND.SYMBOL,#股票代码
            gta.STK_DIVIDEND.DIVIDENTBT,#股票分红
            gta.STK_DIVIDEND.DECLAREDATE#分红消息的时间
        ).filter(
            gta.STK_DIVIDEND.ISDIVIDEND == 'Y',#有分红的股票
            gta.STK_DIVIDEND.DIVDENDYEAR == year-1,
           #且分红信息在上一年度
            gta.STK_DIVIDEND.SYMBOL.in_(stocks_symbol),
            gta.STK_DIVIDEND.SYMBOL.notin_(stocks_symbol_this_year)
        )).dropna(axis=0)
        
        df= pd.concat((df2,df1))
        # 下面四行代码用于选择在当前时间内能已知去年股息信息的股票
        df['pubtime'] = map(lambda x: int(x.split('-')[0]+x.split('-')[1]+x.split('-')[2]),df['DECLAREDATE'])
        currenttime  = int(str(context.current_dt)[0:4]+str(context.current_dt)[5:7]+str(context.current_dt)[8:10])
        
        # 筛选出pubtime小于当前时期的股票，然后剔除'DECLAREDATE','pubtime','SYMBOL'三列
        # 并且将DIVIDENTBT 列转换为float
        df = df[(df.pubtime < currenttime)]
        
        df['SYMBOL']=map(normalize_code,list(df['SYMBOL']))
        df.index=list(df['SYMBOL'])
        
        df=df.drop(['SYMBOL','pubtime','DECLAREDATE'],axis=1)

        df['DIVIDENTBT'] = map(float, df['DIVIDENTBT'])
        
        #接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
        #按照股票代码分堆
        df = df.groupby(df.index).sum()
        
        #得到当前股价
        Price=history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False, fq='pre')
        Price=Price.T
        df['pre_close']=Price
        
        #计算股息率 = 股息/股票价格
        df['divpercent']=df['DIVIDENTBT']/df['pre_close']
        # 从大到小排序股息率
        df=df.sort(columns=['divpercent'], axis=0, ascending=False)
        Buylist =list(df.index)

        print show_rule_execute_result(self, Buylist)

        return Buylist

    def __str__(self):
        return '按股息率从大到小选股'

# 迈克尔•普莱斯低估价值选股策略
class Underestimate_value_pick(Filter_stock_list):
    def __init__(self, params):
        pass

    def filter(self, context, data, stock_list):
        stocks = get_fundamentals(query(
            valuation.code,
            valuation.pb_ratio,
            balance.total_assets,
            balance.total_liability,
            balance.total_current_assets,
            balance.total_current_liability
        ).filter(
            valuation.code.in_(stock_list),
            valuation.pb_ratio < 2,
            valuation.pb_ratio > 0,
            balance.total_current_assets/balance.total_current_liability > 1.2
        ))

        stocks['Debt_Asset'] = stocks['total_liability']/stocks['total_assets']
        median = stocks['Debt_Asset'].median()
        picked_list = stocks[stocks['Debt_Asset'] > median].code
        picked_list = list(picked_list)

        print show_rule_execute_result(self, picked_list);

        return picked_list

    def __str__(self):
        return '低估价值选股策略:\n\t\t1.股价与每股净值比小于2\n\t\t2.负债比例高于市场平均值\n\t\t3.企业的流动资产至少是流动负债的1.2倍'


# 根据bolling策略来选择买入股票
class Bolling_pick(Filter_stock_list):
    def __init__(self, params):
        self.lag = params.get('lag', 5)
        self.lim = params.get('lim', 0.12)

    def filter(self, context, data, stock_list):
        # 首先得到布林带宽在lim以内的股票
        buy_list = self.get_buy_list(context, stock_list)
        if buy_list != []:
            # 得到今日股票前lag日的布林线数据
            up_line,mid_line,dn_line,width = self.get_bollinger(context,buy_list,self.lag)
            # 选取昨日放量上涨且收盘价位于布林中线以上且布林带放大的股票，
            # 依据昨日量价涨幅的综合评分对这些股票进行排序
            buy_list = self.grade_filter(buy_list,self.lag,up_line,width,context)

        print show_rule_execute_result(self, buy_list)
        return buy_list

    def __str__(self):
        return '布林线选股: 考虑%s天的布林轨, 布林带带宽的极限:%s' % (self.lag, self.lim)

    def get_buy_list(self, context, stock_list):
        # 先选出当日未停牌的股票
        # 得到当日是否停牌的dataframe，停牌为1，未停牌为0
        suspend_info = get_price(stock_list,start_date=context.current_dt,end_date=context.current_dt,frequency='daily',fields='paused')['paused'].T
        # 过滤掉停牌股票
        unsuspend_index = suspend_info.iloc[:,0]<1
        unsuspend_stock_ = list(suspend_info[unsuspend_index].index)
        # 进一步筛选出最近lag+1日未曾停牌的股票list
        unsuspend_stock = []
        for stock in unsuspend_stock_:
            if sum(attribute_history(stock,self.lag+1,'1d',('paused'),skip_paused=False))[0]==0:
                unsuspend_stock.append(stock)
        # 如果没有符合要求的股票则返回空
        if unsuspend_stock == []:
            log.info('没有过去十日没停牌的股票')
            return unsuspend_stock
        # 筛选出昨日前lag日布林带宽度在lim以内的股票
        up,mid,dn,wd = self.get_bollinger(context,unsuspend_stock,self.lag)
        narrow_index = wd.iloc[:,-2]<self.lim
        for day in range(2,self.lag):
            narrow_index = narrow_index&(wd.iloc[:,-day]<self.lim)
        narrow_stock = [unsuspend_stock[i] for i in [ind for ind,bool_value in enumerate(narrow_index) if bool_value==True]]
        if len(narrow_stock) != 0:
            log.info('今日潜在满足要求的bolling标的有：'+str(len(narrow_stock)))
        return narrow_stock
        
    def get_bollinger(self, context, buy, lag):
        # 创建以股票代码为index的dataframe对象来存储布林带信息
        dic = dict.fromkeys(buy,[0]*(lag+1)) # 创建一个以股票代码为keys的字典
        up = pd.DataFrame.from_dict(dic).T # 用字典构造dataframe
        mid = pd.DataFrame.from_dict(dic).T
        dn = pd.DataFrame.from_dict(dic).T
        wd = pd.DataFrame.from_dict(dic).T
        for stock in buy:
            for j in range(lag+1):
                up_,mid_,dn_ = Bollinger_Bands(stock,check_date=context.previous_date-datetime.timedelta(days=j),timeperiod=20,nbdevup=2,nbdevdn=2)
                up.loc[stock,j] = up_[stock]
                mid.loc[stock,j] = mid_[stock]
                dn.loc[stock,j] = dn_[stock]
                wd.loc[stock,j] = (up[j][stock] - dn[j][stock])/mid[j][stock]
        return up,mid,dn,wd
        
    def grade_filter(self, buy, lag, up_line, wd, context):
        # 选出连续开口的股票
        open_index = wd.iloc[:,-1]<wd.iloc[:,-2]
        for day in range(1,lag-2):
            open_index = open_index&(wd.iloc[:,-day]<wd.iloc[:,-day-1])
        buy = [buy[i] for i in [ind for ind,bool_value in enumerate(open_index) if bool_value==True]]
        up_line = up_line[open_index]
        wd = wd[open_index]
        # 如果有连续开口的股票，则在连续开口的股票中进行下一步筛选
        if len(buy)>0:
            close_buy = history(lag+1,'1d','close',buy).T
            open_buy = history(lag+1,'1d','open',buy).T
            volume_buy = history(lag+1,'1d','volume',buy).T
            # 选取昨日放量上涨的股票，且收盘价位于中线上方，在上线的下方
            stock_rise_index = (close_buy.iloc[:,-1]>open_buy.iloc[:,-1])&(close_buy.iloc[:,-1]>close_buy.iloc[:,-2])&(volume_buy.iloc[:,-1]>volume_buy.iloc[:,-2])&(close_buy.iloc[:,-1]>up_line.iloc[:,-1])
            close_buy = close_buy[stock_rise_index]
            open_buy = open_buy[stock_rise_index]
            volume_buy = volume_buy[stock_rise_index]
            buy = list(close_buy.index)
            if len(buy)>0:
                # 用一个二维数组来存放股票的涨幅和量的涨幅
                portions = [([0]*2) for i in range(len(close_buy))]
                for i in range(len(close_buy)):
                    portions[i][0] = (close_buy.iloc[i,0]-open_buy.iloc[i,0])/open_buy.iloc[i,0]
                    portions[i][1] = (volume_buy.iloc[i,0]-volume_buy.iloc[i,1])/volume_buy.iloc[i,1]
                portions = self.get_rank(portions)  # 将涨幅指标替换为排名指标
                grade = np.dot(portions,[[1.2],[0.5]])
                grade, buy = self.grade_rank(grade,buy)  # 对grade进行冒泡排序
                return buy
            else:
                return []
        else:
            return []
        
    def get_rank(self, por):
        # 定义一个数组记录一开始的位置
        indexes = range(len(por))
        # 对每一列进行冒泡排序
        for col in range(len(por[0])):
            for row in range(len(por)):
                for nrow in range(row):
                    if por[nrow][col]<por[row][col]:
                        indexes[nrow],indexes[row] = indexes[row],indexes[nrow]
                        for ecol in range(len(por[0])):
                            por[nrow][ecol],por[row][ecol] = por[row][ecol],por[nrow][ecol]
            for row in range(len(por)):
                por[row][col] = row
        # 再对indexes进行一次冒泡排序，使por恢复原顺序，每一行与buy中的股票代码相对应
        for row in range(len(por)):
            for nrow in range(row):
                if indexes[nrow]<indexes[row]:
                    indexes[nrow],indexes[row] = indexes[row],indexes[nrow]
                    for col in range(len(por[0])):
                        por[nrow][col],por[row][col] = por[row][col],por[nrow][col]
        return por
                        
    def grade_rank(self, grades, buys):
        for row in range(len(grades)):
            for nrow in range(row):
                if grades[nrow]>grades[row]:
                    grades[nrow],grades[row] = grades[row],grades[nrow]
                    buys[nrow],buys[row] = buys[row],buys[nrow]
        return grades,buys

        
# '''------------------创业板过滤器-----------------'''
class Filter_gem(Filter_stock_list):
    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:3] != '300']

    def __str__(self):
        return '过滤创业板股票'


class Filter_common(Filter_stock_list):
    def __init__(self, params):
        self.filters = params.get('filters', ['st', 'high_limit', 'low_limit', 'pause', 'new'])

    # 过滤新股，返回上市超过n天的股票
    def del_new_stock(self, context, security_list, n):
        current_data = get_current_data()
        security_list= [stock for stock in security_list if (context.current_dt.date() - get_security_info(stock).start_date).days>n]
        # 返回结果
        return security_list

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

        if 'new' in self.filters:
            stock_list = self.del_new_stock(context, stock_list, 365)
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


# --- 按成交量排序 ---
class Sort_volumn(SortBase):
    def sort(self, context, data, stock_list):
        days = self._params.get('day', 20)
        r = []
        for stock in stock_list:
            volumn = data[stock].mavg(days,'volume')
            if volumn != 0:
                r.append([stock, volumn])
        r = sorted(r, key=lambda x: x[1], reverse=not self.is_asc)
        return [stock for stock, volumn in r]

    def __str__(self):
        return '[权重: %s ] [排序: %s ] %s' % (self.weight, self._sort_type_str(), self.memo)

# '''------------------首席质量因子排序-----------------'''
#（收入(total_operating_revenue) - 成本(total_operating_cost)）/ 资产（assets)
class Sort_gross_profit(SortBase):
    def sort(self, context, data, stock_list):
        q = query(income.code, income.total_operating_revenue, income.total_operating_cost, balance.total_assets).filter(
                valuation.code.in_(stock_list)
            )
        df = get_fundamentals(q)
        df = df.fillna(value = 0)
        df = df[df.total_operating_revenue > 0]
        df = df.reset_index(drop = True)
        df = df[df.total_assets > 0]
        df = df.reset_index(drop = True)
        df['GP'] = 1.0*(df['total_operating_revenue'] - df['total_operating_cost']) / df['total_assets']

        df = df.drop(['total_assets', 'total_operating_revenue', 'total_operating_cost'], axis=1)
        
        df = df.sort(columns='GP', ascending=self.is_asc)
        #print df.head(10)
        stock_list = list(df['code'])
        
        return stock_list

    def __str__(self):
        return '首席质量因子排序器:' + '[权重: %s ] [排序: %s ] %s' % (self.weight, self._sort_type_str(), self.memo)
    

# FFScore长期价值投资打分
class FFScore_value(SortBase):
    def sort(self, context, data, stock_list):
        # 调仓
        statsDate = context.current_dt.date()
        # 取得待购列表
        stock_list = self.fun_get_stock_list(context, statsDate, stock_list)

        print show_rule_execute_result(self, stock_list)
        return stock_list

    def __str__(self):
        s =  '[权重: {0} ] [排序: {1} ] {2}'.format(self.weight, self._sort_type_str(), self.memo)
        s += '\n\t\t1. 盈利水平打分'
        s += '\n\t\t2. 财务杠杆和流动性'
        s += '\n\t\t3. 运营效率'
        return s

    def fun_get_stock_list(self, context, statsDate, stock_list):
        
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

        #2) 盈利水平打分
        # 2.1 资产收益率（ROE）：收益率为正数时ROE=1，否则为0。
        df = get_fundamentals(
            query(indicator.code, indicator.roe),
            date = statsDate - datetime.timedelta(1)
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
            date = statsDate - datetime.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(indicator.code, indicator.roa),
            date = statsDate - datetime.timedelta(365)
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
            date = statsDate - datetime.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - datetime.timedelta(365)
        )
        
        df3 = get_fundamentals(
            query(balance.code, balance.total_non_current_assets, balance.total_non_current_liability),
            date = statsDate - datetime.timedelta(730)
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
        
        # 4）运营效率
        # 4.1 流动资产周转率变化（△CATURN）： 流动资产周转率变化为当期最新可得财务报告的资产周转率同比的变化。变化为正数时△CATURN =1，否则为0。
        # 主营业务收入与流动资产的比例来反映流动资产的周转速度，来衡量企业在生产运营上对流动资产的利用效率。
        df = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - datetime.timedelta(1)
        )
        # 此算法不严谨，先简单实现，看看大体效果
        df2 = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - datetime.timedelta(365)
        )

        df3 = get_fundamentals(
            query(balance.code, balance.total_current_assets, income.total_operating_revenue, income.non_operating_revenue),
            date = statsDate - datetime.timedelta(730)
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
            date = statsDate - datetime.timedelta(1)
        )
        df2 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, balance.total_current_assets, balance.total_non_current_assets),
            date = statsDate - datetime.timedelta(365)
        )
        df3 = get_fundamentals(
            query(balance.code, income.total_operating_revenue, income.non_operating_revenue, balance.total_current_assets, balance.total_non_current_assets),
            date = statsDate - datetime.timedelta(730)
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

        # stock_list = []
        # for stock in FFScore.keys():
        #     if FFScore[stock] == 5:
        #         stock_list.append(stock)

        FFScore = sorted(FFScore.items(), key=lambda x:x[1], reverse=not self.is_asc)
        return [stock for stock, volumn in FFScore]


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
        except Exception as e:
            log.error(str(e))
            pass
        try:
            # 过滤log
            log.set_level(*(self._params.get('level', ['order', 'error'])))
        except Exception as e:
            log.error(str(e))
            pass
        try:
            # 设置基准
            set_benchmark(self._params.get('benchmark', '000300.XSHG'))
        except Exception as e:
            log.error(str(e))
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

def unique(a):
    """ return the list with duplicate elements removed """
    return list(set(a))

def intersect(a, b):
    """ return the intersection of two lists """
    return list(set(a) & set(b))

def union(a, b):
    """ return the union of two lists """
    return list(set(a) | set(b))

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


def show_rule_execute_result(rule, stock_list):
    return "执行了:%s \n得到股票列表，共%s只，显示前10只:\n%s" % (str(rule), len(stock_list), ''.join(['%s ' % (show_stock(stock)) for stock in stock_list[:10]]))

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
