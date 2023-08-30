import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta as rldelta
import calendar as cld
import numpy as np
import decimal as deci
import time

pd.set_option('display.max_columns', 500)

start = time.time()

# Extract dataframes from the input sheet
inputfilepath = 'Input data file.xlsx'

df_item = pd.read_excel(inputfilepath,
                        usecols = 'B,C',
                        skiprows = 1,
                        nrows = 4,
                        header = None)

df_spot_rate = pd.read_excel(inputfilepath,
                             usecols = 'B,C,D',
                             skiprows = 7,
                             nrows = 2,
                             header = 0)

df_ir_base = pd.read_excel(inputfilepath,
                           usecols = 'B,C,D',
                           skiprows = 13,
                           nrows = 12,
                           header = 0,
                           index_col = 'Month')

df_ir_quoted = pd.read_excel(inputfilepath,
                             usecols = 'B,E,F',
                             skiprows = 13,
                             nrows = 12,
                             header = 0,
                             index_col = 'Month')

df_month_code = pd.read_excel('Input data file.xlsx',
                              usecols = 'L, M',
                              nrows = 12,
                              index_col = 'Code')


# Extract input values

## item
target_ccy = df_item.iloc[0,1]
target_base = target_ccy[0:3]
target_quoted = target_ccy[-3:]
trade_date = df_item.iloc[1,1].date()
contract = df_item.iloc[2,1]
contract_month = df_month_code.at[contract, 'Date'].date()
trade_qty = df_item.iloc[3,1]

## spot rate from OTC
pr1 = df_spot_rate.iloc[0,0]
pr2 = df_spot_rate.iloc[1,0]
pr1_base = df_spot_rate.iloc[0,0][:3]
pr1_quoted = df_spot_rate.iloc[0,0][-3:]
pr2_base = df_spot_rate.iloc[1,0][:3]
pr2_quoted = df_spot_rate.iloc[1,0][-3:]

pr1_bid = df_spot_rate.iloc[0,1]
pr1_ask = df_spot_rate.iloc[0,2]
pr2_bid = df_spot_rate.iloc[1,1]
pr2_ask = df_spot_rate.iloc[1,2]

sr_dict = {pr1:(pr1_base, pr1_quoted, pr1_bid, pr1_ask),
           pr2:(pr2_base, pr2_quoted, pr2_bid, pr2_ask)}

## interest rates from money market
ir_dict_base = {}
ir_dict_quoted = {}
for i in df_ir_base.index:
    ir_dict_base[i] = (df_ir_base.at[i, 'Base_Bid'],
                       df_ir_base.at[i, 'Base_Ask'])
    ir_dict_quoted[i] = (df_ir_quoted.at[i, 'Quoted_Bid'],
                         df_ir_quoted.at[i, 'Quoted_Ask'])

# Tenor
value_date = pd.to_datetime(np.busday_offset(trade_date,2)).date()
'''
Settlement date set to be the 3rd wednesday of the contract month
'''
all_wed = [i[2] for i in cld.monthcalendar(contract_month.year, contract_month.month) if i[2] != 0]
third_wed = all_wed[2]
contract_settlement_date = contract_month.replace(day = third_wed)
tenor = (contract_settlement_date - value_date).days

# Spot Rate of Target currency
'''
Cross Rate = (Base/USD) / (Quote/USD)
'''

def get_target(direction):
    for i in sr_dict:
        if target_base in i:
            b1 = sr_dict[i][2] if target_base == sr_dict[i][0] else sr_dict[i][3]
            flag_1 = True if target_base == sr_dict[i][0] else False
            a1 = sr_dict[i][3] if b1 == sr_dict[i][2] else sr_dict[i][2]
        elif target_quoted in i:
            b2 = sr_dict[i][2] if target_quoted == sr_dict[i][1] else sr_dict[i][3]
            flag_2 = True if target_quoted == sr_dict[i][0] else False
            a2 = sr_dict[i][3] if b2 == sr_dict[i][2] else sr_dict[i][2]
        else:
            return 'input error 1'
            
    result = []
    for i in [(b1,b2),(a1,a2)]:
        result.append(round((i[0] if flag_1 else 1/i[0])/(i[1] if flag_2 else 1/i[1]),4))
        
    if direction == 'bid':
        # print(b1, flag_1, b2, flag_2)
        return result[0]
    elif direction == 'ask':
        # print(a1, flag_1, a2, flag_2)
        return result[1]
    else:
        return 'input error 2'

target_spot_bid = get_target('bid')
target_spot_ask = get_target('ask')

# Interest Rate
'''
- Interpolate based on actual day count from value date to contract settlement date.
- Extrapolation is not supported in this version.
'''
## quoted ir
merged_ir = pd.merge(df_ir_base, df_ir_quoted, on = 'Month', how = 'inner')
### number of days between each month to value date
ir_date = {}
for i in merged_ir.index:
    if merged_ir.loc[i].isnull().any():
        ir_date[i] = None
    else:
        ir_date[i] = (value_date + rldelta(months = i)-value_date).days
### find match or closest ir
def find_match_or_bounds():
    if tenor in ir_date.values():
        match = [k for k, v in ir_date.items() if v == tenor]
        return match
    else:
        smaller = None
        greater = None
        for i in ir_date.values():
            if i != None and i < tenor:
                smaller = i
            elif i != None and i > tenor:
                greater = i
                break
        smaller = [(k,v) for k, v in ir_date.items() if v == smaller]
        greater = [(k,v) for k, v in ir_date.items() if v == greater]
        return smaller + greater

check_ir = find_match_or_bounds()

### find actual ir
def get_ir(direction):
    if len(check_ir) == 1:
        res = merged_ir.at[check_ir[0],direction]
    elif len(check_ir) == 2:
        res = merged_ir.at[check_ir[0][0],direction] + \
              (merged_ir.at[check_ir[1][0],direction] - merged_ir.at[check_ir[0][0],direction])\
              / (check_ir[1][1] - check_ir[0][1])\
              * (tenor - check_ir[0][1])
    return res

ir_base_bid = get_ir('Base_Bid')
ir_base_ask = get_ir('Base_Ask')
ir_quoted_bid = get_ir('Quoted_Bid')
ir_quoted_ask = get_ir('Quoted_Ask')

# Swap (cost of carry)
days_in_a_cld_year = 360
swap_bid = (ir_quoted_bid - ir_base_ask)*target_spot_bid*tenor/days_in_a_cld_year
swap_ask = (ir_quoted_ask - ir_base_bid)*target_spot_ask*tenor/days_in_a_cld_year

# Futures
deci_target = pr1_bid if target_quoted in pr1 else pr2_bid
decimal = str(deci_target)[::-1].rfind('.')

target_raw_bid = target_spot_bid + swap_bid
target_raw_ask = target_spot_ask + swap_ask
neutral = round(np.mean((target_raw_bid,target_raw_ask)),decimal)

tick_spread = 5
my_quoted_bid = neutral - 0.1**decimal*(tick_spread//2)
my_quoted_ask = my_quoted_bid + 0.1**decimal*tick_spread


print(f'{target_ccy} {contract}  bid: {my_quoted_bid:.{decimal}f} ask: {my_quoted_ask:.{decimal}f}')

# Time used
end = time.time()
print(f'Total time taken: {end - start:.4f} seconds')









