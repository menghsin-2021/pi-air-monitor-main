import os
import pymongo
from bson.json_util import dumps
from pprint import pprint
from numpy import mean
from playsound import playsound
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY')

client = pymongo.MongoClient(API_KEY) # add your connection string if not local 
change_stream = client.air.air.watch()
# Change streams allow applications to access real-time data changes 
# without the complexity and risk of tailing the oplog. 
# Applications can use change streams to subscribe to all data changes on 
# a collection and immediately react to them.

labels = ['VOC-TGS', 'PM25', 'PM10']


def min2sec(m):
    return m * 60

# 要檢查的 function 為Ture ， reached 清單就會有資料
def check_wrapper(dict_data, checking_function):
    print('checking ', checking_function.__name__)
    reached = []
    # 達到規則就把 label 加到 reached
    for label in labels:
        if checking_function([d[label] for d in dict_data]):
            reached.append(label)
    print(reached)

    return reached

# 給定監測規則1 (數據突然上升)
def check_sudden_rise(data):
    avg_1min = mean(data[-min2sec(1):])
    for i in range(1, 11):  # 10 seconds
        if data[-i] < avg_1min * 1.3 and data[-i] > 3:  # increased 30%
            return False
    return True  # True 才會觸發警報

# 給定監測規則2 (數據連續上升)
def check_continue_rise(data):
    if data[-1] - data[-2] > 0 and data[-2] - data[-3] > 0 and data[-3] - data[-4] > 0:
        return True


# 印出 warning (警報標籤) 然後發出警報
def activate_warning(msg):
    print(msg)  # msg = warning = reached list
    playsound('./warning.wav', block=False)
    # you can send message here

# 將觸發警報標籤給定 warning 物件 
def construct_name(warning_name, labels):
    return warning_name + str(labels)

# 檢查發出警報的時間間隔，太短就給 False，夠長就 True
def check_timing(warnings, warning):
    if warning in warnings:
        diff = datetime.now() - warnings[warning]
        if diff < timedelta(minutes=1):
            print('filtered out warning due to time diff =', diff)
            # filtered out warning due to time diff = 0:00:27.010075 (沒有警報 因為時間間隔只有...)
            return False

    warnings[warning] = datetime.now()
    return True

# main function
warnings = {}
data = []
for change in change_stream:
    d = change['fullDocument']
    pprint(d)
    data.append(d)
    print(len(data))

    # start checking after 1 minutes so we have enough data
    if len(data) > min2sec(1):
        sudden_rise = check_wrapper(data, check_sudden_rise)
        if sudden_rise:
            warning = construct_name('sudden_rise', sudden_rise)
            print(warning) # sudden_rise['VOC-TGS', 'PM25', 'PM10']
            if check_timing(warnings, warning):
                activate_warning(warning)
            print(warnings) # {"sudden_rise['VOC-TGS', 'PM25', 'PM10']": datetime.datetime(2021, 5, 15, 17, 58, 22, 762749)}
    
    # start checking after 5 seconds so we have enough data
    if len(data) > 5:
        continue_rise = check_wrapper(data, check_continue_rise)
        if continue_rise:
            warning = construct_name('continue_rise', continue_rise)
            print(warning) # continue_rise['PM10']
            if check_timing(warnings, warning):
                activate_warning(warning)
            print(warnings) # {"continue_rise['PM10']": datetime.datetime(2021, 5, 15, 22, 34, 2, 413740)}

    # # after 5 minutes, start popping data to avoid data size too large
    if len(data) > min2sec(5):
        data.pop(0)


# 執行結果
# 41
# checking  check_continue_rise
# []
# {'PM10': 2.7,
#  'PM25': 1.9,
#  'VOC-TGS': 474,
#  '_id': ObjectId('609fdbd8f025f6b02d71ba71'),
#  'at': datetime.datetime(2021, 5, 15, 14, 34, 0, 305000)}
# 42
# checking  check_continue_rise
# []
# {'PM10': 2.8,
#  'PM25': 1.9,
#  'VOC-TGS': 474,
#  '_id': ObjectId('609fdbd9f025f6b02d71ba72'),
#  'at': datetime.datetime(2021, 5, 15, 14, 34, 1, 309000)}
# 43
# checking  check_continue_rise
# []
# {'PM10': 2.9,
#  'PM25': 2.0,
#  'VOC-TGS': 473,
#  '_id': ObjectId('609fdbdaf025f6b02d71ba73'),
#  'at': datetime.datetime(2021, 5, 15, 14, 34, 2, 313000)}
# 44
# checking  check_continue_rise
# ['PM10']
# continue_rise['PM10']
# continue_rise['PM10']
# {"continue_rise['PM10']": datetime.datetime(2021, 5, 15, 22, 34, 2, 413740)}
# {'PM10': 3.0,
#  'PM25': 2.1,
#  'VOC-TGS': 473,
#  '_id': ObjectId('609fdbdbf025f6b02d71ba74'),
#  'at': datetime.datetime(2021, 5, 15, 14, 34, 3, 316000)}