import pandas as pd
import re

# 读取Excel文件
df = pd.read_excel(r'zufanglist4.xlsx')

# 定义正则表达式模式来提取面积信息
area_pattern = re.compile(r'(\d+(\.\d+)?)㎡')

# 定义正则表达式模式来提取楼层信息
floor_number_pattern = re.compile(r'(\d+)层')
floor_type_pattern = re.compile(r'([低中高])楼层')

# 定义正则表达式模式来提取价格信息
price_pattern = re.compile(r'(\d+)-(\d+)')

# 提取面积信息
def extract_area(house_info):
    match = re.search(area_pattern, house_info)
    if match:
        area = float(match.group(1))  # 转换为浮点数
        return area
    else:
        return None

df['面积'] = df['房屋信息'].apply(extract_area)

# 提取楼层信息
def extract_floor_info(house_info):
    floor_match = re.search(floor_number_pattern, house_info)
    type_match = re.search(floor_type_pattern, house_info)
    
    if floor_match:
        floor_number = int(floor_match.group(1))  # 转换为整数
    else:
        floor_number = None
    
    if type_match:
        floor_type = type_match.group(1)
        if floor_type == '高':
            floor_type_value = 3
        elif floor_type == '中':
            floor_type_value = 2
        elif floor_type == '低':
            floor_type_value = 1
        else:
            floor_type_value = None
    else:
        floor_type_value = None
    
    return floor_number, floor_type_value

df[['楼层', '楼层类型']] = df['房屋信息'].apply(extract_floor_info).apply(pd.Series)

# 提取价格信息并计算平均值
def extract_price(price_info):
    match = re.search(price_pattern, price_info)
    if match:
        low_price = int(match.group(1))
        high_price = int(match.group(2))
        return (low_price + high_price) / 2
    else:
        try:
            return float(price_info)
        except ValueError:
            return None

df['价格'] = df['价格'].apply(extract_price)

# 处理缺失值：如果面积或楼层信息为空，则填充为默认值
df['面积'] = df['面积'].fillna(0.0)
df['楼层类型'] = df['楼层类型'].fillna(2).astype(int)
df['价格'] = df['价格'].fillna(0.0)

# 提取所需的列
columns_to_extract = ['省份', '城市',  '面积', '楼层类型', '价格']
df_extracted = df[columns_to_extract]

# 显示结果
print(df_extracted)

# 保存到新的Excel文件
save_path = r'extracted_data.xlsx' #记得转换为csv
df_extracted.to_excel(save_path, index=False, engine='openpyxl')

print(f"数据已保存到 {save_path}")