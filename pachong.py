import random
import time
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd
import os

# 城市列表
urls = [
    # 'https://bj.lianjia.com/zufang/pg{}/#contentList',
    # 'https://sh.lianjia.com/zufang/pg{}/#contentList',
    # 'https://tj.lianjia.com/zufang/pg{}/#contentList',
    # 'https://cq.lianjia.com/zufang/pg{}/#contentList',
    'https://sy.lianjia.com/zufang/pg{}/#contentList',
    'https://cc.lianjia.com/zufang/pg{}/#contentList',
    'https://hz.lianjia.com/zufang/pg{}/#contentList',
    'https://hf.lianjia.com/zufang/pg{}/#contentList',
    # 'https://nj.lianjia.com/zufang/pg{}/#contentList',
    # 'https://jn.lianjia.com/zufang/pg{}/#contentList',
    # 'https://wh.lianjia.com/zufang/pg{}/#contentList',
    # 'https://gy.lianjia.com/zufang/pg{}/#contentList',
    'https://xa.lianjia.com/zufang/pg{}/#contentList',
    'https://lz.lianjia.com/zufang/pg{}/#contentList',
    'https://nc.lianjia.com/zufang/pg{}/#contentList',
    'https://nn.lianjia.com/zufang/pg{}/#contentList',
    # 'https://hk.lianjia.com/zufang/pg{}/#contentList',
    # 'https://km.lianjia.com/zufang/pg{}/#contentList',
    # 'https://ls.lianjia.com/zufang/pg{}/#contentList',
    # 'https://yinchuan.lianjia.com/zufang/pg{}/#contentList',
    # 'https://wlmq.lianjia.com/zufang/pg{}/#contentList',
    'https://sz.lianjia.com/zufang/pg{}/#contentList',
    'https://fz.lianjia.com/zufang/pg{}/#contentList',
    'https://cs.lianjia.com/zufang/pg{}/#contentList',
    'https://cd.lianjia.com/zufang/pg{}/#contentList',
    # 'https://hhht.lianjia.com/zufang/pg{}/#contentList'
]

# 城市与省份映射
city_province_map = {
    'sz': ('深圳', '广东'),
    'fz': ('福州', '福建'),
    'cs': ('长沙', '湖南'),
    'cd': ('成都', '四川'),
    'bj': ('北京', '北京'),
    'sh': ('上海', '上海'),
    'tj': ('天津', '天津'),
    'cq': ('重庆', '重庆'),
    'sy': ('沈阳', '辽宁'),
    'cc': ('长春', '吉林'),
    'hz': ('杭州', '浙江'),
    'hf': ('合肥', '安徽'),
    'nj': ('南京', '江苏'),
    'jn': ('济南', '山东'),
    'wh': ('武汉', '湖北'),
    'gy': ('贵阳', '贵州'),
    'xa': ('西安', '陕西'),
    'lz': ('兰州', '甘肃'),
    'nc': ('南昌', '江西'),
    'nn': ('南宁', '广西'),
    'hk': ('海口', '海南'),
    'km': ('昆明', '云南'),
    'ls': ('拉萨', '西藏'),
    'yinchuan': ('银川', '宁夏'),
    'wlmq': ('乌鲁木齐', '新疆'),
    'hhht': ('呼和浩特', '内蒙古')
}

# 请求头设置
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sy.lianjia.com/"
}

# 数据存储列表
data_list = []

# 输出文件路径
output_dir = 'D:\\VSProject\\pythonproject\\bigdata2'
os.makedirs(output_dir, exist_ok=True)  # 确保目录存在
output_file = os.path.join(output_dir, 'zufanglist4.xlsx')

# 抓取函数
def fetch_data(url, num_pages=1):
    for num in range(1, num_pages + 1):
        time.sleep(random.randint(10, 15))  # 防止请求过于频繁
        new_url = url.format(num)
        city_abbr = url.split('/')[2].split('.')[0]  # 提取城市缩写
        city, province = city_province_map.get(city_abbr, ('未知城市', '未知省份'))  # 根据缩写获取城市和省份
        print(f"Fetching page {num} of {city}...")
        try:
            ua = UserAgent()
            headers['User-Agent'] = ua.random
            res = requests.get(new_url, headers=headers)
            res.raise_for_status()  # 检查请求是否成功
            bs = BeautifulSoup(res.text, 'lxml')
            li_elements = bs.find_all("div", class_="content__list--item")
            for li_element in li_elements:
                item_locations = li_element.find('p', class_='content__list--item--des')
                item_location = ''
                if item_locations is not None:
                    item_locations = item_locations.find_all('a')
                    item_location = " - ".join([str(location.text.strip()) for location in item_locations])
                item_houses = li_element.find('p', class_='content__list--item--des')
                item_house = ",".join([str(house.text.strip()) for house in item_houses]).replace('\n', '').replace(' ', '').replace('-', '').replace(',/', '')
                item_tags = li_element.find('p', class_='content__list--item--bottom oneline').find_all('i')
                item_tag = ",".join([str(tag.text.strip()) for tag in item_tags])
                item_brand = li_element.find('span', class_='brand')
                if item_brand is not None:
                    item_brand = item_brand.text.strip()
                item_date = li_element.find('span', class_='content__list--item--time oneline')
                if item_date is not None:
                    item_date = item_date.text.strip()
                item_price_content = li_element.find('span', class_='content__list--item-price').text.strip()
                item_price = item_price_content.split(' ')[0]
                item_unit = item_price_content.split(' ')[1]
                data_list.append([province, city,  item_location, item_house, item_brand, item_date, item_price, item_unit])
        except requests.RequestException as e:
            print(f"Request failed: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

# 执行抓取
for url in urls:
    top = random.randint(5, 6)  # 随机生成 10 到 20 之间的数字
    fetch_data(url, top)
    time.sleep(random.randint(1, 2))

# 将数据保存到Excel文件中
df = pd.DataFrame(data_list, columns=['省份', '城市', '位置', '房屋信息', '品牌', '日期', '价格', '单位'])
df.to_excel(output_file, index=False, engine='openpyxl')

print(f"Data has been successfully saved to {output_file}.")