import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import OneHotEncoder

# 加载模型
model_path = '/home/gaojinpeng/01_CQUProject/12_MMSA-FET/cut/XGBoost/xgboost_gpu_model.json'
model = xgb.Booster()
model.load_model(model_path)

# 加载编码器
data = pd.read_csv('/home/gaojinpeng/01_CQUProject/12_MMSA-FET/cut/XGBoost/extracted_data.csv')
features = ['城市', '房屋面积', '楼层']
encoder = OneHotEncoder(sparse_output=False)
encoder.fit(data[['城市']])

def predict_rent(city, area, floor):
    try:
        # 独热编码城市特征
        city_encoded = encoder.transform([[city]])
        city_encoded_df = pd.DataFrame(city_encoded, columns=encoder.get_feature_names_out(['城市']))

        # 创建最终的数据框
        input_data = pd.concat([city_encoded_df, pd.DataFrame({'房屋面积': [area], '楼层': [floor]})], axis=1)

        # 预测
        dtest = xgb.DMatrix(input_data)
        prediction = model.predict(dtest)[0]

        return f"预测月租: {prediction:.2f} 元"
    except Exception as e:
        return f"请输入有效的数据: {e}"

if __name__ == "__main__":
    while True:
        city = input("请输入城市名称 (或输入 '退出' 以结束): ")
        if city.lower() == '退出':
            break
        try:
            area = float(input("请输入房屋面积 (平方米): "))
            floor = int(input("请输入楼层: "))
            result = predict_rent(city, area, floor)
            print(result)
        except ValueError:
            print("请输入有效的数字。")