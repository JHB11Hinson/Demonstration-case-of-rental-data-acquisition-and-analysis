import pandas as pd
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import xgboost as xgb
from sklearn.preprocessing import OneHotEncoder
import matplotlib.pyplot as plt
import seaborn as sns

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
plt.rcParams['axes.unicode_minus'] = False    # 解决负号显示问题

# 读取CSV文件
data = pd.read_csv('extracted_data.csv')

# 查看数据结构
print(data.head())

# 特征和目标变量
features = ['城市', '房屋面积', '楼层']
target = '月租'

# 将特征和目标变量分开
X = data[features]
y = data[target]

# 对类别型特征进行独热编码
encoder = OneHotEncoder(sparse_output=False)
X_encoded = encoder.fit_transform(X[['城市']])

# 合并独热编码后的特征和其他数值型特征
X_encoded_df = pd.DataFrame(X_encoded, columns=encoder.get_feature_names_out(['城市']))
X_final = pd.concat([X_encoded_df, X[['房屋面积', '楼层']].reset_index(drop=True)], axis=1)

# 数据集划分
X_train, X_test, y_train, y_test = train_test_split(X_final, y, test_size=0.2, random_state=42)

# 设置基础参数
base_params = {
    'objective': 'reg:squarederror',
    'tree_method': 'hist',  # 使用hist树方法
    'device': 'cuda',       # 使用CUDA设备
    'eval_metric': 'rmse'
}

# 定义随机搜索参数分布（已调整）
param_dist = {'max_depth': [3],
              'gamma': [0.9],
              'subsample': [1],
              'colsample_bytree': [0.7],
              'reg_alpha':[0.05],
              'reg_lambda':[1],
              'eta':[0.1]}

# 自定义评分函数以适应XGBRegressor
def custom_scorer(estimator, X, y):
    preds = estimator.predict(X)
    return -mean_squared_error(y, preds, squared=False)  # RMSE

# 使用RandomizedSearchCV进行超参数调优
xgb_reg = xgb.XGBRegressor(tree_method='hist', device='cuda', objective='reg:squarederror')
random_search = RandomizedSearchCV(
    estimator=xgb_reg,
    param_distributions=param_dist,
    scoring=custom_scorer,
    cv=3,
    verbose=1,
    n_jobs=-1,  # 确保只使用一个进程，以便GPU资源不会被分散
    n_iter=150  # 随机搜索的迭代次数
)
random_search.fit(X_train, y_train)  # 直接使用DataFrame

# 最佳参数
best_params = random_search.best_params_
print(f'Best parameters found: {best_params}')

# 创建DMatrix
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

# 训练模型并捕获评估结果
evals_result = {}
final_model = xgb.train(
    params={**base_params, **best_params},
    dtrain=dtrain,
    num_boost_round=200,
    evals=[(dtest, 'eval')],
    early_stopping_rounds=10,
    evals_result=evals_result
)

# 预测
preds = final_model.predict(dtest)

# 评估模型
rmse = mean_squared_error(y_test, preds, squared=False)
print(f'Final RMSE: {rmse}')

# 保存模型
final_model.save_model('xgboost_gpu_model.json')

# 特征重要性图
plt.figure(figsize=(10, 6))
ax = xgb.plot_importance(final_model, height=0.8)
ax.set_title('特征重要性')
ax.set_xlabel('F-Score (特征在树中出现的次数)')
ax.set_ylabel('特征')
plt.tight_layout()
plt.savefig('feature_importance.png')
plt.close()


# 实际值 vs 预测值散点密度图
plt.figure(figsize=(10, 6))
sns.jointplot(x=y_test, y=preds, kind='kde', fill=True, cmap='coolwarm')
plt.xlabel('实际值')
plt.ylabel('预测值')
plt.suptitle('实际值 vs 预测值 散点密度图', y=1.02)
plt.tight_layout()
plt.savefig('actual_vs_predicted_kde.png')
plt.close()

# R^2 Score
r2 = r2_score(y_test, preds)
print(f'R^2 Score: {r2}')






