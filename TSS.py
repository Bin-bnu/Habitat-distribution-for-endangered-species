import rasterio
from sklearn.metrics import confusion_matrix
import numpy as np
# Step 1: 读取真实和模拟的 LULC 图像
with rasterio.open('tif/LUCCTYPRO/history_2020.tif') as src:
    y_true = src.read(1)

with rasterio.open('tif/LUCCTYPRO/real_2020.tif') as src:
    y_pred = src.read(1)

# Step 2: 确保两个图像尺寸一致
assert y_true.shape == y_pred.shape, "图像尺寸不一致，请先进行裁剪或重采样"

# Step 3: 展平为一维数组，并过滤无效值（假设 0 是 NoData）
valid_mask = (y_true != 0) & (y_pred != 0)
y_true_valid = y_true[valid_mask]
y_pred_valid = y_pred[valid_mask]

# Step 4: 设置类别标签
labels = [1, 2, 3, 4, 5, 6]
class_names = ['农田', '森林', '草原', '城市', '荒地', '水体']

# Step 5: 为每个类别单独计算 TSS（One-vs-Rest）
tss_scores = []

for i, label in enumerate(labels):
    # 构建二分类标签（当前类为正类，其余为负类）
    y_true_bin = (y_true_valid == label).astype(int)
    y_pred_bin = (y_pred_valid == label).astype(int)

    # 构建混淆矩阵
    cm = confusion_matrix(y_true_bin, y_pred_bin)
    if cm.shape == (2, 2):
        tn, fp, fn, tp = cm.ravel()
        tpr = tp / (tp + fn)
        tnr = tn / (tn + fp)
        tss = tpr + tnr - 1
        tss_scores.append(tss)
        print(f"类别 {class_names[i]} 的 TSS = {tss:.4f}")
    else:
        print(f"类别 {class_names[i]} 的混淆矩阵异常，跳过该类别")
        tss_scores.append(np.nan)

# Step 6: 计算宏平均 TSS（Macro-average TSS）
macro_tss = np.nanmean(tss_scores)
print(f"\n宏平均 TSS（Macro-average TSS）= {macro_tss:.4f}")