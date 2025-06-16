import rasterio
import numpy as np
from scipy.stats import pearsonr
from shapely.geometry import box, mapping
from rasterio.mask import mask

# 哺乳

# # 鸟
# tif1_path = "lastresults/combine/real/2020/real_2020_BIRD.tif"
# tif2_path = "Datasat/doi_10_5061_dryad_02v6wwq48__v20221116/Richness_birds/Richness_birds/Birds_Richness_AOH_RedList1000.tif"

for tp in ['mammals', 'bird']:
    if tp == 'mammals':
        tif1_path = "lastresults/combine/real/2020/real_2020_MAMMALS.tif"
        tif2_path = "Datasat/doi_10_5061_dryad_02v6wwq48__v20221116/Richness_mammals/Richness_mammals/Mammals_Richness_AOH_RedList1000.tif"
    if tp == 'bird':
        tif1_path = "lastresults/combine/real/2020/real_2020_BIRD.tif"
        tif2_path = "Datasat/doi_10_5061_dryad_02v6wwq48__v20221116/Richness_birds/Richness_birds/Birds_Richness_AOH_RedList1000.tif"
    for zz in [1]:
        # 读取第一个tif，找到所有有效值 > 1 区域，获取边界框
        with rasterio.open(tif1_path) as src1:
            data1_full = src1.read(1)
            nodata1 = src1.nodata
            transform1 = src1.transform

            valid_mask1 = (data1_full != nodata1) & (data1_full > 1)
            rows, cols = np.where(valid_mask1)
            min_row, max_row = rows.min(), rows.max()
            min_col, max_col = cols.min(), cols.max()

            # 计算对应的地理边界框
            minx, miny = transform1 * (min_col, max_row)
            maxx, maxy = transform1 * (max_col + 1, min_row + 1)
            geom = box(minx, miny, maxx, maxy)
            geojson_geom = [mapping(geom)]

        # 裁剪第一个tif
        with rasterio.open(tif1_path) as src1:
            clipped1, _ = mask(src1, geojson_geom, crop=True)
            data1 = clipped1[0]
            nodata1 = src1.nodata

        # 裁剪第二个tif（同一区域）
        with rasterio.open(tif2_path) as src2:
            clipped2, _ = mask(src2, geojson_geom, crop=True)
            data2 = clipped2[0]
            nodata2 = src2.nodata

        # 对裁剪后数据对齐大小（取最小shape）
        min_rows = min(data1.shape[0], data2.shape[0])
        min_cols = min(data1.shape[1], data2.shape[1])
        data1 = data1[:min_rows, :min_cols]
        data2 = data2[:min_rows, :min_cols]

        # 构建两个tif同时满足条件的掩膜
        valid_mask = (data1 != nodata1) & (data2 != nodata2) & (data1 > zz) & (data2 > zz)

        # 取有效像素值
        masked_data1 = data1[valid_mask]
        masked_data2 = data2[valid_mask]

        # 计算皮尔逊相关系数
        if masked_data1.size > 1 and masked_data2.size > 1:
            corr, p_val = pearsonr(masked_data1, masked_data2)
            print(f"{tp}皮尔逊相关系数{zz}: {corr:.4f}")
            print(f"{tp}P 值{zz}: {p_val:.5e}")
        else:
            print("有效像素太少，无法计算相关性。")
