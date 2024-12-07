import geopandas as gpd
import rasterio
import numpy as np
import gc
import os
from scipy.sparse import csr_matrix
from rasterio.mask import mask
from shapely.geometry import mapping
from shapely.ops import unary_union
import csv
import time
import warnings
warnings.filterwarnings("ignore")
# ssp1_26_2030.tif
scenerios = ['ssp1_26', 'ssp2_45', 'ssp3_70', 'ssp5_85']
years = ['2030', '2050', '2070', '2100']
# scenerios = ['history']
# scenerios = ['real']
# years = ['2020']
leixings = ['两栖类','哺乳类','爬行类','鸟']
# leixings = ['植物']
# leixings = ['test']


print("读取要素中")
# gdb_file = 'your_geodatabase.gdb'
layer_name = r'shp/Lastmerged.shp'
gdf = gpd.read_file(layer_name,encoding='gbk')
print("读取要素完成")
for leixing in leixings:
    for year in years:
        for scenerio in scenerios:
            # 用于存储未找到要素的 names 列表
            not_found_names = []
            # 用于存储找到要素的 names 列表
            found_names = []
            memory_not_found= []
            memory_not_found_lucc = []
            memory_not_found_low = []
            memory_not_found_high = []
            lulc_tif_path = r'tif/LUCCTYPRO/'+str(scenerio)+'_'+str(year)+'.tif'
            # lulc_tif_path = r'tif/LUCCTYPRO/ssp1_26_2030.tif'
            dem_tif_path = r'tif/DEM1000.tif'
            with open('Lastcsv/'+str(leixing)+'匹配后最新.csv', 'r', encoding='gb18030') as file1:
                csv_reader1 = csv.reader(file1)
                counts = sum(1 for row in csv_reader1)  # Count the rows
                file1.close()
            with open('Lastcsv/'+str(leixing)+'匹配后最新.csv', 'r',encoding='gb18030') as file:
                # 创建CSV读取器
                csv_reader = csv.reader(file)
                # counts = sum(1 for row in csv_reader)
                # print(csv_reader,counts)
                # 读取CSV文件的标题行
                header = next(csv_reader)
                # 确保你的文件包含NAME、LULC、LOW、HIGH列
                name_index = header.index('scientificName')
                lulc_index = header.index('LUCC')
                low_index = header.index('DEM_MIN')
                high_index = header.index('DEM_MAX')
                # 循环读取每一行
                rows_processed = 1
                lulc_src = rasterio.open(lulc_tif_path)
                dem_src = rasterio.open(dem_tif_path)
                for row in csv_reader:
                    rows_processed += 1
                    try:
                        # 获取特定列的值
                        name = row[name_index]
                        lulc = row[lulc_index]
                        low = float(row[low_index]) if row[low_index] else 0
                        high = float(row[high_index]) if row[high_index] else 5000
                        # 在这里进行你的处理逻辑，可以输出或者进行其他操作
                        print(f"NAME: {name}, LULC: {lulc}, LOW: {low}, HIGH: {high}")
                        # 核心代码处理中
                        target_feature = gdf[gdf['sci_name'] == name]
                        if target_feature.empty:
                            not_found_names.append(name)
                            # print("未查询到相关数据")
                            continue
                        else:
                            type = target_feature['types'].iloc[0]
                            target_geometry = unary_union(target_feature.geometry.values)
                            # 从 target_geometry 创建 GeoJSON 几何
                            geometry = mapping(target_geometry)
                            # 使用 Rasterio 的 mask 函数获取覆盖在几何上的土地利用数据
                            lulc_data, lulc_transform = mask(lulc_src, [geometry], crop=True,nodata=255)
                            lulc_data = lulc_data[0]  # 去掉单一元素的列表
                            # 使用 Rasterio 的 mask 函数获取覆盖在几何上的 DEM 数据
                            dem_data, dem_transform = mask(dem_src, [geometry], crop=True)
                            dem_data = dem_data[0]  # 去掉单一元素的列表
                            # Ensure lulc_data and dem_data have the same shape
                            min_rows = min(lulc_data.shape[0], dem_data.shape[0])
                            min_cols = min(lulc_data.shape[1], dem_data.shape[1])
                            lulc_data = lulc_data[:min_rows, :min_cols]
                            dem_data = dem_data[:min_rows, :min_cols]
                            lulc_values = list(map(int, lulc.split(',')))
                            lulc_values = [x for x in lulc_values]
                            # 进行筛选并给稀疏矩阵赋值
                            indices = np.where(np.isin(lulc_data, lulc_values) & (dem_data > low) & (dem_data < high))
                            rows, cols = indices
                            del indices
                            del dem_data
                            data = np.zeros_like(rows, dtype=np.uint8)
                            # result = csr_matrix((data, (rows, cols)), shape=lulc_data.shape, dtype=np.uint8)
                            result = csr_matrix((lulc_data.shape[0], lulc_data.shape[1]), dtype=np.uint8)
                            # 将符合条件的位置赋值为 1
                            result[rows, cols] = 1
                            # print(result)
                            del data
                            del lulc_data
                            if np.all(result.toarray() == 0):
                                print(f"Skipping saving for {name}: result is all zeros")
                                continue
                            # 保存结果
                            # 'ssp1_26', 'ssp2_45', 'ssp3_70', 'ssp5_85'
                            if scenerio == 'real':
                                sceneriolast  = 'real'
                            if scenerio == 'history':
                                sceneriolast  = 'baseline'
                            if scenerio == 'ssp1_26':
                                sceneriolast  = 'SSP1_RCP26'
                            if scenerio == 'ssp2_45':
                                sceneriolast  = 'SSP2_RCP45'
                            if scenerio == 'ssp3_70':
                                sceneriolast  = 'SSP3_RCP70'
                            if scenerio == 'ssp5_85':
                                sceneriolast  = 'SSP5_RCP85'
                            output_result_filemake = f'lastresults/'+str(sceneriolast) +'/' +str(year) +'/' + str(type)
                            os.makedirs(output_result_filemake, exist_ok=True)
                            output_result_file = f'lastresults/'+str(sceneriolast) +'/' +str(year) +'/' + str(type) + f'/{name}.tif'
                            transform = rasterio.transform.from_origin(target_geometry.bounds[0], target_geometry.bounds[3],
                                                                       dem_transform[0], -dem_transform[4])
                            with rasterio.open(output_result_file, 'w', driver='GTiff', height=result.shape[0],
                                               width=result.shape[1],
                                               count=1, dtype='uint8', crs=dem_src.crs, transform=transform) as dst:
                                dst.write(result.toarray(), 1)
                            del result
                            gc.collect()

                        # 更新计数器
                        # rows_processed += 1
                        # 计算百分比
                        percentage = (rows_processed / counts) * 100
                        # 打印当前百分比
                        print(f"Processed {rows_processed} rows out of {counts} ({percentage:.2f}%)")
                        found_names.append(name)
                    except Exception as e:
                        print(e)
                        memory_not_found.append(name)
                        memory_not_found_lucc.append(lulc)
                        memory_not_found_low.append(low)
                        memory_not_found_high.append(high)

            # import pandas as pd
            # # 将未找到要素的 names 列表保存为 CSV 文件
            # not_found_df = pd.DataFrame({'NAME': not_found_names})
            # not_found_df.to_csv('habitat/outputcsv/'+ str(scenerio) +'/' + str(year) + "/" + str(type) +'not_found_names.csv', index=False)
            # # 将找到要素的 names 列表保存为 CSV 文件
            # found_df = pd.DataFrame({'NAME': found_names})
            # found_df.to_csv('habitat/outputcsv/'+ str(scenerio) +'/' + str(year) + "/" + str(type) +'found_names.csv', index=False)
            # memory_df =  pd.DataFrame({'NAME': memory_not_found,'LULC':memory_not_found_lucc,'LOW':memory_not_found_low,'HIGH':memory_not_found_high})
            # memory_df.to_csv('habitat/outputcsv/'+ str(scenerio) +'/' + str(year) + "/" + str(type) +'memory_not.csv', index=False)
