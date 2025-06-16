import geopandas as gpd
import rasterio
import numpy as np
import os
import gc
from shapely.geometry import mapping
from rasterio.mask import mask
import csv
import time
import warnings
warnings.filterwarnings("ignore")
from rasterio.features import shapes
from shapely.geometry import shape

def get_nonzero_mask_shape(lulc_src):
    data = lulc_src.read(1)
    mask = data != 0
    results = (
        {'geometry': shape(geom), 'value': value}
        for geom, value in shapes(mask.astype(np.uint8), mask=mask, transform=lulc_src.transform)
        if value == 1
    )
    geoms = [r['geometry'] for r in results]
    if not geoms:
        return gpd.GeoDataFrame({'geometry': []}, crs=lulc_src.crs)
    gdf = gpd.GeoDataFrame(geometry=geoms, crs=lulc_src.crs)
    unioned = gdf.unary_union
    return gpd.GeoDataFrame(geometry=[unioned], crs=lulc_src.crs)




def process_species(name, lulc, low, high, gdf, lulc_src, dem_src, sceneriolast, year, type_, output_dir,bufferX, lulc_mask_shape):
    try:
        target_features = gdf[gdf['sci_name'] == name]
        if target_features.empty:
            return {'status': 'not_found', 'name': name}
        results = []
        for idx, row in enumerate(target_features.itertuples()):
            geom = row.geometry
            geom = geom.intersection(lulc_mask_shape.geometry[0])
            if geom.is_empty:
                continue
            # 判断是否为MultiPolygon且子面数量是否大于10000
            if geom.geom_type == 'MultiPolygon':
                num_subpolygons = len(geom.geoms)
                # print(num_subpolygons)
                if num_subpolygons > 3:
                    polygons_to_process = geom.geoms
                else:
                    polygons_to_process = [geom]
            else:
                polygons_to_process = [geom]
            for sub_idx, subgeom in enumerate(polygons_to_process):
                area = subgeom.area
                buffer_distance = np.sqrt(area) * bufferX
                buffered = subgeom.buffer(buffer_distance)
                if buffered.is_empty:
                    continue
                # geometry = mapping(geom)
                geometry = mapping(buffered)
                # 裁剪栅格
                try:
                    lulc_data, lulc_transform = mask(lulc_src, [geometry], crop=True, nodata=255)
                    dem_data, dem_transform = mask(dem_src, [geometry], crop=True)
                except Exception as e:
                    print(f"Rasterio mask error for {name} part {idx} subpart {sub_idx}: {e}")
                    continue
                lulc_data = lulc_data[0]
                dem_data = dem_data[0]

                # 对齐尺寸
                min_rows = min(lulc_data.shape[0], dem_data.shape[0])
                min_cols = min(lulc_data.shape[1], dem_data.shape[1])
                lulc_data = lulc_data[:min_rows, :min_cols]
                dem_data = dem_data[:min_rows, :min_cols]

                lulc_values = list(map(int, lulc.split(',')))
                mask_index = np.isin(lulc_data, lulc_values) & (dem_data > low) & (dem_data < high)
                result_array = mask_index.astype(np.uint8)

                if not np.any(result_array):
                    # print(f"{name} part {idx} subpart {sub_idx} 无符合条件的像素，跳过")
                    continue

                transform = rasterio.transform.from_origin(
                    buffered.bounds[0], buffered.bounds[3],
                    dem_transform[0], -dem_transform[4]
                )

                # 生成文件名，拆分多面时加上 subpart 索引
                if len(target_features) == 1 and len(polygons_to_process) == 1:
                    output_result_file = os.path.join(output_dir, f"{name}.tif")
                elif len(polygons_to_process) == 1:
                    output_result_file = os.path.join(output_dir, f"{name}_part{idx}.tif")
                else:
                    output_result_file = os.path.join(output_dir, f"{name}_part{idx}_sub{sub_idx}.tif")

                with rasterio.open(output_result_file, 'w', driver='GTiff',
                                   height=result_array.shape[0], width=result_array.shape[1],
                                   count=1, dtype='uint8', crs=dem_src.crs,
                                   transform=transform) as dst:
                    dst.write(result_array, 1)

                results.append({'status': 'success', 'name': name, 'part': idx, 'subpart': sub_idx})

        del lulc_data, dem_data, result_array, mask_index
        gc.collect()

        if len(results) == 0:
            return {'status': 'no_result', 'name': name}
        else:
            return {'status': 'multi_success', 'name': name, 'parts': len(results)}

    except Exception as e:
        print(f"Error processing {name}: {e}")
        return {'status': 'error', 'name': name, 'error': str(e)}

def main(bufferX):
    start_time = time.time()
    for leixing in leixings:
        for year in years:
            for scenerio in scenerios:
                print(f"\nProcessing: {leixing}, {year}, {scenerio}")
                lulc_tif_path = f'E:/Datasatlast/tif/LUCCTYPRO/{scenerio}_{year}.tif'
                dem_tif_path = r'E:/Datasatlast/tif/DEM1000.tif'
                with rasterio.open(lulc_tif_path) as lulc_src, rasterio.open(dem_tif_path) as dem_src:
                    lulc_mask_shape = get_nonzero_mask_shape(lulc_src)
                    csv_path = f'E:/Datasatlast/Lastcsv/{leixing}匹配后最新.csv'
                    with open(csv_path, 'r', encoding='gb18030') as file:
                        rows = list(csv.reader(file))
                    header = rows[0]
                    data_rows = rows[1:]
                    counts = len(data_rows)
                    name_index = header.index('scientificName')
                    lulc_index = header.index('LUCC')
                    low_index = header.index('DEM_MIN')
                    high_index = header.index('DEM_MAX')
                    sceneriolast = scenerio_map.get(scenerio, 'unknown')
                    if bufferX == 0.1:
                        output_base = f'E:/Datasatlast/DataBuffer/lastresults0.1/{sceneriolast}/{year}'
                    if bufferX == -0.1:
                        output_base = f'E:/Datasatlast/DataBuffer/lastresults-0.1/{sceneriolast}/{year}'
                    if bufferX == 0:
                        output_base = f'E:/Datasatlast/DataBuffer/lastresults0/{sceneriolast}/{year}'

                    os.makedirs(output_base, exist_ok=True)
                    success_count = 0
                    error_count = 0
                    for idx, row in enumerate(data_rows):
                        try:
                            name = row[name_index]
                            lulc = row[lulc_index]
                            low = float(row[low_index]) if row[low_index] else 0
                            high = float(row[high_index]) if row[high_index] else 5000

                            print(f"[{idx+1}/{counts}] Processing {name}")
                            type_row = gdf[gdf['sci_name'] == name]
                            type_ = type_row['types'].iloc[0] if not type_row.empty else 'unknown'
                            output_dir = os.path.join(output_base, type_)
                            os.makedirs(output_dir, exist_ok=True)
                            result = process_species(name, lulc, low, high, gdf, lulc_src, dem_src, sceneriolast, year, type_, output_dir,bufferX, lulc_mask_shape)
                            if result['status'] in ['success', 'multi_success']:
                                success_count += 1
                            else:
                                error_count += 1
                            percentage = ((idx + 1) / counts) * 100
                            print(f"Progress: {idx+1}/{counts} ({percentage:.2f}%)")
                        except Exception as e:
                            print(f"Error processing {name}: {e}")
                            continue

                    print(f"\nSummary for {leixing}, {year}, {scenerio}:")
                    print(f"Success: {success_count}, Errors: {error_count}")
                    gc.collect()
                    time.sleep(10)
            end_time = time.time()
            print(f"\nTotal execution time: {(end_time - start_time)/60:.2f} minutes")


# 场景配置
# scenerios = ['ssp1_26', 'ssp2_45', 'ssp3_70', 'ssp5_85']
scenerios = ['ssp1_26', 'ssp2_45']
# # leixings = ['哺乳类']
leixings = ['鸟']
years = ['2100']
# years = ['2100']
# scenerios = ['real']
# years = ['2020']
# leixings = ['两栖类', '哺乳类', '爬行类', '鸟']
from multiprocessing import Process
if __name__ == '__main__':
    layer_name = r'E:/Datasatlast/shp/Lastmerged.shp'
    gdf = gpd.read_file(layer_name, encoding='gbk')
    scenerio_map = {
        'real': 'real',
        'history': 'baseline',
        'ssp1_26': 'SSP1_RCP26',
        'ssp2_45': 'SSP2_RCP45',
        'ssp3_70': 'SSP3_RCP70',
        'ssp5_85': 'SSP5_RCP85'
    }
    bufferXs = [0.1, 0, -0.1]
    for bufferX in bufferXs:
        main(bufferX)
    # bufferXs = [0.1, -0.1, 0]
    # processes = []
    #
    # for bufferX in bufferXs:
    #     p = Process(target=main, args=(bufferX,))
    #     p.start()
    #     processes.append(p)
    #
    # for p in processes:
    #     p.join()  # 等待所有进程完成