import arcpy
import os
from multiprocessing import Pool, cpu_count

# 定义要处理的情景、年份和类型
scenerios = ['SSP1_RCP26', 'SSP2_RCP45', 'SSP3_RCP70', 'SSP5_RCP85']
years = ['2030', '2050', '2070', '2100']
# 单独处理基
# scenerios = ['history']
# scenerios = ['real']
# years = ['2020']
#
# years =  ['2030', '2050', '2070', '2100']
leixing = ['AMPHIBIANS', 'BIRD', 'MAMMALS', 'REPTILES']
# leixing = ['PLANTS']
def process_mosaic(params):
    type1, year, scenerio = params
    # scenerio = 'SSP2_RCP45'
    print(params)
    # 设置工作空间
    arcpy.env.workspace = f'lastresults/{scenerio}/{year}/{type1}'
    print(arcpy.env.workspace)
    # 列出工作空间下的所有 TIF 文件
    tif_list = arcpy.ListRasters("*.tif")
    if type1 == 'BIRD':
        tif_list = [file for file in tif_list if file != "Megalurulus rufus.tif"]
    print(tif_list)
    # 设置输出文件的完整路径
    output_path = f'lastresults/combine/{scenerio}/{year}'
    os.makedirs(output_path, exist_ok=True)
    output_name = f'{scenerio}_{year}_{type1}.tif'
    # output_full_path = os.path.join(output_path, output_name)

    # 使用 MosaicToNewRaster 进行合并，使用 "SUM" 方法，设置像素类型为 "32_BIT_FLOAT"
    arcpy.MosaicToNewRaster_management(tif_list, output_path, output_name, pixel_type="32_BIT_FLOAT",
                                       number_of_bands="1", mosaic_method='SUM')
    print(f"Mosaic {output_name} completed successfully.")

if __name__ == "__main__":
    # 获取所有组合参数
    params_list = [(type1, year, scenerio) for type1 in leixing for year in years for scenerio in scenerios]

    # 使用多进程处理
    # with Pool(cpu_count()) as pool:
    with Pool(2) as pool:
        pool.map(process_mosaic, params_list)