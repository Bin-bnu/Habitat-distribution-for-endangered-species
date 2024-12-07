import os
import rasterio
import numpy as np
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor


def process_single_file(args):
    tif_file, folder_A, folder_B, output_folder = args

    # 读取2020年栖息地数据
    with rasterio.open(os.path.join(folder_A, tif_file)) as src_2020:
        habitat_2020 = src_2020.read(1)
        meta = src_2020.meta.copy()  # 保留元数据，用于保存新文件

    # 初始化一个新的栖息地变化区域的矩阵
    change_area = np.zeros_like(habitat_2020, dtype=np.float32)

    # 检查B文件夹中是否存在对应的文件
    tif_file_B_path = os.path.join(folder_B, tif_file)
    if os.path.exists(tif_file_B_path):
        # 读取2050年的栖息地数据
        with rasterio.open(tif_file_B_path) as src_2050:
            habitat_2050 = src_2050.read(1)
        # 未变化区域
        change_area[(habitat_2020 > 0) & (habitat_2050 > 0)] = 0.001  # 未变化区域（低值）
        # 找出2050年相较于2020年减少的区域
        change_area[(habitat_2020 > 0) & (habitat_2050 == 0)] = -1
        # 找出2050年相较于2020年增加的区域{year}/
        change_area[(habitat_2020 == 0) & (habitat_2050 > 0)] = 1
    else:
        # 如果B文件夹中没有对应的文件，表示全部损失
        change_area[habitat_2020 > 0] = -1

    # 更新元数据以写入单个band的TIFF
    meta.update(dtype=rasterio.float32, count=1, compress='lzw')

    # 保存新的变化区域TIFF文件
    output_file_path = os.path.join(output_folder, f"change_{tif_file}")
    with rasterio.open(output_file_path, 'w', **meta) as dst:
        dst.write(change_area, 1)

    print(f"保存完毕 {output_file_path}")


def process_files(args):
    year, leixing, lujing = args
    print(f"Processing {year} - {leixing}")

    # 文件夹路径
    folder_A = f"lastresults/baseline/2020/{leixing}"  # 替换为A文件夹的路径
    folder_B = f"lastresults/{lujing}/{year}/{leixing}"  # 替换为B文件夹的路径
    output_folder = f"change_results/{lujing}/{year}/{leixing}"  # 输出文件夹路径

    # 确保输出文件夹存在
    os.makedirs(output_folder, exist_ok=True)

    # 获取文件列表
    tif_files_A = [f for f in os.listdir(folder_A) if f.endswith('.tif')]

    # 使用多线程处理文件
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        tasks = [(tif_file, folder_A, folder_B, output_folder) for tif_file in tif_files_A]
        executor.map(process_single_file, tasks)


def main():
    lujings = ['SSP1_RCP26', 'SSP2_RCP45', 'SSP3_RCP70', 'SSP5_RCP85']
    leixings = ['AMPHIBIANS', 'BIRD', 'MAMMALS', 'REPTILES', 'PLANTS']
    years = ['2030', '2050', '2070', '2100']

    # 创建进程池
    with Pool(processes=4) as pool:
        # 创建任务列表
        tasks = [(year, leixing, lujing) for year in years for leixing in leixings for lujing in lujings]
        # 并行处理
        pool.map(process_files, tasks)


if __name__ == '__main__':
    main()
