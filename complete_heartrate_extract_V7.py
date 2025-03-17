from lxml import etree
import csv
import os
from datetime import datetime
import time

def extract_heartrate_from_cda():
    # 文件配置
    xml_file = "C:/apple_health_export/export_cda.xml"
    csv_file = "C:/apple_health_export/heartrate_extracted.csv"
    log_file = "C:/apple_health_export/heartrate_extraction_log.txt"
    
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    
    # 设置命名空间
    NSMAP = {
        'cda': 'urn:hl7-org:v3'
    }
    
    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f_csv, \
         open(log_file, 'w', encoding='utf-8') as f_log:
        
        # CSV字段
        fieldnames = [
            'Date', 'Value', 'Unit', 'Motion_Context', 
            'Source_Name', 'Source_Version', 'Device'
        ]
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        
        # 记录开始
        start_time = time.time()
        f_log.write(f"开始提取心率数据: {datetime.now()}\n")
        f_log.write(f"源文件: {xml_file}\n")
        f_log.write(f"目标文件: {csv_file}\n\n")
        
        # 计数器
        record_count = 0
        process_count = 0
        
        # 使用上下文管理器解析XML
        context = etree.iterparse(xml_file, events=('end',), tag=f'{{{NSMAP["cda"]}}}observation')
        
        # 处理每个observation元素
        for _, elem in context:
            process_count += 1
            
            # 每处理100万个元素记录一次进度
            if process_count % 1000000 == 0:
                elapsed = time.time() - start_time
                f_log.write(f"已处理 {process_count} 个observation元素, 找到 {record_count} 条心率记录, 用时 {elapsed:.2f} 秒\n")
                f_log.flush()
            
            # 检查是否为心率记录
            is_heartrate = False
            
            # 查找code元素
            code_elem = elem.find(f'.//{{{NSMAP["cda"]}}}code[@displayName="Heart rate"]')
            if code_elem is not None:
                is_heartrate = True
            else:
                # 查找type元素
                type_elem = elem.find(f'.//{{{NSMAP["cda"]}}}type')
                if type_elem is not None and type_elem.text and 'HeartRate' in type_elem.text:
                    is_heartrate = True
            
            if is_heartrate:
                record_count += 1
                record = {field: '' for field in fieldnames}
                
                # 提取值和单位
                value_elem = elem.find(f'.//{{{NSMAP["cda"]}}}value')
                if value_elem is not None:
                    record['Value'] = value_elem.text if value_elem.text else value_elem.get('value', '')
                    
                unit_elem = elem.find(f'.//{{{NSMAP["cda"]}}}unit')
                if unit_elem is not None:
                    record['Unit'] = unit_elem.text if unit_elem.text else unit_elem.get('unit', '')
                
                # 提取日期
                effectiveTime = elem.find(f'.//{{{NSMAP["cda"]}}}effectiveTime')
                if effectiveTime is not None:
                    low_elem = effectiveTime.find(f'.//{{{NSMAP["cda"]}}}low')
                    if low_elem is not None:
                        record['Date'] = low_elem.get('value', '')
                
                # 提取运动上下文
                metadataEntry = elem.find(f'.//{{{NSMAP["cda"]}}}metadataEntry')
                if metadataEntry is not None:
                    key_elem = metadataEntry.find(f'.//{{{NSMAP["cda"]}}}key')
                    value_elem = metadataEntry.find(f'.//{{{NSMAP["cda"]}}}value')
                    if key_elem is not None and key_elem.text and 'HeartRateMotionContext' in key_elem.text:
                        if value_elem is not None:
                            record['Motion_Context'] = value_elem.text
                
                # 提取来源信息
                sourceName = elem.find(f'.//{{{NSMAP["cda"]}}}sourceName')
                if sourceName is not None:
                    record['Source_Name'] = sourceName.text
                
                sourceVersion = elem.find(f'.//{{{NSMAP["cda"]}}}sourceVersion')
                if sourceVersion is not None:
                    record['Source_Version'] = sourceVersion.text
                
                # 提取设备信息
                device = elem.find(f'.//{{{NSMAP["cda"]}}}device')
                if device is not None:
                    record['Device'] = device.text
                
                # 写入CSV
                writer.writerow(record)
                
                # 记录样本数据
                if record_count <= 5 or record_count % 100000 == 0:
                    f_log.write(f"\n心率记录 #{record_count}:\n")
                    for k, v in record.items():
                        f_log.write(f"  {k}: {v}\n")
                    f_log.write("\n")
                    f_log.flush()
                
                # 定期刷新
                if record_count % 10000 == 0:
                    f_csv.flush()
            
            # 清理元素释放内存
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        # 统计
        end_time = time.time()
        total_time = end_time - start_time
        avg_speed = record_count / total_time if total_time > 0 and record_count > 0 else 0
        
        f_log.write("\n==== 最终统计 ====\n")
        f_log.write(f"总处理元素数: {process_count}\n")
        f_log.write(f"总提取心率记录: {record_count}\n")
        f_log.write(f"总用时: {total_time:.2f} 秒\n")
        f_log.write(f"平均处理速度: {avg_speed:.2f} 条/秒\n")
        f_log.write(f"提取完成: {datetime.now()}\n")
        
        return record_count, csv_file

if __name__ == "__main__":
    try:
        count, output_file = extract_heartrate_from_cda()
        print(f"提取完成。共提取 {count} 条心率记录，已保存至 {output_file}")
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
