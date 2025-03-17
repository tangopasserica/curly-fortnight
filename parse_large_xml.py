import xml.etree.ElementTree as ET
import csv
import os

# 输入文件路径
input_file = "C:\\apple_health_export\\export_cda.xml"
output_file = "C:\\apple_health_export\\parsed_data_large.csv"

# 定义命名空间
namespaces = {
    "cda": "urn:hl7-org:v3"
}

# 初始化 CSV 文件
header = ["Data Type", "Start Date", "End Date", "Source Name", "Source Version"]
with open(output_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(header)  # 写入表头

# 使用 iterparse 逐行解析 XML 文件
context = ET.iterparse(input_file, events=("start", "end"))
context = iter(context)

# 遍历 XML 文件
for event, elem in context:
    # 检查是否是 <observation> 标签的结束事件
    if event == "end" and elem.tag.endswith("observation"):
        # 提取数据类型
        data_type_elem = elem.find(".//cda:type", namespaces)
        data_type = data_type_elem.text if data_type_elem is not None else "N/A"

        # 提取起止时间
        effective_time = elem.find(".//cda:effectiveTime", namespaces)
        if effective_time is not None:
            start_date_elem = effective_time.find(".//cda:low", namespaces)
            end_date_elem = effective_time.find(".//cda:high", namespaces)
            start_date = start_date_elem.attrib.get("value", "N/A") if start_date_elem is not None else "N/A"
            end_date = end_date_elem.attrib.get("value", "N/A") if end_date_elem is not None else "N/A"
        else:
            start_date = "N/A"
            end_date = "N/A"

        # 提取数据表头
        source_name_elem = elem.find(".//cda:text/cda:sourceName", namespaces)
        source_version_elem = elem.find(".//cda:text/cda:sourceVersion", namespaces)
        source_name = source_name_elem.text if source_name_elem is not None else "N/A"
        source_version = source_version_elem.text if source_version_elem is not None else "N/A"

        # 将提取的数据写入 CSV 文件
        with open(output_file, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow([data_type, start_date, end_date, source_name, source_version])

        # 清理已处理的元素以释放内存
        elem.clear()

print(f"Data successfully extracted and saved to {output_file}")
