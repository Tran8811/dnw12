import duckdb
from minio import Minio
from minio.error import S3Error
import os
import pandas as pd

# ========================
# 1️⃣ Khởi tạo client MinIO (7.x+)
# ========================
client = Minio(
    endpoint="localhost:9000",       # Hoặc EXTERNAL-IP của MinIO
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

bucket_name = "datalake"
object_name = "sample_data.csv"

# Tạo bucket nếu chưa có
if not client.bucket_exists(bucket_name=bucket_name):
    client.make_bucket(bucket_name=bucket_name)
    print(f"✓ Bucket '{bucket_name}' đã được tạo")
else:
    print(f"✓ Bucket '{bucket_name}' đã tồn tại")

# ========================
# 2️⃣ Tạo CSV sample
# ========================
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, 35, 28, 32],
    'city': ['Hanoi', 'HCMC', 'Danang', 'Hanoi', 'HCMC'],
    'salary': [50000, 60000, 70000, 55000, 58000]
})

csv_file = 'sample_data.csv'
df.to_csv(csv_file, index=False)
print(f"✓ File CSV đã được tạo: {csv_file}")

# Upload CSV lên MinIO
client.fput_object(bucket_name=bucket_name, object_name=object_name, file_path=csv_file, content_type="text/csv")
print(f"✓ File '{csv_file}' đã được upload thành công!\n")

# ========================
# 3️⃣ Download CSV từ MinIO
# ========================
try:
    print("📥 Đang tải dữ liệu từ MinIO...")
    response = client.get_object(bucket_name=bucket_name, object_name=object_name)
    csv_data = response.read().decode('utf-8')
    response.close()
    response.release_conn()
    print("✓ Đã tải xong!\n")
    
    # Lưu file tạm
    temp_file = 'temp_data.csv'
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(csv_data)
    
    # ========================
    # 4️⃣ Kết nối DuckDB & đọc CSV
    # ========================
    con = duckdb.connect(':memory:')
    con.execute(f"""
        CREATE TABLE data AS 
        SELECT * FROM read_csv_auto(
            '{temp_file}',
            delim=',',
            quote='"',
            strict_mode=False,
            encoding='utf-8'
        )
    """)
    
    # ========================
    # 5️⃣ TRUY VẤN 1: Tất cả dữ liệu
    # ========================
    print("="*70)
    print("TRUY VẤN 1: TẤT CẢ DỮ LIỆU")
    print("="*70)
    print(con.execute("SELECT * FROM data").fetchdf().to_string(index=False))
    print()
    
    # ========================
    # 6️⃣ TRUY VẤN 2: Nhân viên tuổi >28 & lương >55000
    # ========================
    print("="*70)
    print("TRUY VẤN 2: NHÂN VIÊN CÓ TUỔI > 28 VÀ LƯƠNG > 55000")
    print("="*70)
    print(con.execute("""
        SELECT * FROM data
        WHERE age > 28 AND salary > 55000
        ORDER BY salary DESC
    """).fetchdf().to_string(index=False))
    print()
    
    # ========================
    # 7️⃣ TRUY VẤN 3: Thống kê theo thành phố
    # ========================
    print("="*70)
    print("TRUY VẤN 3: THỐNG KÊ THEO THÀNH PHỐ")
    print("="*70)
    print(con.execute("""
        SELECT city,
               COUNT(*) as total_people,
               ROUND(AVG(age),1) as avg_age,
               ROUND(AVG(salary),0) as avg_salary,
               MIN(salary) as min_salary,
               MAX(salary) as max_salary
        FROM data
        GROUP BY city
        ORDER BY total_people DESC
    """).fetchdf().to_string(index=False))
    print()
    
    # ========================
    # 8️⃣ TRUY VẤN 4: Top 5 lương cao nhất
    # ========================
    print("="*70)
    print("TRUY VẤN 4: TOP 5 NGƯỜI CÓ LƯƠNG CAO NHẤT")
    print("="*70)
    print(con.execute("""
        SELECT name, age, city, salary
        FROM data
        ORDER BY salary DESC
        LIMIT 5
    """).fetchdf().to_string(index=False))
    print()
    
    # ========================
    # 9️⃣ TRUY VẤN 5: Thống kê tổng quan
    # ========================
    print("="*70)
    print("TRUY VẤN 5: THỐNG KÊ TỔNG QUAN")
    print("="*70)
    print(con.execute("""
        SELECT COUNT(*) as total_records,
               COUNT(DISTINCT city) as total_cities,
               MIN(age) as youngest,
               MAX(age) as oldest,
               ROUND(AVG(age),1) as avg_age,
               MIN(salary) as min_salary,
               MAX(salary) as max_salary,
               ROUND(AVG(salary),0) as avg_salary
        FROM data
    """).fetchdf().to_string(index=False))
    print()
    
    # ========================
    # 🔟 TRUY VẤN 6: Phân loại mức lương
    # ========================
    print("="*70)
    print("TRUY VẤN 6: PHÂN LOẠI MỨC LƯƠNG")
    print("="*70)
    print(con.execute("""
        SELECT name, age, city, salary,
               CASE
                   WHEN salary >= 65000 THEN 'Cao'
                   WHEN salary >= 55000 THEN 'Trung bình'
                   ELSE 'Thấp'
               END as salary_level
        FROM data
        ORDER BY salary DESC
    """).fetchdf().to_string(index=False))
    print()
    
    con.close()
    
    # Xóa file tạm
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    print("="*70)
    print("✓ HOÀN THÀNH TẤT CẢ TRUY VẤN!")
    print("="*70)

except S3Error as e:
    print(f"❌ Lỗi MinIO: {e}")
except Exception as e:
    print(f"❌ Lỗi: {e}")
