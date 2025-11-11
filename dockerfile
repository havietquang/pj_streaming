# Kế thừa từ image gốc bạn đang dùng
FROM apache/airflow:2.7.1

# Cài đặt thư viện bạn cần
RUN pip install kafka-python