# Fecom Inc. — Phân tích dữ liệu thương mại điện tử với PySpark

## Mô tả dự án

Fecom Inc. là công ty thương mại điện tử có trụ sở tại Berlin, Đức. Bài thực hành này sử dụng **Apache Spark DataFrame** để phân tích bộ dữ liệu bán hàng giai đoạn 2022–2024, bao gồm 99.441 đơn hàng, 102.727 khách hàng và 3.095 người bán trải rộng 338 thành phố tại 28 quốc gia.

---

## Cấu trúc thư mục

```
LAB-4
│
├── fecom_analysis.ipynb        # Notebook chính chứa toàn bộ lời giải
│
├── fecom_analysis.py           # File python chứa toàn bộ lời giải
│
├── data/                       # Thư mục chứa dữ liệu đầu vào (CSV, phân cách bằng ';')
│   ├── Orders.csv              # Thông tin đơn hàng (trạng thái, thời gian mua/giao...)
│   ├── Customer_List.csv       # Thông tin khách hàng (địa chỉ, tuổi, giới tính...)
│   ├── Order_Items.csv         # Chi tiết sản phẩm trong đơn hàng (giá, phí ship...)
│   ├── Products.csv            # Thông tin sản phẩm (danh mục, kích thước, trọng lượng)
│   └── Order_Reviews.csv       # Đánh giá đơn hàng (điểm, bình luận, thời gian...)
││
├── evidence-on-terminal.docx   # Minh chứng thực hiện code (python) trên terminal

└── README.md                   # Tài liệu mô tả dự án (file này)

```

---

## Mô tả các file dữ liệu

| File | Mô tả | Các cột chính |
|------|-------|---------------|
| `Orders.csv` | Thông tin đơn hàng | `Order_ID`, `Customer_Trx_ID`, `Order_Status`, `Order_Purchase_Timestamp`, `Order_Delivered_Carrier_Date` |
| `Customer_List.csv` | Thông tin khách hàng | `Customer_Trx_ID`, `Subscriber_ID`, `Customer_Country`, `Age`, `Gender` |
| `Order_Items.csv` | Chi tiết sản phẩm trong đơn | `Order_ID`, `Product_ID`, `Seller_ID`, `Price`, `Freight_Value`, `Shipping_Limit_Date` |
| `Products.csv` | Danh mục & thông số sản phẩm | `Product_ID`, `Product_Category_Name`, `Product_Weight_Gr` |
| `Order_Reviews.csv` | Đánh giá của khách hàng | `Review_ID`, `Order_ID`, `Review_Score` (1–5), `Review_Comment_Message_En` |

---

## Các câu hỏi được giải quyết

### Bắt buộc (Câu 1–5)

| Câu | Nội dung |
|-----|---------|
| **1** | Đọc dữ liệu từ CSV với `inferSchema=True` và `sep=";"` |
| **2** | Thống kê tổng số đơn hàng, khách hàng và người bán |
| **3** | Phân tích số đơn hàng theo quốc gia — sắp xếp giảm dần |
| **4** | Phân tích đơn hàng theo năm/tháng — năm tăng dần, tháng giảm dần |
| **5** | Thống kê điểm đánh giá trung bình & số lượng theo mức (1–5); xử lý NULL và ngoại lệ |

### Tự chọn (3 trong số câu 6–10) — đã chọn câu 6, 7, 8

| Câu | Nội dung |
|-----|---------|
| **6** | Doanh thu năm 2024 (Price + Freight_Value) nhóm theo danh mục sản phẩm |
| **7** | Sản phẩm bán nhiều nhất + điểm đánh giá trung bình từng sản phẩm |
| **8** | Hiệu suất giao hàng: hiệu số ngày giao thực tế vs. ngày dự kiến (Shipping_Limit_Date) |

---

## Yêu cầu môi trường

- Python ≥ 3.8
- Apache Spark ≥ 3.x (PySpark)
- Java ≥ 8 (bắt buộc để chạy Spark)

Cài đặt PySpark:

```bash
pip install pyspark
```

---

## Cách chạy

1. **Đặt các file CSV** vào thư mục `data/` (hoặc cập nhật biến `DATA_PATH` trong notebook).
2. **Mở notebook:**
   ```bash
   jupyter notebook fecom_analysis.ipynb
   ```
3. **Chạy tuần tự** từ đầu đến cuối (`Run All Cells`).

---

## Ghi chú kỹ thuật

- `inferSchema="true"` tự động nhận diện kiểu dữ liệu (timestamp, int, double...).
- Cột `Review_Score` được làm sạch: loại bỏ NULL và các giá trị nằm ngoài khoảng [1, 5].
- Hiệu số giao hàng âm có nghĩa là giao **trước** hạn; dương nghĩa là **trễ**.
- `SparkSession` được dừng ở cell cuối cùng sau khi hoàn thành phân tích.
