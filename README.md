
# Cashier Schedule App 🧮

Ứng dụng **phân ca làm việc tự động và quản lý lịch làm việc** cho nhân viên thu ngân (Cashier) và bộ phận chăm sóc khách hàng (Customer Service), xây dựng bằng [Streamlit](https://streamlit.io/).

## 🎯 Tính năng nổi bật

- 📥 Import / Export danh sách nhân viên từ file CSV
- 🛠️ Tùy chỉnh ca làm việc theo ngày, theo nhân viên
- 🧠 Sắp xếp lịch làm việc tự động bằng thuật toán Memetic Algorithm
- 📊 Thống kê theo tuần, theo ngày, theo sáng/tối
- 🧾 Xuất báo cáo chi tiết sang file CSV
- 💾 Lưu trữ dữ liệu bằng SQLite (file `schedule.db`)

## 🚀 Cài đặt & chạy thử (trên máy tính cá nhân)

```bash
# Tạo môi trường ảo (tuỳ chọn)
python -m venv venv
source venv/bin/activate  # Hoặc venv\Scripts\activate trên Windows

# Cài các thư viện cần thiết
pip install -r requirements.txt

# Chạy ứng dụng
streamlit run cashier_schedule_app.py
```

> Ứng dụng sẽ chạy tại: `http://localhost:8501`

## ☁️ Triển khai lên Streamlit Cloud

1. Đảm bảo bạn đã có:
   - `cashier_schedule_app.py`
   - `requirements.txt` (chứa các thư viện: `streamlit`, `pandas`, `numpy`, ...)

2. Push code lên GitHub

3. Truy cập [https://streamlit.io/cloud](https://streamlit.io/cloud)

4. Chọn **New App → kết nối repo → chọn file `cashier_schedule_app.py` → Deploy**

## 📁 Cấu trúc dự án

```
.
├── cashier_schedule_app.py     # Mã chính của ứng dụng
├── requirements.txt            # Danh sách thư viện cần thiết
├── README.md                   # Tài liệu mô tả (file này)
└── schedule.db                 # (tự tạo) file SQLite lưu dữ liệu
```

## 📌 Mô tả kỹ thuật

- Ngôn ngữ: Python 3.9+
- Giao diện: [Streamlit](https://streamlit.io)
- Cơ sở dữ liệu: SQLite (dạng file)
- Giải thuật: Memetic Algorithm (lai giữa Genetic và Local Search)
- Phân ca theo chu kỳ từ ngày **26 tháng trước đến ngày 25 tháng hiện tại**

## ✅ Ví dụ file CSV danh sách nhân viên

```csv
ID,Họ Tên,Cấp bậc,Bộ phận
E001,Nguyễn Văn A,Junior,Cashier
E002,Trần Thị B,Senior,Customer Service
E003,Lê Văn C,Manager,Cashier
```

> Chấp nhận bộ phận: `Cashier`, `Customer Service`  
> Chấp nhận cấp bậc: `Junior`, `Senior`, `Manager`

## 📄 License

MIT License – bạn có thể sử dụng, chỉnh sửa và phân phối lại miễn là ghi rõ tác giả.

---

**🛠️ Developer**: Hiệp Nguyễn | ITLPRO.IO.VN
📬 Liên hệ: [nohlevel@gmail.com]
