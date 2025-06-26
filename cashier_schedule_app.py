import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import calendar
import random
import numpy as np
import sqlite3
import os

# Hàm kết nối và khởi tạo cơ sở dữ liệu SQLite
def init_db():
    conn = sqlite3.connect('schedule.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS employees
                 (id TEXT PRIMARY KEY, name TEXT, rank TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS schedule
                 (emp_id TEXT, date TEXT, shift TEXT, PRIMARY KEY (emp_id, date))''')
    c.execute('''CREATE TABLE IF NOT EXISTS manual_shifts
                 (emp_id TEXT, date TEXT, shift TEXT, PRIMARY KEY (emp_id, date))''')
    conn.commit()
    return conn

# Hàm lưu nhân viên vào DB
def save_employees_to_db(employees):
    conn = init_db()
    c = conn.cursor()
    c.execute('DELETE FROM employees')
    for emp in employees:
        c.execute('INSERT OR REPLACE INTO employees (id, name, rank) VALUES (?, ?, ?)',
                  (emp['ID'], emp['Họ Tên'], emp['Cấp bậc']))
    conn.commit()
    conn.close()

# Hàm tải nhân viên từ DB
def load_employees_from_db():
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT id, name, rank FROM employees')
    employees = [{'ID': row[0], 'Họ Tên': row[1], 'Cấp bậc': row[2]} for row in c.fetchall()]
    conn.close()
    return employees

# Hàm lưu lịch vào DB
def save_schedule_to_db(schedule, month_days):
    conn = init_db()
    c = conn.cursor()
    c.execute('DELETE FROM schedule')
    for emp_id, shifts in schedule.items():
        for day, shift in enumerate(shifts):
            if shift:
                date = month_days[day].strftime('%Y-%m-%d')
                c.execute('INSERT OR REPLACE INTO schedule (emp_id, date, shift) VALUES (?, ?, ?)',
                          (emp_id, date, shift))
    conn.commit()
    conn.close()

# Hàm tải lịch từ DB
def load_schedule_from_db(month_days):
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT emp_id, date, shift FROM schedule')
    schedule = {}
    date_to_index = {d.strftime('%Y-%m-%d'): i for i, d in enumerate(month_days)}
    for emp_id, date, shift in c.fetchall():
        if emp_id not in schedule:
            schedule[emp_id] = [''] * len(month_days)
        if date in date_to_index:
            schedule[emp_id][date_to_index[date]] = shift
    conn.close()
    return schedule

# Hàm lưu manual_shifts vào DB
def save_manual_shifts_to_db(manual_shifts, month_days):
    conn = init_db()
    c = conn.cursor()
    c.execute('DELETE FROM manual_shifts')
    for (emp_id, day), shift in manual_shifts.items():
        date = month_days[day].strftime('%Y-%m-%d')
        c.execute('INSERT OR REPLACE INTO manual_shifts (emp_id, date, shift) VALUES (?, ?, ?)',
                  (emp_id, date, shift))
    conn.commit()
    conn.close()

# Hàm tải manual_shifts từ DB
def load_manual_shifts_from_db(month_days):
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT emp_id, date, shift FROM manual_shifts')
    manual_shifts = {}
    date_to_index = {d.strftime('%Y-%m-%d'): i for i, d in enumerate(month_days)}
    for emp_id, date, shift in c.fetchall():
        if date in date_to_index:
            manual_shifts[(emp_id, date_to_index[date])] = shift
    conn.close()
    return manual_shifts

# Hàm tạo danh sách các ca hợp lệ
def get_valid_shifts():
    shifts = []
    for start_hour in range(7, 17):  # VX: 7h00 đến 16h00
        shifts.append(f"VX{start_hour:02d}")
    for start_hour in range(7, 22):  # V8: 7h00 đến 21h00
        shifts.append(f"V8{start_hour:02d}")
    for start_hour in range(7, 28):  # V6: 7h00 đến 27h00
        shifts.append(f"V6{start_hour:02d}")
    return shifts

# Hàm kiểm tra quy tắc lịch
def is_valid_schedule(employee_schedule, employee, day, shift, month_days, sundays):
    consecutive_days = 0
    for d in range(day - 1, max(-1, day - 8), -1):
        if d < 0 or employee_schedule.get(employee, [None] * len(month_days))[d] not in ["PRD", "AL", "NPL", None]:
            consecutive_days += 1
        else:
            break
    if consecutive_days >= 7 and shift not in ["PRD", "AL", "NPL"]:
        return False
    
    vx_count = sum(1 for s in employee_schedule.get(employee, []) if isinstance(s, str) and s.startswith("VX"))
    v6_count = sum(1 for s in employee_schedule.get(employee, []) if isinstance(s, str) and s.startswith("V6"))
    if shift.startswith("VX"):
        vx_count += 1
    elif shift.startswith("V6"):
        v6_count += 1
    if vx_count > v6_count + 1 or v6_count > vx_count + 1:
        return False
    
    date = month_days[day]
    weekday = date.weekday()
    if weekday in [5, 6] and shift in ["PRD", "AL", "NPL"] and (employee, day) not in st.session_state.manual_shifts:
        return False
    
    return True

# Hàm tạo lịch tự động
def auto_schedule(employees, month_days, sundays):
    if not employees:
        return {}
    
    schedule = {emp["ID"]: [""] * len(month_days) for emp in employees}
    manual_shifts = st.session_state.get("manual_shifts", {})
    
    # Điền các ca thủ công
    for (emp_id, day), shift in manual_shifts.items():
        if emp_id in schedule:
            schedule[emp_id][day] = shift
    
    # Phân bổ ngày PRD
    total_employees = len(employees)
    prd_per_employee = len(sundays)
    non_weekend_days = [i for i, d in enumerate(month_days) if d.weekday() not in [5, 6]]
    
    if non_weekend_days and prd_per_employee > 0:
        prd_per_day = (total_employees * prd_per_employee) // len(non_weekend_days)
        prd_counts = [prd_per_day] * len(non_weekend_days)
        extra_prds = (total_employees * prd_per_employee) - prd_per_day * len(non_weekend_days)
        for i in range(extra_prds):
            prd_counts[i] += 1
        
        for emp in employees:
            emp_id = emp["ID"]
            available_days = [d for d in non_weekend_days if schedule[emp_id][d] == ""]
            if len(available_days) < prd_per_employee:
                continue
            selected_prd_days = random.sample(available_days, prd_per_employee)
            for day in selected_prd_days:
                schedule[emp_id][day] = "PRD"
                prd_counts[non_weekend_days.index(day)] -= 1
                if prd_counts[non_weekend_days.index(day)] < 0:
                    prd_counts[non_weekend_days.index(day)] = 0
    
    valid_shifts = get_valid_shifts()
    morning_shifts = [s for s in valid_shifts if int(s[2:4]) < 12]
    evening_shifts = [s for s in valid_shifts if int(s[2:4]) >= 12]
    
    for day in range(len(month_days)):
        random.shuffle(employees)
        morning_count = int(len(employees) * 0.4)
        for i, emp in enumerate(employees):
            emp_id = emp["ID"]
            if schedule[emp_id][day] in ["PRD", "AL", "NPL"]:
                continue
            shift_pool = morning_shifts if i < morning_count else evening_shifts
            for shift in random.sample(shift_pool, len(shift_pool)):
                if is_valid_schedule(schedule, emp_id, day, shift, month_days, sundays):
                    schedule[emp_id][day] = shift
                    break
            else:
                schedule[emp_id][day] = "PRD" if month_days[day].weekday() not in [5, 6] else random.choice(shift_pool)
    
    save_schedule_to_db(schedule, month_days)
    return schedule

# Khởi tạo trạng thái phiên
if "employees" not in st.session_state:
    st.session_state.employees = load_employees_from_db()
if "schedule" not in st.session_state:
    st.session_state.schedule = {}
if "manual_shifts" not in st.session_state:
    st.session_state.manual_shifts = {}

# Giao diện chính
st.title("Phần mềm quản lý ca làm việc cho Cashier")

# Khai báo các tab
tab1, tab2, tab3 = st.tabs(["Quản lý nhân viên", "Sắp lịch", "Báo cáo"])

# Tab 1: Quản lý nhân viên
with tab1:
    st.subheader("Quản lý nhân viên")
    
    # Import nhân viên từ CSV
    st.subheader("Import nhân viên từ CSV")
    uploaded_file = st.file_uploader("Chọn file CSV", type=["csv"])
    if uploaded_file:
        df_uploaded = pd.read_csv(uploaded_file)
        expected_columns = ["ID", "Họ Tên", "Cấp bậc"]
        if all(col in df_uploaded.columns for col in expected_columns):
            for _, row in df_uploaded.iterrows():
                if row["ID"] not in [emp["ID"] for emp in st.session_state.employees]:
                    st.session_state.employees.append({
                        "ID": str(row["ID"]),
                        "Họ Tên": row["Họ Tên"],
                        "Cấp bậc": row["Cấp bậc"]
                    })
            save_employees_to_db(st.session_state.employees)
            st.success("Đã import nhân viên thành công!")
        else:
            st.error("File CSV phải chứa các cột: ID, Họ Tên, Cấp bậc")
    
    # Export nhân viên ra CSV
    st.subheader("Export nhân viên ra CSV")
    if st.session_state.employees:
        if st.button("Tải danh sách nhân viên"):
            df_employees = pd.DataFrame(st.session_state.employees)
            csv = df_employees.to_csv(index=False)
            st.download_button(
                label="Tải file CSV",
                data=csv,
                file_name="danh_sach_nhan_vien.csv",
                mime="text/csv"
            )
    
    # Thêm nhân viên thủ công
    st.subheader("Thêm nhân viên thủ công")
    with st.form("employee_form"):
        emp_id = st.text_input("ID nhân viên")
        emp_name = st.text_input("Họ Tên")
        emp_rank = st.selectbox("Cấp bậc", ["Junior", "Senior", "Manager"], key="add_rank")
        submitted = st.form_submit_button("Thêm nhân viên")
        if submitted and emp_id and emp_name:
            if emp_id not in [emp["ID"] for emp in st.session_state.employees]:
                st.session_state.employees.append({"ID": emp_id, "Họ Tên": emp_name, "Cấp bậc": emp_rank})
                save_employees_to_db(st.session_state.employees)
                st.success(f"Đã thêm nhân viên {emp_name}")
            else:
                st.error("ID nhân viên đã tồn tại!")
    
    # Điều chỉnh thông tin nhân viên
    st.subheader("Điều chỉnh thông tin nhân viên")
    if st.session_state.employees:
        emp_ids = [emp["ID"] for emp in st.session_state.employees]
        selected_emp_id = st.selectbox("Chọn ID nhân viên để chỉnh sửa", emp_ids)
        selected_emp = next(emp for emp in st.session_state.employees if emp["ID"] == selected_emp_id)
        
        with st.form("edit_employee_form"):
            edit_emp_id = st.text_input("ID nhân viên", value=selected_emp["ID"], key="edit_id")
            edit_emp_name = st.text_input("Họ Tên", value=selected_emp["Họ Tên"], key="edit_name")
            edit_emp_rank = st.selectbox("Cấp bậc", ["Junior", "Senior", "Manager"], 
                                       index=["Junior", "Senior", "Manager"].index(selected_emp["Cấp bậc"]), 
                                       key="edit_rank")
            edit_submitted = st.form_submit_button("Cập nhật nhân viên")
            if edit_submitted:
                if edit_emp_id != selected_emp_id and edit_emp_id in emp_ids:
                    st.error("ID nhân viên mới đã tồn tại!")
                else:
                    for emp in st.session_state.employees:
                        if emp["ID"] == selected_emp_id:
                            emp["ID"] = edit_emp_id
                            emp["Họ Tên"] = edit_emp_name
                            emp["Cấp bậc"] = edit_emp_rank
                            if edit_emp_id != selected_emp_id and st.session_state.schedule:
                                st.session_state.schedule[edit_emp_id] = st.session_state.schedule.pop(selected_emp_id, [])
                                st.session_state.manual_shifts = {
                                    (edit_emp_id if k[0] == selected_emp_id else k[0], k[1]): v 
                                    for k, v in st.session_state.manual_shifts.items()
                                }
                                save_schedule_to_db(st.session_state.schedule, month_days)
                                save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                            break
                    save_employees_to_db(st.session_state.employees)
                    st.success(f"Đã cập nhật thông tin nhân viên {edit_emp_name}")
    
    # Hiển thị danh sách nhân viên
    if st.session_state.employees:
        st.subheader("Danh sách nhân viên")
        df_employees = pd.DataFrame(st.session_state.employees)
        st.dataframe(df_employees)

# Tab 2: Sắp lịch
with tab2:
    st.subheader("Sắp lịch làm việc")
    year = st.number_input("Năm", min_value=2020, max_value=2030, value=2025)
    month = st.number_input("Tháng", min_value=1, max_value=12, value=datetime.now().month)
    
    _, last_day = calendar.monthrange(year, month)
    start_date = datetime(year, month, 26)
    end_date = datetime(year, month + 1, 25) if month < 12 else datetime(year + 1, 1, 25)
    month_days = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    sundays = [i for i, d in enumerate(month_days) if d.weekday() == 6]
    
    # Tải manual_shifts từ DB nếu chưa có
    if not st.session_state.manual_shifts:
        st.session_state.manual_shifts = load_manual_shifts_from_db(month_days)
    
    if st.button("Tạo lịch tự động"):
        if not st.session_state.employees:
            st.error("Vui lòng thêm nhân viên trước khi tạo lịch!")
        else:
            st.session_state.schedule = auto_schedule(st.session_state.employees, month_days, sundays)
    
    if st.session_state.schedule:
        # Tải lịch từ DB nếu chưa có
        if not any(st.session_state.schedule.values()):
            st.session_state.schedule = load_schedule_from_db(month_days)
        
        st.subheader("Chỉnh sửa lịch")
        valid_shifts = get_valid_shifts() + ["PRD", "AL", "NPL"]
        columns = [f"{d.strftime('%a %d/%m')}" for d in month_days]
        schedule_data = {col: [] for col in ["Họ Tên"] + columns}
        
        for emp in st.session_state.employees:
            emp_id = emp["ID"]
            schedule_data["Họ Tên"].append(emp["Họ Tên"])
            for day, col in enumerate(columns):
                current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                schedule_data[col].append(current_shift)
        
        df_schedule = pd.DataFrame(schedule_data)
        
        # Tạo bảng chỉnh sửa với st.data_editor
        st.write("Chọn ca cho từng ngày:")
        column_config = {
            "Họ Tên": st.column_config.TextColumn(disabled=True),
        }
        for col in columns:
            column_config[col] = st.column_config.SelectboxColumn(
                options=valid_shifts,
                default="",
                width="small"
            )
        
        edited_df = st.data_editor(
            df_schedule,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key="schedule_editor"
        )
        
        # Cập nhật lịch nếu có thay đổi
        for i, emp in enumerate(st.session_state.employees):
            emp_id = emp["ID"]
            for day, col in enumerate(columns):
                new_shift = edited_df.iloc[i][col]
                current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                if new_shift != current_shift and new_shift in valid_shifts:
                    if is_valid_schedule(st.session_state.schedule, emp_id, day, new_shift, month_days, sundays):
                        st.session_state.schedule[emp_id][day] = new_shift
                        st.session_state.manual_shifts[(emp_id, day)] = new_shift
                        save_schedule_to_db(st.session_state.schedule, month_days)
                        save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                    else:
                        st.warning(f"Ca {new_shift} không hợp lệ cho {emp['Họ Tên']} vào ngày {col}")
        
        st.subheader("Lịch làm việc")
        st.dataframe(df_schedule, use_container_width=True)

# Tab 3: Báo cáo
with tab3:
    st.subheader("Báo cáo")
    if st.session_state.schedule:
        if st.button("Tải báo cáo"):
            df_report = pd.DataFrame(st.session_state.schedule).T
            df_report.index.name = "ID Nhân viên"
            df_report.columns = [d.strftime("%d/%m") for d in month_days]
            csv = df_report.to_csv()
            st.download_button(
                label="Tải báo cáo CSV",
                data=csv,
                file_name=f"lich_ca_{year}_{month}.csv",
                mime="text/csv"
            )
    else:
        st.write("Cần tạo lịch trước khi tải báo cáo.")