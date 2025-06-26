import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import calendar
import numpy as np
import sqlite3
from functools import lru_cache
from ortools.sat.python import cp_model
import logging
import time
import math

# Thiết lập logging
logging.basicConfig(filename='schedule_debug.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Danh sách ngày lễ
HOLIDAYS = [
    "01/01", "03/02", "08/03", "26/03", "30/04", "01/05",
    "01/06", "27/07", "02/09", "10/10", "20/10", "20/11", "22/12", "24/12"
]

# Hàm kiểm tra ngày lễ
def is_holiday(date):
    return date.strftime("%d/%m") in HOLIDAYS

# Hàm lấy danh sách mã ca mặc định theo bộ phận
def get_default_shifts(department):
    all_shifts = get_valid_shifts()
    cs_shifts = ["V633", "V614", "V616", "V618", "V620", "V814", "V816", "V818", "V820", "V829", "VX22", "VX25", "PRD"]
    if department == "Customer Service":
        return cs_shifts
    elif department == "Cashier":
        return all_shifts + ["PRD"]
    else:  # Tất cả
        return list(set(all_shifts + cs_shifts))

# Hàm kết nối và khởi tạo cơ sở dữ liệu SQLite
def init_db():
    conn = sqlite3.connect('schedule.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS employees
                 (id TEXT PRIMARY KEY, name TEXT, rank TEXT, department TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS schedule
                 (emp_id TEXT, date TEXT, shift TEXT, PRIMARY KEY (emp_id, date))''')
    c.execute('''CREATE TABLE IF NOT EXISTS manual_shifts
                 (emp_id TEXT, date TEXT, shift TEXT, PRIMARY KEY (emp_id, date))''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY, value TEXT)''')
    conn.commit()
    return conn

# Hàm lưu nhân viên vào DB
def save_employees_to_db():
    conn = init_db()
    c = conn.cursor()
    c.execute('DELETE FROM employees')
    for emp in st.session_state.employees:
        c.execute('INSERT OR REPLACE INTO employees (id, name, rank, department) VALUES (?, ?, ?, ?)',
                  (emp['ID'], emp['Họ Tên'], emp['Cấp bậc'], emp['Bộ phận']))
    conn.commit()
    conn.close()

# Hàm tải nhân viên từ DB
def load_employees_from_db():
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT id, name, rank, department FROM employees')
    employees = [{'ID': row[0], 'Họ Tên': row[1], 'Cấp bậc': row[2], 'Bộ phận': row[3]} for row in c.fetchall()]
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

# Hàm lưu giới hạn VX tối thiểu vào DB
def save_vx_min_to_db(vx_min):
    conn = init_db()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
              ('vx_min', str(vx_min)))
    conn.commit()
    conn.close()

# Hàm tải giới hạn VX tối thiểu từ DB
def load_vx_min_from_db():
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = ?', ('vx_min',))
    result = c.fetchone()
    conn.close()
    return int(result[0]) if result else 3

# Hàm tạo danh sách các ca hợp lệ
def get_valid_shifts():
    shifts = []
    for code in range(14, 26):  # VX: 7h00 đến 12h30
        shifts.append(f"VX{code:02d}")
    for code in range(14, 30):  # V8: 7h00 đến 14h30
        shifts.append(f"V8{code:02d}")
    for code in range(14, 34):  # V6: 7h00 đến 16h30
        shifts.append(f"V6{code:02d}")
    return shifts

# Hàm lấy giờ bắt đầu từ mã ca
@lru_cache(maxsize=10000)
def get_shift_start_hour(shift):
    if shift in ["PRD", "AL", "NPL"]:
        return None
    code = int(shift[2:4])
    start_hour = code / 2
    return start_hour

# Hàm lấy giờ kết thúc từ mã ca
@lru_cache(maxsize=10000)
def get_shift_end_hour(shift):
    if shift in ["PRD", "AL", "NPL"]:
        return None
    start_hour = get_shift_start_hour(shift)
    if shift.startswith("VX"):
        return start_hour + 10
    elif shift.startswith("V8"):
        return start_hour + 8
    elif shift.startswith("V6"):
        return start_hour + 6
    return None

# Hàm kiểm tra tính khả thi của lịch
def check_feasibility(employees, month_days, selected_shifts):
    cs_employees = [emp for emp in employees if emp["Bộ phận"] == "Customer Service"]
    if len(cs_employees) < 3 and st.session_state.department_filter in ["Customer Service", "Tất cả"]:
        return False, "Cần ít nhất 3 nhân viên Customer Service để phân bổ ca bắt buộc"
    
    required_shifts = ["V814", "V614", "V818", "V618", "V829", "V633"]
    missing_shifts = [s for s in required_shifts if s not in selected_shifts]
    if missing_shifts:
        return False, "Thiếu ca bắt buộc: " + ", ".join(missing_shifts)
    
    if not any(s for s in selected_shifts if get_shift_start_hour(s) and get_shift_start_hour(s) < 12):
        return False, "Thiếu ca Sáng (bắt đầu trước 12h)"
    if not any(s for s in selected_shifts if get_shift_start_hour(s) and get_shift_start_hour(s) >= 12):
        return False, "Thiếu ca Tối (bắt đầu từ 12h trở đi)"
    
    if "PRD" not in selected_shifts:
        return False, "PRD không được chọn trong danh sách ca"
    
    return True, ""

# Hàm kiểm tra tính hợp lệ của lịch
def is_valid_schedule(employee_schedule, employee, day, shift, month_days, sundays, vx_min):
    errors = []
    
    # Kiểm tra không quá 7 ngày làm việc liên tục
    consecutive_days = 0
    for d in range(day - 1, max(-1, day - 8), -1):
        if d < 0 or employee_schedule.get(employee, [None] * len(month_days))[d] not in ["PRD", "AL", "NPL", None]:
            consecutive_days += 1
        else:
            break
    if consecutive_days >= 7 and shift not in ["PRD", "AL", "NPL"]:
        errors.append("Vượt quá 7 ngày làm việc liên tục")
    
    # Kiểm tra không PRD, VX, V6 liên tiếp
    if day > 0:
        prev_shift = employee_schedule.get(employee, [''] * len(month_days))[day - 1]
        if prev_shift and shift in ["PRD", "AL", "NPL"] and prev_shift in ["PRD", "AL", "NPL"]:
            errors.append("PRD/AL/NPL liên tiếp với ngày trước")
        if prev_shift and shift.startswith("VX") and prev_shift.startswith("VX"):
            errors.append("Ca VX liên tiếp với ngày trước")
        if prev_shift and shift.startswith("V6") and prev_shift.startswith("V6"):
            errors.append("Ca V6 liên tiếp với ngày trước")
    
    # Kiểm tra giãn cách 12 giờ
    if day > 0 and shift not in ["PRD", "AL", "NPL"]:
        prev_shift = employee_schedule.get(employee, [''] * len(month_days))[day - 1]
        if prev_shift and prev_shift not in ["PRD", "AL", "NPL"]:
            current_start = get_shift_start_hour(shift)
            prev_end = get_shift_end_hour(prev_shift)
            if current_start is not None and prev_end is not None:
                prev_end_hour = prev_end % 24
                prev_end_day_offset = 1 if prev_end >= 24 else 0
                prev_end_minutes = int((prev_end % 1) * 60)
                current_start_hour = int(current_start)
                current_start_minutes = int((current_start % 1) * 60)
                current_time = month_days[day].replace(hour=current_start_hour, minute=current_start_minutes)
                prev_time = month_days[day - 1 + prev_end_day_offset].replace(hour=int(prev_end_hour), minute=prev_end_minutes)
                time_diff = (current_time - prev_time).total_seconds() / 3600
                if time_diff < 12:
                    errors.append(f"Giãn cách dưới 12 giờ (giờ ra ca trước: {int(prev_end_hour):02d}:{prev_end_minutes:02d}, giờ bắt đầu: {current_start_hour:02d}:{current_start_minutes:02d})")
    
    # Kiểm tra số ca VX = V6 và không dưới vx_min
    vx_count = sum(1 for s in employee_schedule.get(employee, []) if isinstance(s, str) and s.startswith("VX"))
    v6_count = sum(1 for s in employee_schedule.get(employee, []) if isinstance(s, str) and s.startswith("V6"))
    if shift.startswith("VX"):
        vx_count += 1
    elif shift.startswith("V6"):
        v6_count += 1
    if day == len(month_days) - 1 and vx_count != v6_count:
        errors.append("Số ca VX và V6 không bằng nhau")
    if day == len(month_days) - 1 and vx_count < vx_min:
        errors.append(f"Số ca VX ({vx_count}) nhỏ hơn giới hạn tối thiểu ({vx_min})")
    
    # Kiểm tra PRD không rơi vào thứ 7, chủ nhật hoặc ngày lễ trừ khi nhập tay
    date = month_days[day]
    if (date.weekday() in [5, 6] or is_holiday(date)) and shift == "PRD" and (employee, day) not in st.session_state.manual_shifts:
        errors.append("PRD vào thứ 7/chủ nhật/ngày lễ không được phân tự động")
    
    # Kiểm tra AL, NPL chỉ được nhập tay
    if shift in ["AL", "NPL"] and (employee, day) not in st.session_state.manual_shifts:
        errors.append(f"Ca {shift} chỉ được nhập tay")
    
    # Kiểm tra số ngày PRD bằng số ngày Chủ nhật
    if day == len(month_days) - 1:
        prd_count = sum(1 for s in employee_schedule.get(employee, []) if s == "PRD")
        if prd_count != len(sundays):
            errors.append(f"Số ngày PRD ({prd_count}) không bằng số ngày Chủ nhật ({len(sundays)})")
    
    # Kiểm tra ca có trong danh sách ca đã chọn (trừ ca thủ công)
    if (employee, day) not in st.session_state.manual_shifts and shift not in ["PRD", "AL", "NPL"]:
        if shift not in st.session_state.selected_shifts:
            errors.append(f"Ca {shift} không nằm trong danh sách ca đã chọn")
    
    # Kiểm tra số lượng ca bắt buộc cho Customer Service
    emp_dept = next((emp["Bộ phận"] for emp in st.session_state.employees if emp["ID"] == employee), None)
    if emp_dept == "Customer Service":
        temp_schedule = employee_schedule.copy()
        if employee not in temp_schedule:
            temp_schedule[employee] = [''] * len(month_days)
        temp_schedule[employee][day] = shift
        cs_shifts = [temp_schedule.get(emp["ID"], [''] * len(month_days))[day] 
                     for emp in st.session_state.employees if emp["Bộ phận"] == "Customer Service"]
        
        v814_v614_count = cs_shifts.count("V814") + cs_shifts.count("V614")
        v818_v618_count = cs_shifts.count("V818") + cs_shifts.count("V618")
        v829_v633_count = cs_shifts.count("V829") + cs_shifts.count("V633")
        v633_count = cs_shifts.count("V633")
        
        if v814_v614_count > 1:
            errors.append("V814 hoặc V614 có hơn 1 nhân viên trong ngày")
        if v818_v618_count > 1:
            errors.append("V818 hoặc V618 có hơn 1 nhân viên trong ngày")
        if v829_v633_count > 2:
            errors.append("V829 hoặc V633 có hơn 2 nhân viên trong ngày")
        if v633_count > 1:
            errors.append("V633 có hơn 1 ca trong ngày")
    
    return len(errors) == 0, errors

# Hàm điều chỉnh ca không hợp lệ
def adjust_invalid_shifts(schedule, employees, month_days, sundays, vx_min, valid_shifts):
    invalid_cells = {}
    for emp in employees:
        emp_id = emp["ID"]
        for day in range(len(month_days)):
            shift = schedule.get(emp_id, [''] * len(month_days))[day]
            if shift:
                is_valid, errors = is_valid_schedule(schedule, emp_id, day, shift, month_days, sundays, vx_min)
                if not is_valid:
                    invalid_cells[(emp_id, day)] = errors
    
    # Thử điều chỉnh các ca không hợp lệ
    for (emp_id, day), errors in invalid_cells.items():
        logging.info(f"Điều chỉnh ca không hợp lệ cho {emp_id} ngày {day}: {errors}")
        current_shift = schedule[emp_id][day]
        if (emp_id, day) in st.session_state.manual_shifts:
            continue  # Bỏ qua ca thủ công
        for new_shift in valid_shifts:
            if new_shift == current_shift:
                continue
            is_valid, new_errors = is_valid_schedule(schedule, emp_id, day, new_shift, month_days, sundays, vx_min)
            if is_valid:
                schedule[emp_id][day] = new_shift
                logging.info(f"Đã thay {current_shift} bằng {new_shift} cho {emp_id} ngày {day}")
                break
        else:
            logging.warning(f"Không tìm được ca thay thế hợp lệ cho {emp_id} ngày {day}")
    
    # Kiểm tra lại lịch sau khi điều chỉnh
    invalid_cells = {}
    for emp in employees:
        emp_id = emp["ID"]
        for day in range(len(month_days)):
            shift = schedule.get(emp_id, [''] * len(month_days))[day]
            if shift:
                is_valid, errors = is_valid_schedule(schedule, emp_id, day, shift, month_days, sundays, vx_min)
                if not is_valid:
                    invalid_cells[(emp_id, day)] = errors
    
    return schedule, invalid_cells

# Hàm tạo lịch tự động sử dụng CP-SAT Solver
def auto_schedule(employees, month_days, sundays, vx_min, department_filter, balance_morning_evening, max_morning_evening_diff):
    start_time = time.time()
    logging.info(f"Bắt đầu tạo lịch với CP-SAT: {len(employees)} nhân viên, {len(month_days)} ngày, bộ phận: {department_filter}")
    
    if not employees:
        logging.error("Không có nhân viên để tạo lịch")
        return {}
    
    # Lọc nhân viên theo bộ phận
    if department_filter != "Tất cả":
        employees = [emp for emp in employees if emp["Bộ phận"] == department_filter]
    
    if not employees:
        logging.error(f"Không có nhân viên thuộc bộ phận {department_filter}")
        return {}
    
    # Kiểm tra tính khả thi
    is_feasible, reason = check_feasibility(employees, month_days, st.session_state.selected_shifts)
    if not is_feasible:
        logging.error(f"Kiểm tra tính khả thi thất bại: {reason}")
        st.error(f"Không thể tạo lịch: {reason}")
        return {}
    
    # Khởi tạo CP-SAT model
    model = cp_model.CpModel()
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 60.0  # Giới hạn thời gian 60 giây
    valid_shifts = [s for s in st.session_state.selected_shifts if s not in ["AL", "NPL"]]  # Loại bỏ "" và chỉ giữ PRD
    if not valid_shifts:
        logging.error("Danh sách ca hợp lệ rỗng")
        st.error("Danh sách ca hợp lệ rỗng. Vui lòng chọn ít nhất một mã ca!")
        return {}
    
    shift_to_idx = {shift: idx for idx, shift in enumerate(valid_shifts)}
    idx_to_shift = {idx: shift for shift, idx in shift_to_idx.items()}
    cs_employees = [emp for emp in employees if emp["Bộ phận"] == "Customer Service"]
    manual_shifts = st.session_state.get("manual_shifts", {})
    
    logging.info(f"Số ca khả dụng: {len(valid_shifts)}, số nhân viên CS: {len(cs_employees)}")
    
    # Thanh tiến trình
    progress_bar = st.progress(0)
    progress_text = st.empty()
    progress_text.text("Khởi tạo biến...")
    
    # Định nghĩa biến: variables[emp_id][day] là chỉ số ca được gán
    variables = {}
    total_steps = len(employees) * len(month_days) + len(employees) * (len(month_days) - 1) * 2 + len(employees) + len(month_days) + 10
    current_step = 0
    
    for emp in employees:
        emp_id = emp["ID"]
        variables[emp_id] = {}
        for day in range(len(month_days)):
            if (emp_id, day) in manual_shifts:
                shift = manual_shifts[(emp_id, day)]
                if shift not in valid_shifts and shift not in ["AL", "NPL"]:
                    logging.warning(f"Ca thủ công {shift} không hợp lệ cho {emp_id} ngày {day}")
                    continue
                variables[emp_id][day] = model.NewIntVar(shift_to_idx.get(shift, 0), shift_to_idx.get(shift, 0), f"{emp_id}_{day}")
            else:
                shift_pool = valid_shifts
                if emp["Cấp bậc"] in ["Senior", "Manager"]:
                    morning_shifts = [s for s in st.session_state.selected_shifts if get_shift_start_hour(s) < 12]
                    if morning_shifts:
                        shift_pool = morning_shifts + ["PRD"]
                variables[emp_id][day] = model.NewIntVar(0, len(valid_shifts) - 1, f"{emp_id}_{day}")
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc giãn cách 12 giờ...")
    
    # Ràng buộc giãn cách 12 giờ
    for emp in employees:
        emp_id = emp["ID"]
        for day in range(1, len(month_days)):
            current_var = variables[emp_id][day]
            prev_var = variables[emp_id][day - 1]
            for prev_idx in range(len(valid_shifts)):
                if prev_idx not in idx_to_shift:
                    logging.warning(f"Chỉ số {prev_idx} không tồn tại trong idx_to_shift")
                    continue
                prev_shift = idx_to_shift[prev_idx]
                if prev_shift in ["PRD", "AL", "NPL"]:
                    continue
                prev_end = get_shift_end_hour(prev_shift)
                if prev_end is None:
                    continue
                prev_end_hour = prev_end % 24
                prev_end_day_offset = 1 if prev_end >= 24 else 0
                prev_end_minutes = int((prev_end % 1) * 60)
                for current_idx in range(len(valid_shifts)):
                    if current_idx not in idx_to_shift:
                        logging.warning(f"Chỉ số {current_idx} không tồn tại trong idx_to_shift")
                        continue
                    current_shift = idx_to_shift[current_idx]
                    if current_shift in ["PRD", "AL", "NPL"]:
                        continue
                    current_start = get_shift_start_hour(current_shift)
                    if current_start is None:
                        continue
                    current_start_hour = int(current_start)
                    current_start_minutes = int((current_start % 1) * 60)
                    current_time = month_days[day].replace(hour=current_start_hour, minute=current_start_minutes)
                    prev_time = month_days[day - 1 + prev_end_day_offset].replace(hour=int(prev_end_hour), minute=prev_end_minutes)
                    time_diff = (current_time - prev_time).total_seconds() / 3600
                    if time_diff < 12:
                        not_prev_var = model.NewBoolVar(f"not_prev_{emp_id}_{day-1}_{prev_idx}")
                        model.Add(prev_var != prev_idx).OnlyEnforceIf(not_prev_var)
                        model.Add(prev_var == prev_idx).OnlyEnforceIf(not_prev_var.Not())
                        is_current = model.NewBoolVar(f"current_{emp_id}_{day}_{current_idx}")
                        model.Add(current_var == current_idx).OnlyEnforceIf(is_current)
                        model.Add(current_var != current_idx).OnlyEnforceIf(is_current.Not())
                        model.AddBoolOr([not_prev_var]).OnlyEnforceIf(is_current)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc không PRD/VX/V6 liên tiếp...")
    
    # Ràng buộc không PRD/VX/V6 liên tiếp
    for emp in employees:
        emp_id = emp["ID"]
        for day in range(1, len(month_days)):
            current_var = variables[emp_id][day]
            prev_var = variables[emp_id][day - 1]
            prd_idx = shift_to_idx.get("PRD")
            vx_indices = [shift_to_idx[s] for s in valid_shifts if s.startswith("VX")]
            v6_indices = [shift_to_idx[s] for s in valid_shifts if s.startswith("V6")]
            if prd_idx is not None:
                is_prd = model.NewBoolVar(f"is_prd_{emp_id}_{day}")
                model.Add(current_var == prd_idx).OnlyEnforceIf(is_prd)
                model.Add(current_var != prd_idx).OnlyEnforceIf(is_prd.Not())
                prev_is_prd = model.NewBoolVar(f"prev_is_prd_{emp_id}_{day-1}")
                model.Add(prev_var == prd_idx).OnlyEnforceIf(prev_is_prd)
                model.Add(prev_var != prd_idx).OnlyEnforceIf(prev_is_prd.Not())
                model.AddBoolOr([prev_is_prd.Not()]).OnlyEnforceIf(is_prd)
            if vx_indices:
                vx_bools = [
                    model.NewBoolVar(f"vx_bool_{emp_id}_{day}_{idx}")
                    for idx in vx_indices
                ]
                for i, idx in enumerate(vx_indices):
                    model.Add(current_var == idx).OnlyEnforceIf(vx_bools[i])
                    model.Add(current_var != idx).OnlyEnforceIf(vx_bools[i].Not())
                is_vx = model.NewBoolVar(f"is_vx_{emp_id}_{day}")
                model.AddBoolOr(vx_bools).OnlyEnforceIf(is_vx)
                model.AddBoolOr([vb.Not() for vb in vx_bools]).OnlyEnforceIf(is_vx.Not())
                prev_not_vx_bools = [
                    model.NewBoolVar(f"prev_not_vx_{emp_id}_{day-1}_{i}")
                    for i in range(len(valid_shifts)) if i in idx_to_shift and i not in vx_indices
                ]
                for i, idx in enumerate([i for i in range(len(valid_shifts)) if i in idx_to_shift and i not in vx_indices]):
                    model.Add(prev_var == idx).OnlyEnforceIf(prev_not_vx_bools[i])
                    model.Add(prev_var != idx).OnlyEnforceIf(prev_not_vx_bools[i].Not())
                if prev_not_vx_bools:
                    model.AddBoolOr(prev_not_vx_bools).OnlyEnforceIf(is_vx)
            if v6_indices:
                v6_bools = [
                    model.NewBoolVar(f"v6_bool_{emp_id}_{day}_{idx}")
                    for idx in v6_indices
                ]
                for i, idx in enumerate(v6_indices):
                    model.Add(current_var == idx).OnlyEnforceIf(v6_bools[i])
                    model.Add(current_var != idx).OnlyEnforceIf(v6_bools[i].Not())
                is_v6 = model.NewBoolVar(f"is_v6_{emp_id}_{day}")
                model.AddBoolOr(v6_bools).OnlyEnforceIf(is_v6)
                model.AddBoolOr([vb.Not() for vb in v6_bools]).OnlyEnforceIf(is_v6.Not())
                prev_not_v6_bools = [
                    model.NewBoolVar(f"prev_not_v6_{emp_id}_{day-1}_{i}")
                    for i in range(len(valid_shifts)) if i in idx_to_shift and i not in v6_indices
                ]
                for i, idx in enumerate([i for i in range(len(valid_shifts)) if i in idx_to_shift and i not in v6_indices]):
                    model.Add(prev_var == idx).OnlyEnforceIf(prev_not_v6_bools[i])
                    model.Add(prev_var != idx).OnlyEnforceIf(prev_not_v6_bools[i].Not())
                if prev_not_v6_bools:
                    model.AddBoolOr(prev_not_v6_bools).OnlyEnforceIf(is_v6)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc không quá 7 ngày làm liên tục...")
    
    # Ràng buộc không quá 7 ngày làm liên tục
    prd_idx = shift_to_idx.get("PRD")
    for emp in employees:
        emp_id = emp["ID"]
        for day in range(7, len(month_days)):
            vars_window = [variables[emp_id][d] for d in range(day - 7, day + 1)]
            work_days = [model.NewBoolVar(f"work_{emp_id}_{d}") for d in range(day - 7, day + 1)]
            for i, var in enumerate(vars_window):
                if prd_idx is not None:
                    model.Add(var != prd_idx).OnlyEnforceIf(work_days[i])
                    model.Add(var == prd_idx).OnlyEnforceIf(work_days[i].Not())
            model.Add(sum(work_days) <= 7)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc không PRD vào thứ 7, chủ nhật, ngày lễ...")
    
    # Tạo danh sách các ngày không cho phép PRD
    non_prd_days = [
        day for day in range(len(month_days))
        if (month_days[day].weekday() in [5, 6] or is_holiday(month_days[day]))
    ]
    non_prd_days_no_saturday = [
        day for day in range(len(month_days))
        if (month_days[day].weekday() == 6 or is_holiday(month_days[day]))
    ]
    if prd_idx is not None:
        for emp in employees:
            emp_id = emp["ID"]
            for day in non_prd_days_no_saturday:
                if (emp_id, day) not in manual_shifts:
                    var = variables[emp_id][day]
                    model.Add(var != prd_idx)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc số ngày PRD bằng số ngày Chủ nhật...")
    
    # Ràng buộc số ngày PRD bằng số ngày Chủ nhật
    if prd_idx is not None:
        for emp in employees:
            emp_id = emp["ID"]
            vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
            prd_count = sum(model.NewBoolVar(f"prd_{emp_id}_{day}") for day in range(len(month_days)))
            for day in range(len(month_days)):
                model.Add(variables[emp_id][day] == prd_idx).OnlyEnforceIf(model.NewBoolVar(f"prd_{emp_id}_{day}"))
                model.Add(variables[emp_id][day] != prd_idx).OnlyEnforceIf(model.NewBoolVar(f"prd_{emp_id}_{day}").Not())
            model.Add(prd_count == len(sundays))
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc VX = V6 và tối thiểu VX...")
    
    # Ràng buộc VX = V6 và tối thiểu VX
    for emp in employees:
        emp_id = emp["ID"]
        vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
        vx_indices = [shift_to_idx[s] for s in valid_shifts if s.startswith("VX")]
        v6_indices = [shift_to_idx[s] for s in valid_shifts if s.startswith("V6")]
        vx_count = sum(model.NewBoolVar(f"vx_{emp_id}_{day}") for day in range(len(month_days)))
        v6_count = sum(model.NewBoolVar(f"v6_{emp_id}_{day}") for day in range(len(month_days)))
        for day in range(len(month_days)):
            if vx_indices:
                vx_bools = [
                    model.NewBoolVar(f"vx_bool_{emp_id}_{day}_{idx}")
                    for idx in vx_indices
                ]
                for i, idx in enumerate(vx_indices):
                    model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(vx_bools[i])
                    model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(vx_bools[i].Not())
                model.AddBoolOr(vx_bools).OnlyEnforceIf(model.NewBoolVar(f"vx_{emp_id}_{day}"))
                model.AddBoolOr([vb.Not() for vb in vx_bools]).OnlyEnforceIf(model.NewBoolVar(f"vx_{emp_id}_{day}").Not())
            if v6_indices:
                v6_bools = [
                    model.NewBoolVar(f"v6_bool_{emp_id}_{day}_{idx}")
                    for idx in v6_indices
                ]
                for i, idx in enumerate(v6_indices):
                    model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(v6_bools[i])
                    model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(v6_bools[i].Not())
                model.AddBoolOr(v6_bools).OnlyEnforceIf(model.NewBoolVar(f"v6_{emp_id}_{day}"))
                model.AddBoolOr([vb.Not() for vb in v6_bools]).OnlyEnforceIf(model.NewBoolVar(f"v6_{emp_id}_{day}").Not())
        model.Add(vx_count == v6_count)
        model.Add(vx_count >= vx_min)
        current_step += 1
        progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc ưu tiên V633 cho Customer Service...")
    
    # Ràng buộc ưu tiên V633 cho nhân viên Customer Service
    v633_idx = shift_to_idx.get("V633")
    if v633_idx is not None:
        for emp in cs_employees:
            emp_id = emp["ID"]
            vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
            v633_count = sum(model.NewBoolVar(f"v633_{emp_id}_{day}") for day in range(len(month_days)))
            for day in range(len(month_days)):
                model.Add(variables[emp_id][day] == v633_idx).OnlyEnforceIf(model.NewBoolVar(f"v633_{emp_id}_{day}"))
                model.Add(variables[emp_id][day] != v633_idx).OnlyEnforceIf(model.NewBoolVar(f"v633_{emp_id}_{day}").Not())
            model.Add(v633_count >= 1)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc ca bắt buộc Customer Service...")
    
    # Ràng buộc ca bắt buộc Customer Service
    for day in range(len(month_days)):
        cs_vars = [variables[emp["ID"]][day] for emp in cs_employees]
        v814_v614_indices = [shift_to_idx.get(s) for s in ["V814", "V614"] if s in shift_to_idx]
        v818_v618_indices = [shift_to_idx.get(s) for s in ["V818", "V618"] if s in shift_to_idx]
        v829_v633_indices = [shift_to_idx.get(s) for s in ["V829", "V633"] if s in shift_to_idx]
        v633_idx = shift_to_idx.get("V633")
        
        if v814_v614_indices:
            v814_v614_bools = []
            for emp in cs_employees:
                for idx in v814_v614_indices:
                    if idx is not None:
                        is_shift = model.NewBoolVar(f"v814_v614_{emp['ID']}_{day}_{idx}")
                        model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                        model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                        v814_v614_bools.append(is_shift)
            if v814_v614_bools:
                model.Add(sum(v814_v614_bools) == 1)
        if v818_v618_indices:
            v818_v618_bools = []
            for emp in cs_employees:
                for idx in v818_v618_indices:
                    if idx is not None:
                        is_shift = model.NewBoolVar(f"v818_v618_{emp['ID']}_{day}_{idx}")
                        model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                        model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                        v818_v618_bools.append(is_shift)
            if v818_v618_bools:
                model.Add(sum(v818_v618_bools) == 1)
        if v829_v633_indices:
            v829_v633_bools = []
            for emp in cs_employees:
                for idx in v829_v633_indices:
                    if idx is not None:
                        is_shift = model.NewBoolVar(f"v829_v633_{emp['ID']}_{day}_{idx}")
                        model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                        model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                        v829_v633_bools.append(is_shift)
            if v829_v633_bools:
                model.Add(sum(v829_v633_bools) == 2)
        if v633_idx is not None:
            v633_bools = []
            for emp in cs_employees:
                is_shift = model.NewBoolVar(f"v633_{emp['ID']}_{day}")
                model.Add(variables[emp["ID"]][day] == v633_idx).OnlyEnforceIf(is_shift)
                model.Add(variables[emp["ID"]][day] != v633_idx).OnlyEnforceIf(is_shift.Not())
                v633_bools.append(is_shift)
            if v633_bools:
                model.Add(sum(v633_bools) <= 1)
        current_step += 1
        progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Thêm ràng buộc số ca PRD mỗi ngày...")
    
    # Tính toán số ca PRD tối đa mỗi ngày
    total_prd = len(sundays) * len(employees)
    manual_prd_count = sum(1 for (emp_id, day), shift in manual_shifts.items() if shift == "PRD")
    non_prd_days_count = sum(1 for day in range(len(month_days)) if (month_days[day].weekday() == 6 or is_holiday(month_days[day])))
    prd_possible_days = len(month_days) - non_prd_days_count
    max_prd_per_day = math.ceil((total_prd - manual_prd_count) / max(prd_possible_days, 1)) if prd_possible_days > 0 else 0
    logging.info(f"Tổng số PRD: {total_prd}, PRD nhập tay: {manual_prd_count}, Ngày có thể sắp PRD: {prd_possible_days}, Max PRD/ngày: {max_prd_per_day}")
    
    # Thêm ràng buộc số ca PRD mỗi ngày
    if prd_idx is not None:
        for day in range(len(month_days)):
            if month_days[day].weekday() == 5:  # Thứ 7
                continue
            day_vars = [variables[emp["ID"]][day] for emp in employees]
            prd_bools = []
            for emp in employees:
                is_prd = model.NewBoolVar(f"prd_day_{emp['ID']}_{day}")
                model.Add(variables[emp["ID"]][day] == prd_idx).OnlyEnforceIf(is_prd)
                model.Add(variables[emp["ID"]][day] != prd_idx).OnlyEnforceIf(is_prd.Not())
                prd_bools.append(is_prd)
            model.Add(sum(prd_bools) <= max_prd_per_day)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    # Tối ưu hóa: Cân bằng ca Sáng-Tối
    if balance_morning_evening:
        progress_text.text("Thêm ràng buộc cân bằng ca Sáng-Tối...")
        for emp in employees:
            emp_id = emp["ID"]
            vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
            morning_indices = [shift_to_idx[s] for s in valid_shifts if s not in ["PRD", "AL", "NPL"] and get_shift_start_hour(s) < 12]
            evening_indices = [shift_to_idx[s] for s in valid_shifts if s not in ["PRD", "AL", "NPL"] and get_shift_start_hour(s) >= 12]
            morning_bools = []
            evening_bools = []
            for day in range(len(month_days)):
                for idx in morning_indices:
                    is_morning = model.NewBoolVar(f"morning_{emp_id}_{day}_{idx}")
                    model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(is_morning)
                    model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(is_morning.Not())
                    morning_bools.append(is_morning)
                for idx in evening_indices:
                    is_evening = model.NewBoolVar(f"evening_{emp_id}_{day}_{idx}")
                    model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(is_evening)
                    model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(is_evening.Not())
                    evening_bools.append(is_evening)
            morning_count = sum(morning_bools)
            evening_count = sum(evening_bools)
            diff = model.NewIntVar(-len(month_days), len(month_days), f"diff_{emp_id}")
            model.Add(diff == morning_count - evening_count)
            model.AddAbsEquality(max_morning_evening_diff, diff)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
    
    progress_text.text("Giải bài toán CP-SAT...")
    
    # Giải bài toán
    status = solver.Solve(model)
    elapsed_time = time.time() - start_time
    logging.info(f"Kết thúc giải bài toán CP-SAT. Trạng thái: {solver.StatusName(status)}, thời gian: {elapsed_time:.2f} giây")
    
    progress_bar.progress(1.0)
    progress_text.text(f"Hoàn tất! Trạng thái: {solver.StatusName(status)} trong {elapsed_time:.2f} giây")
    
    if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:
        schedule = {emp["ID"]: ["PRD"] * len(month_days) for emp in employees}  # Khởi tạo mặc định bằng PRD
        for emp in employees:
            emp_id = emp["ID"]
            for day in range(len(month_days)):
                shift_idx = solver.Value(variables[emp_id][day])
                if shift_idx in idx_to_shift:
                    schedule[emp_id][day] = idx_to_shift[shift_idx]
                else:
                    logging.warning(f"Chỉ số ca {shift_idx} không hợp lệ cho {emp_id} ngày {day}")
                    schedule[emp_id][day] = "PRD"  # Gán PRD nếu ca không hợp lệ
        
        current_schedule = st.session_state.get("schedule", {})
        for emp_id in current_schedule:
            if emp_id not in schedule:
                schedule[emp_id] = current_schedule[emp_id]
        
        # Kiểm tra và điều chỉnh ca không hợp lệ
        progress_text.text("Kiểm tra và điều chỉnh ca không hợp lệ...")
        schedule, invalid_cells = adjust_invalid_shifts(schedule, employees, month_days, sundays, vx_min, valid_shifts)
        if invalid_cells:
            st.warning(f"Còn {len(invalid_cells)} ca không hợp lệ sau khi điều chỉnh. Vui lòng kiểm tra lịch.")
            for (emp_id, day), errors in invalid_cells.items():
                logging.warning(f"Ca không hợp lệ còn lại: {emp_id} ngày {month_days[day].strftime('%d/%m')} - {schedule[emp_id][day]}. Lý do: {'; '.join(errors)}")
        
        save_schedule_to_db(schedule, month_days)
        logging.info("Lịch đã được lưu vào DB")
        return schedule
    else:
        # Thử lại mà không áp dụng ràng buộc PRD cho thứ 7
        logging.warning("Không tìm được lịch. Thử lại mà không hạn chế PRD vào thứ 7...")
        st.warning("Không tìm được lịch với ràng buộc PRD. Thử lại mà không hạn chế PRD vào thứ 7...")
        progress_bar.progress(0)
        progress_text.text("Thử lại mà không hạn chế PRD vào thứ 7...")
        current_step = 0
        
        model = cp_model.CpModel()
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 60.0
        
        variables = {}
        for emp in employees:
            emp_id = emp["ID"]
            variables[emp_id] = {}
            for day in range(len(month_days)):
                if (emp_id, day) in manual_shifts:
                    shift = manual_shifts[(emp_id, day)]
                    if shift not in valid_shifts and shift not in ["AL", "NPL"]:
                        logging.warning(f"Ca thủ công {shift} không hợp lệ cho {emp_id} ngày {day}")
                        continue
                    variables[emp_id][day] = model.NewIntVar(shift_to_idx.get(shift, 0), shift_to_idx.get(shift, 0), f"{emp_id}_{day}")
                else:
                    shift_pool = valid_shifts
                    if emp["Cấp bậc"] in ["Senior", "Manager"]:
                        morning_shifts = [s for s in st.session_state.selected_shifts if get_shift_start_hour(s) < 12]
                        if morning_shifts:
                            shift_pool = morning_shifts + ["PRD"]
                    variables[emp_id][day] = model.NewIntVar(0, len(valid_shifts) - 1, f"{emp_id}_{day}")
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc giãn cách 12 giờ (thử lại)...")
        for emp in employees:
            emp_id = emp["ID"]
            for day in range(1, len(month_days)):
                current_var = variables[emp_id][day]
                prev_var = variables[emp_id][day - 1]
                for prev_idx in range(len(valid_shifts)):
                    if prev_idx not in idx_to_shift:
                        logging.warning(f"Chỉ số {prev_idx} không tồn tại trong idx_to_shift")
                        continue
                    prev_shift = idx_to_shift[prev_idx]
                    if prev_shift in ["PRD", "AL", "NPL"]:
                        continue
                    prev_end = get_shift_end_hour(prev_shift)
                    if prev_end is None:
                        continue
                    prev_end_hour = prev_end % 24
                    prev_end_day_offset = 1 if prev_end >= 24 else 0
                    prev_end_minutes = int((prev_end % 1) * 60)
                    for current_idx in range(len(valid_shifts)):
                        if current_idx not in idx_to_shift:
                            logging.warning(f"Chỉ số {current_idx} không tồn tại trong idx_to_shift")
                            continue
                        current_shift = idx_to_shift[current_idx]
                        if current_shift in ["PRD", "AL", "NPL"]:
                            continue
                        current_start = get_shift_start_hour(current_shift)
                        if current_start is None:
                            continue
                        current_start_hour = int(current_start)
                        current_start_minutes = int((current_start % 1) * 60)
                        current_time = month_days[day].replace(hour=current_start_hour, minute=current_start_minutes)
                        prev_time = month_days[day - 1 + prev_end_day_offset].replace(hour=int(prev_end_hour), minute=prev_end_minutes)
                        time_diff = (current_time - prev_time).total_seconds() / 3600
                        if time_diff < 12:
                            not_prev_var = model.NewBoolVar(f"not_prev_{emp_id}_{day-1}_{prev_idx}")
                            model.Add(prev_var != prev_idx).OnlyEnforceIf(not_prev_var)
                            model.Add(prev_var == prev_idx).OnlyEnforceIf(not_prev_var.Not())
                            is_current = model.NewBoolVar(f"current_{emp_id}_{day}_{current_idx}")
                            model.Add(current_var == current_idx).OnlyEnforceIf(is_current)
                            model.Add(current_var != current_idx).OnlyEnforceIf(is_current.Not())
                            model.AddBoolOr([not_prev_var]).OnlyEnforceIf(is_current)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc không PRD/VX/V6 liên tiếp (thử lại)...")
        for emp in employees:
            emp_id = emp["ID"]
            for day in range(1, len(month_days)):
                current_var = variables[emp_id][day]
                prev_var = variables[emp_id][day - 1]
                if prd_idx is not None:
                    is_prd = model.NewBoolVar(f"is_prd_{emp_id}_{day}")
                    model.Add(current_var == prd_idx).OnlyEnforceIf(is_prd)
                    model.Add(current_var != prd_idx).OnlyEnforceIf(is_prd.Not())
                    prev_is_prd = model.NewBoolVar(f"prev_is_prd_{emp_id}_{day-1}")
                    model.Add(prev_var == prd_idx).OnlyEnforceIf(prev_is_prd)
                    model.Add(prev_var != prd_idx).OnlyEnforceIf(prev_is_prd.Not())
                    model.AddBoolOr([prev_is_prd.Not()]).OnlyEnforceIf(is_prd)
                if vx_indices:
                    vx_bools = [
                        model.NewBoolVar(f"vx_bool_{emp_id}_{day}_{idx}")
                        for idx in vx_indices
                    ]
                    for i, idx in enumerate(vx_indices):
                        model.Add(current_var == idx).OnlyEnforceIf(vx_bools[i])
                        model.Add(current_var != idx).OnlyEnforceIf(vx_bools[i].Not())
                    is_vx = model.NewBoolVar(f"is_vx_{emp_id}_{day}")
                    model.AddBoolOr(vx_bools).OnlyEnforceIf(is_vx)
                    model.AddBoolOr([vb.Not() for vb in vx_bools]).OnlyEnforceIf(is_vx.Not())
                    prev_not_vx_bools = [
                        model.NewBoolVar(f"prev_not_vx_{emp_id}_{day-1}_{i}")
                        for i in range(len(valid_shifts)) if i in idx_to_shift and i not in vx_indices
                    ]
                    for i, idx in enumerate([i for i in range(len(valid_shifts)) if i in idx_to_shift and i not in vx_indices]):
                        model.Add(prev_var == idx).OnlyEnforceIf(prev_not_vx_bools[i])
                        model.Add(prev_var != idx).OnlyEnforceIf(prev_not_vx_bools[i].Not())
                    if prev_not_vx_bools:
                        model.AddBoolOr(prev_not_vx_bools).OnlyEnforceIf(is_vx)
                if v6_indices:
                    v6_bools = [
                        model.NewBoolVar(f"v6_bool_{emp_id}_{day}_{idx}")
                        for idx in v6_indices
                    ]
                    for i, idx in enumerate(v6_indices):
                        model.Add(current_var == idx).OnlyEnforceIf(v6_bools[i])
                        model.Add(current_var != idx).OnlyEnforceIf(v6_bools[i].Not())
                    is_v6 = model.NewBoolVar(f"is_v6_{emp_id}_{day}")
                    model.AddBoolOr(v6_bools).OnlyEnforceIf(is_v6)
                    model.AddBoolOr([vb.Not() for vb in v6_bools]).OnlyEnforceIf(is_v6.Not())
                    prev_not_v6_bools = [
                        model.NewBoolVar(f"prev_not_v6_{emp_id}_{day-1}_{i}")
                        for i in range(len(valid_shifts)) if i in idx_to_shift and i not in v6_indices
                    ]
                    for i, idx in enumerate([i for i in range(len(valid_shifts)) if i in idx_to_shift and i not in v6_indices]):
                        model.Add(prev_var == idx).OnlyEnforceIf(prev_not_v6_bools[i])
                        model.Add(prev_var != idx).OnlyEnforceIf(prev_not_v6_bools[i].Not())
                    if prev_not_v6_bools:
                        model.AddBoolOr(prev_not_v6_bools).OnlyEnforceIf(is_v6)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc không quá 7 ngày làm liên tục (thử lại)...")
        for emp in employees:
            emp_id = emp["ID"]
            for day in range(7, len(month_days)):
                vars_window = [variables[emp_id][d] for d in range(day - 7, day + 1)]
                work_days = [model.NewBoolVar(f"work_{emp_id}_{d}") for d in range(day - 7, day + 1)]
                for i, var in enumerate(vars_window):
                    if prd_idx is not None:
                        model.Add(var != prd_idx).OnlyEnforceIf(work_days[i])
                        model.Add(var == prd_idx).OnlyEnforceIf(work_days[i].Not())
                model.Add(sum(work_days) <= 7)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc không PRD vào chủ nhật, ngày lễ (thử lại)...")
        if prd_idx is not None:
            for emp in employees:
                emp_id = emp["ID"]
                for day in [d for d in range(len(month_days)) if (month_days[d].weekday() == 6 or is_holiday(month_days[d]))]:
                    if (emp_id, day) not in manual_shifts:
                        var = variables[emp_id][day]
                        model.Add(var != prd_idx)
                    current_step += 1
                    progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc số ngày PRD bằng số ngày Chủ nhật (thử lại)...")
        if prd_idx is not None:
            for emp in employees:
                emp_id = emp["ID"]
                vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
                prd_count = sum(model.NewBoolVar(f"prd_{emp_id}_{day}") for day in range(len(month_days)))
                for day in range(len(month_days)):
                    model.Add(variables[emp_id][day] == prd_idx).OnlyEnforceIf(model.NewBoolVar(f"prd_{emp_id}_{day}"))
                    model.Add(variables[emp_id][day] != prd_idx).OnlyEnforceIf(model.NewBoolVar(f"prd_{emp_id}_{day}").Not())
                model.Add(prd_count == len(sundays))
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc VX = V6 và tối thiểu VX (thử lại)...")
        for emp in employees:
            emp_id = emp["ID"]
            vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
            vx_count = sum(model.NewBoolVar(f"vx_{emp_id}_{day}") for day in range(len(month_days)))
            v6_count = sum(model.NewBoolVar(f"v6_{emp_id}_{day}") for day in range(len(month_days)))
            for day in range(len(month_days)):
                if vx_indices:
                    vx_bools = [
                        model.NewBoolVar(f"vx_bool_{emp_id}_{day}_{idx}")
                        for idx in vx_indices
                    ]
                    for i, idx in enumerate(vx_indices):
                        model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(vx_bools[i])
                        model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(vx_bools[i].Not())
                    model.AddBoolOr(vx_bools).OnlyEnforceIf(model.NewBoolVar(f"vx_{emp_id}_{day}"))
                    model.AddBoolOr([vb.Not() for vb in vx_bools]).OnlyEnforceIf(model.NewBoolVar(f"vx_{emp_id}_{day}").Not())
                if v6_indices:
                    v6_bools = [
                        model.NewBoolVar(f"v6_bool_{emp_id}_{day}_{idx}")
                        for idx in v6_indices
                    ]
                    for i, idx in enumerate(v6_indices):
                        model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(v6_bools[i])
                        model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(v6_bools[i].Not())
                    model.AddBoolOr(v6_bools).OnlyEnforceIf(model.NewBoolVar(f"v6_{emp_id}_{day}"))
                    model.AddBoolOr([vb.Not() for vb in v6_bools]).OnlyEnforceIf(model.NewBoolVar(f"v6_{emp_id}_{day}").Not())
            model.Add(vx_count == v6_count)
            model.Add(vx_count >= vx_min)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc ưu tiên V633 cho Customer Service (thử lại)...")
        if v633_idx is not None:
            for emp in cs_employees:
                emp_id = emp["ID"]
                vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
                v633_count = sum(model.NewBoolVar(f"v633_{emp_id}_{day}") for day in range(len(month_days)))
                for day in range(len(month_days)):
                    model.Add(variables[emp_id][day] == v633_idx).OnlyEnforceIf(model.NewBoolVar(f"v633_{emp_id}_{day}"))
                    model.Add(variables[emp_id][day] != v633_idx).OnlyEnforceIf(model.NewBoolVar(f"v633_{emp_id}_{day}").Not())
                model.Add(v633_count >= 1)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc ca bắt buộc Customer Service (thử lại)...")
        for day in range(len(month_days)):
            cs_vars = [variables[emp["ID"]][day] for emp in cs_employees]
            if v814_v614_indices:
                v814_v614_bools = []
                for emp in cs_employees:
                    for idx in v814_v614_indices:
                        if idx is not None:
                            is_shift = model.NewBoolVar(f"v814_v614_{emp['ID']}_{day}_{idx}")
                            model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                            model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                            v814_v614_bools.append(is_shift)
                if v814_v614_bools:
                    model.Add(sum(v814_v614_bools) == 1)
            if v818_v618_indices:
                v818_v618_bools = []
                for emp in cs_employees:
                    for idx in v818_v618_indices:
                        if idx is not None:
                            is_shift = model.NewBoolVar(f"v818_v618_{emp['ID']}_{day}_{idx}")
                            model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                            model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                            v818_v618_bools.append(is_shift)
                if v818_v618_bools:
                    model.Add(sum(v818_v618_bools) == 1)
            if v829_v633_indices:
                v829_v633_bools = []
                for emp in cs_employees:
                    for idx in v829_v633_indices:
                        if idx is not None:
                            is_shift = model.NewBoolVar(f"v829_v633_{emp['ID']}_{day}_{idx}")
                            model.Add(variables[emp["ID"]][day] == idx).OnlyEnforceIf(is_shift)
                            model.Add(variables[emp["ID"]][day] != idx).OnlyEnforceIf(is_shift.Not())
                            v829_v633_bools.append(is_shift)
                if v829_v633_bools:
                    model.Add(sum(v829_v633_bools) == 2)
            if v633_idx is not None:
                v633_bools = []
                for emp in cs_employees:
                    is_shift = model.NewBoolVar(f"v633_{emp['ID']}_{day}")
                    model.Add(variables[emp["ID"]][day] == v633_idx).OnlyEnforceIf(is_shift)
                    model.Add(variables[emp["ID"]][day] != v633_idx).OnlyEnforceIf(is_shift.Not())
                    v633_bools.append(is_shift)
                if v633_bools:
                    model.Add(sum(v633_bools) <= 1)
            current_step += 1
            progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Thêm ràng buộc số ca PRD mỗi ngày (thử lại)...")
        if prd_idx is not None:
            for day in range(len(month_days)):
                if month_days[day].weekday() == 6 or is_holiday(month_days[day]):
                    continue
                day_vars = [variables[emp["ID"]][day] for emp in employees]
                prd_bools = []
                for emp in employees:
                    is_prd = model.NewBoolVar(f"prd_day_{emp['ID']}_{day}")
                    model.Add(variables[emp["ID"]][day] == prd_idx).OnlyEnforceIf(is_prd)
                    model.Add(variables[emp["ID"]][day] != prd_idx).OnlyEnforceIf(is_prd.Not())
                    prd_bools.append(is_prd)
                model.Add(sum(prd_bools) <= max_prd_per_day)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        if balance_morning_evening:
            progress_text.text("Thêm ràng buộc cân bằng ca Sáng-Tối (thử lại)...")
            for emp in employees:
                emp_id = emp["ID"]
                vars_emp = [variables[emp_id][day] for day in range(len(month_days))]
                morning_bools = []
                evening_bools = []
                for day in range(len(month_days)):
                    for idx in morning_indices:
                        is_morning = model.NewBoolVar(f"morning_{emp_id}_{day}_{idx}")
                        model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(is_morning)
                        model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(is_morning.Not())
                        morning_bools.append(is_morning)
                    for idx in evening_indices:
                        is_evening = model.NewBoolVar(f"evening_{emp_id}_{day}_{idx}")
                        model.Add(variables[emp_id][day] == idx).OnlyEnforceIf(is_evening)
                        model.Add(variables[emp_id][day] != idx).OnlyEnforceIf(is_evening.Not())
                        evening_bools.append(is_evening)
                morning_count = sum(morning_bools)
                evening_count = sum(evening_bools)
                diff = model.NewIntVar(-len(month_days), len(month_days), f"diff_{emp_id}")
                model.Add(diff == morning_count - evening_count)
                model.AddAbsEquality(max_morning_evening_diff, diff)
                current_step += 1
                progress_bar.progress(min(current_step / total_steps, 0.99))
        
        progress_text.text("Giải bài toán CP-SAT (thử lại)...")
        status = solver.Solve(model)
        elapsed_time = time.time() - start_time
        logging.info(f"Kết thúc thử lại. Trạng thái: {solver.StatusName(status)}, thời gian: {elapsed_time:.2f} giây")
        
        progress_bar.progress(1.0)
        progress_text.text(f"Hoàn tất thử lại! Trạng thái: {solver.StatusName(status)} trong {elapsed_time:.2f} giây")
        
        if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:
            schedule = {emp["ID"]: ["PRD"] * len(month_days) for emp in employees}
            for emp in employees:
                emp_id = emp["ID"]
                for day in range(len(month_days)):
                    shift_idx = solver.Value(variables[emp_id][day])
                    if shift_idx in idx_to_shift:
                        schedule[emp_id][day] = idx_to_shift[shift_idx]
                    else:
                        logging.warning(f"Chỉ số ca {shift_idx} không hợp lệ cho {emp_id} ngày {day}")
                        schedule[emp_id][day] = "PRD"
            
            current_schedule = st.session_state.get("schedule", {})
            for emp_id in current_schedule:
                if emp_id not in schedule:
                    schedule[emp_id] = current_schedule[emp_id]
            
            # Kiểm tra và điều chỉnh ca không hợp lệ
            progress_text.text("Kiểm tra và điều chỉnh ca không hợp lệ (thử lại)...")
            schedule, invalid_cells = adjust_invalid_shifts(schedule, employees, month_days, sundays, vx_min, valid_shifts)
            if invalid_cells:
                st.warning(f"Còn {len(invalid_cells)} ca không hợp lệ sau khi điều chỉnh. Vui lòng kiểm tra lịch.")
                for (emp_id, day), errors in invalid_cells.items():
                    logging.warning(f"Ca không hợp lệ còn lại: {emp_id} ngày {month_days[day].strftime('%d/%m')} - {schedule[emp_id][day]}. Lý do: {'; '.join(errors)}")
            
            save_schedule_to_db(schedule, month_days)
            logging.info("Lịch thử lại đã được lưu vào DB")
            return schedule
        
        errors = []
        if len(cs_employees) < 3:
            errors.append("Cần ít nhất 3 nhân viên Customer Service.")
        if not all(s in st.session_state.selected_shifts for s in ["V814", "V614", "V818", "V618", "V829", "V633"]):
            missing = [s for s in ["V814", "V614", "V818", "V618", "V829", "V633"] if s not in st.session_state.selected_shifts]
            errors.append(f"Thiếu ca bắt buộc: {', '.join(missing)}")
        if not st.session_state.selected_shifts:
            errors.append("Danh sách ca trống.")
        if not any(s for s in st.session_state.selected_shifts if get_shift_start_hour(s) < 12):
            errors.append("Thiếu ca Sáng (bắt đầu trước 12h).")
        if not any(s for s in st.session_state.selected_shifts if get_shift_start_hour(s) >= 12):
            errors.append("Thiếu ca Tối (bắt đầu từ 12h trở đi).")
        sundays_count = len(sundays)
        if len(cs_employees) * sundays_count > len([s for s in st.session_state.selected_shifts if s == "PRD"]):
            errors.append(f"Không đủ ca PRD để phân bổ {sundays_count} ngày Chủ nhật cho {len(cs_employees)} nhân viên")
        st.error(f"Không tìm được lịch thỏa mãn tất cả ràng buộc! Lý do có thể bao gồm: {'; '.join(errors)}")
        logging.error(f"Không tìm được lịch. Lý do: {'; '.join(errors)}")
        return {}

# Khởi tạo trạng thái phiên
if "employees" not in st.session_state:
    st.session_state.employees = load_employees_from_db()
if "schedule" not in st.session_state:
    st.session_state.schedule = {}
if "manual_shifts" not in st.session_state:
    st.session_state.manual_shifts = {}
if "vx_min" not in st.session_state:
    st.session_state.vx_min = load_vx_min_from_db()
if "department_filter" not in st.session_state:
    st.session_state.department_filter = "Tất cả"
if "selected_shifts" not in st.session_state:
    st.session_state.selected_shifts = get_valid_shifts()
if "balance_morning_evening" not in st.session_state:
    st.session_state.balance_morning_evening = True
if "max_morning_evening_diff" not in st.session_state:
    st.session_state.max_morning_evening_diff = 4
if "show_manual_editor" not in st.session_state:
    st.session_state.show_manual_editor = False

# Giao diện chính
st.title("Phần mềm quản lý ca làm việc cho Cashier")

# Khai báo các tab
tab1, tab2, tab3 = st.tabs(["Quản lý nhân viên", "Sắp lịch", "Báo cáo"])

# Tab 1: Quản lý nhân viên
with tab1:
    st.subheader("Quản lý nhân viên")
    
    st.subheader("Import nhân viên từ CSV")
    uploaded_file = st.file_uploader("Chọn file CSV", type=["csv"])
    if uploaded_file:
        df_uploaded = pd.read_csv(uploaded_file)
        expected_columns = ["ID", "Họ Tên", "Cấp bậc", "Bộ phận"]
        if all(col in df_uploaded.columns for col in expected_columns):
            for _, row in df_uploaded.iterrows():
                if row["ID"] not in [emp["ID"] for emp in st.session_state.employees]:
                    department = row["Bộ phận"]
                    if department not in ["Cashier", "Customer Service"]:
                        st.error(f"Bộ phận {department} không hợp lệ! Chỉ chấp nhận 'Cashier' hoặc 'Customer Service'.")
                        break
                    st.session_state.employees.append({
                        "ID": str(row["ID"]),
                        "Họ Tên": row["Họ Tên"],
                        "Cấp bậc": row["Cấp bậc"],
                        "Bộ phận": department
                    })
            else:
                save_employees_to_db()
                st.success("Đã import nhân viên thành công!")
        else:
            st.error("File CSV phải chứa các cột: ID, Họ Tên, Cấp bậc, Bộ phận")
    
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
    
    st.subheader("Thêm nhân viên thủ công")
    with st.form("employee_form"):
        emp_id = st.text_input("ID nhân viên")
        emp_name = st.text_input("Họ Tên")
        emp_rank = st.selectbox("Cấp bậc", ["Junior", "Senior", "Manager"], key="add_rank")
        emp_department = st.selectbox("Bộ phận", ["Cashier", "Customer Service"], key="add_department")
        submitted = st.form_submit_button("Thêm nhân viên")
        if submitted and emp_id and emp_name:
            if emp_id not in [emp["ID"] for emp in st.session_state.employees]:
                st.session_state.employees.append({
                    "ID": emp_id,
                    "Họ Tên": emp_name,
                    "Cấp bậc": emp_rank,
                    "Bộ phận": emp_department
                })
                save_employees_to_db()
                st.success(f"Đã thêm nhân viên {emp_name}")
            else:
                st.error("ID nhân viên đã tồn tại!")
    
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
            edit_emp_department = st.selectbox("Bộ phận", ["Cashier", "Customer Service"], 
                                             index=["Cashier", "Customer Service"].index(selected_emp["Bộ phận"]), 
                                             key="edit_department")
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
                            emp["Bộ phận"] = edit_emp_department
                            if edit_emp_id != selected_emp_id and st.session_state.schedule:
                                st.session_state.schedule[edit_emp_id] = st.session_state.schedule.pop(selected_emp_id, [])
                                st.session_state.manual_shifts = {
                                    (edit_emp_id if k[0] == selected_emp_id else k[0], k[1]): v 
                                    for k, v in st.session_state.manual_shifts.items()
                                }
                                save_schedule_to_db(st.session_state.schedule, month_days)
                                save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                            break
                    save_employees_to_db()
                    st.success(f"Đã cập nhật thông tin nhân viên {edit_emp_name}")
    
    if st.session_state.employees:
        st.subheader("Danh sách nhân viên")
        df_employees = pd.DataFrame(st.session_state.employees)
        st.dataframe(df_employees)

# Tab 2: Sắp lịch
with tab2:
    st.subheader("Sắp lịch làm việc")
    
    # Nhập tháng, năm, số ca VX tối thiểu
    year = st.number_input("Năm", min_value=2020, max_value=2030, value=2025)
    month = st.number_input("Tháng", min_value=1, max_value=12, value=datetime.now().month)
    st.session_state.vx_min = st.number_input("Số ca VX tối thiểu mỗi nhân viên trong tháng", min_value=1, value=st.session_state.vx_min, step=1)
    save_vx_min_to_db(st.session_state.vx_min)
    
    # Tùy chọn cân bằng ca Sáng-Tối
    st.session_state.balance_morning_evening = st.checkbox("Cân bằng ca Sáng và ca Tối", value=True)
    if st.session_state.balance_morning_evening:
        st.session_state.max_morning_evening_diff = st.number_input("Độ lệch tối đa ca Sáng và ca Tối", min_value=0, max_value=10, value=st.session_state.max_morning_evening_diff, step=1)
    
    # Chọn bộ phận và mã ca
    st.session_state.department_filter = st.selectbox("Bộ phận", ["Tất cả", "Cashier", "Customer Service"], index=["Tất cả", "Cashier", "Customer Service"].index(st.session_state.department_filter))
    all_shifts = get_valid_shifts()
    default_shifts = get_default_shifts(st.session_state.department_filter)
    
    st.session_state.selected_shifts = st.multiselect(
        "Chọn mã ca",
        options=all_shifts + ["PRD", "AL", "NPL"],
        default=default_shifts,
        key="shift_selector"
    )
    
    # Tính toán danh sách ngày trong tháng
    _, last_day = calendar.monthrange(year, month)
    start_date = datetime(year, month, 26)
    end_date = datetime(year, month + 1, 25) if month < 12 else datetime(year + 1, 1, 25)
    month_days = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    sundays = [i for i in range(len(month_days)) if month_days[i].weekday() == 6]
    
    # Tải manual_shifts từ DB nếu chưa có
    if not st.session_state.manual_shifts:
        st.session_state.manual_shifts = load_manual_shifts_from_db(month_days)
    
    # Nút Xác nhận để hiển thị bảng chỉnh sửa ca
    if st.button("Xác nhận"):
        if not st.session_state.employees:
            st.error("Vui lòng xác định nhân viên trước khi tạo lịch!")
        elif st.session_state.department_filter != "Tất cả" and not any(emp["Bộ phận"] == st.session_state.department_filter for emp in st.session_state.employees):
            st.error(f"Không có nhân viên thuộc bộ phận {st.session_state.department_filter}!")
        elif not st.session_state.selected_shifts:
            st.error("Vui lòng chọn ít nhất một mã ca!")
        else:
            is_feasible, reason = check_feasibility(st.session_state.employees, month_days, st.session_state.selected_shifts)
            if not is_feasible:
                st.error(f"Không thể tạo lịch: {reason}")
                logging.error(f"Kiểm tra tính khả thi thất bại: {reason}")
            else:
                st.session_state.show_manual_editor = True
                st.session_state.schedule = {emp["ID"]: [""] * len(month_days) for emp in st.session_state.employees}
                for (emp_id, day), shift in st.session_state.manual_shifts.items():
                    if emp_id in st.session_state.schedule and day < len(month_days):
                        st.session_state.schedule[emp_id][day] = shift
    
    # Hiển thị bảng chỉnh sửa ca nếu đã nhấn Xác nhận
    if st.session_state.get("show_manual_editor", False):
        st.subheader("Chỉnh sửa ca thủ công")
        valid_shifts = get_valid_shifts() + ["PRD", "AL", "NPL"]
        columns = [f"{d.strftime('%a %d/%m')}" for d in month_days]
        schedule_data = {col: [] for col in ["ID Nhân viên", "Họ Tên"] + columns}
        
        filtered_employees = st.session_state.employees if st.session_state.department_filter == "Tất cả" else [
            emp for emp in st.session_state.employees if emp["Bộ phận"] == st.session_state.department_filter
        ]
        
        invalid_cells = {}
        for emp in filtered_employees:
            emp_id = emp["ID"]
            schedule_data["ID Nhân viên"].append(emp_id)
            schedule_data["Họ Tên"].append(emp["Họ Tên"])
            for day, col in enumerate(columns):
                current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                schedule_data[col].append(current_shift)
                if current_shift and current_shift not in [""]:
                    is_valid, errors = is_valid_schedule(st.session_state.schedule, emp_id, day, current_shift, month_days, sundays, st.session_state.vx_min)
                    if not is_valid:
                        invalid_cells[(emp_id, day)] = errors
        
        df_schedule = pd.DataFrame(schedule_data)
        
        st.write("Nhập ca thủ công (ô đỏ là ca không hợp lệ):")
        column_config = {
            "ID Nhân viên": st.column_config.TextColumn(disabled=True),
            "Họ Tên": st.column_config.TextColumn(disabled=True),
        }
        for col in columns:
            column_config[col] = st.column_config.SelectboxColumn(
                options=[""] + valid_shifts,
                default="",
                width="small"
            )
        
        def style_invalid_cells(df):
            def apply_style(row):
                styles = [''] * len(row)
                emp_id = row["ID Nhân viên"]
                for day in range(len(columns)):
                    if (emp_id, day) in invalid_cells:
                        styles[day + 2] = 'background-color: #ffcccc'
                return styles
            return df.style.apply(apply_style, axis=1)
        
        edited_df = st.data_editor(
            df_schedule,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key="manual_shift_editor"
        )
        
        # Cập nhật manual_shifts và schedule khi chỉnh sửa
        for i, emp in enumerate(filtered_employees):
            emp_id = emp["ID"]
            for day, col in enumerate(columns):
                new_shift = edited_df.iloc[i][col] if col in edited_df.columns else ""
                current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                if new_shift != current_shift and new_shift in valid_shifts + [""]:
                    is_valid, errors = is_valid_schedule(st.session_state.schedule, emp_id, day, new_shift, month_days, sundays, st.session_state.vx_min)
                    if is_valid:
                        if emp_id not in st.session_state.schedule:
                            st.session_state.schedule[emp_id] = [''] * len(month_days)
                        st.session_state.schedule[emp_id][day] = new_shift
                        if new_shift:
                            st.session_state.manual_shifts[(emp_id, day)] = new_shift
                        elif (emp_id, day) in st.session_state.manual_shifts:
                            del st.session_state.manual_shifts[(emp_id, day)]
                        save_schedule_to_db(st.session_state.schedule, month_days)
                        save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                        if (emp_id, day) in invalid_cells:
                            del invalid_cells[(emp_id, day)]
                    else:
                        st.warning(f"Ca {new_shift} không hợp lệ cho {emp['Họ Tên']} vào ngày {col}. Lý do: {'; '.join(errors)}")
                        logging.warning(f"Ca không hợp lệ: {new_shift} cho {emp['Họ Tên']} ngày {col}. Lý do: {'; '.join(errors)}")
                        invalid_cells[(emp_id, day)] = errors
        
        st.subheader("Lịch làm việc (Ca thủ công)")
        st.dataframe(style_invalid_cells(df_schedule), use_container_width=True)
        
        # Nút Sắp lịch tự động
        if st.button("Sắp lịch tự động"):
            st.session_state.schedule = auto_schedule(
                st.session_state.employees,
                month_days,
                sundays,
                st.session_state.vx_min,
                st.session_state.department_filter,
                st.session_state.balance_morning_evening,
                st.session_state.max_morning_evening_diff
            )
            if st.session_state.schedule:
                st.success("Đã tạo lịch thành công!")
                st.session_state.show_manual_editor = False  # Ẩn bảng chỉnh sửa sau khi tạo lịch
            else:
                st.error("Không thể tạo lịch tự động. Vui lòng kiểm tra lại các ca thủ công và ràng buộc.")

    # Hiển thị lịch cuối cùng nếu đã tạo
    if st.session_state.schedule and not st.session_state.get("show_manual_editor", False):
        st.subheader("Lịch làm việc cuối cùng")
        columns = [f"{d.strftime('%a %d/%m')}" for d in month_days]
        schedule_data = {col: [] for col in ["ID Nhân viên", "Họ Tên"] + columns}
        
        filtered_employees = st.session_state.employees if st.session_state.department_filter == "Tất cả" else [
            emp for emp in st.session_state.employees if emp["Bộ phận"] == st.session_state.department_filter
        ]
        
        invalid_cells = {}
        for emp in filtered_employees:
            emp_id = emp["ID"]
            schedule_data["ID Nhân viên"].append(emp_id)
            schedule_data["Họ Tên"].append(emp["Họ Tên"])
            for day, col in enumerate(columns):
                current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                schedule_data[col].append(current_shift)
                if current_shift and current_shift not in [""]:
                    is_valid, errors = is_valid_schedule(st.session_state.schedule, emp_id, day, current_shift, month_days, sundays, st.session_state.vx_min)
                    if not is_valid:
                        invalid_cells[(emp_id, day)] = errors
        
        df_schedule = pd.DataFrame(schedule_data)
        
        st.write("Lịch làm việc (ô đỏ là ca không hợp lệ):")
        column_config = {
            "ID Nhân viên": st.column_config.TextColumn(disabled=True),
            "Họ Tên": st.column_config.TextColumn(disabled=True),
        }
        for col in columns:
            column_config[col] = st.column_config.TextColumn(disabled=True)
        
        def style_invalid_cells(df):
            def apply_style(row):
                styles = [''] * len(row)
                emp_id = row["ID Nhân viên"]
                for day in range(len(columns)):
                    if (emp_id, day) in invalid_cells:
                        styles[day + 2] = 'background-color: #ffcccc'
                return styles
            return df.style.apply(apply_style, axis=1)
        
        st.dataframe(style_invalid_cells(df_schedule), use_container_width=True)

# Tab 3: Báo cáo
with tab3:
    st.subheader("Báo cáo")
    if st.session_state.schedule:
        st.subheader("Lịch làm việc")
        if st.button("Tải báo cáo Lịch"):
            df_report = pd.DataFrame(st.session_state.schedule).T
            df_report.index.name = "ID Nhân viên"
            df_report.columns = [d.strftime("%d/%m") for d in month_days]
            csv = df_report.to_csv()
            st.download_button(
                label="Tải báo cáo Lịch CSV",
                data=csv,
                file_name=f"lich_ca_{year}_{month}.csv",
                mime="text/csv"
            )
        
        st.subheader("Báo cáo chi tiết")
        report_data = {
            "ID Nhân viên": [],
            "Họ Tên": [],
            "Bộ phận": [],
            "Ca Sáng": [],
            "Ca Tối": [],
            "Ca VX": [],
            "Ca V6": [],
            "Ca V8": [],
            "PRD": [],
            "AL": [],
            "NPL": []
        }
        for emp in st.session_state.employees:
            emp_id = emp["ID"]
            shifts = st.session_state.schedule.get(emp_id, [''] * len(month_days))
            morning = sum(1 for s in shifts if s and s not in ["PRD", "AL", "NPL"] and get_shift_start_hour(s) < 12)
            evening = sum(1 for s in shifts if s and s not in ["PRD", "AL", "NPL"] and get_shift_start_hour(s) >= 12)
            vx = sum(1 for s in shifts if s.startswith("VX"))
            v6 = sum(1 for s in shifts if s.startswith("V6"))
            v8 = sum(1 for s in shifts if s.startswith("V8"))
            prd = sum(1 for s in shifts if s == "PRD")
            al = sum(1 for s in shifts if s == "AL")
            npl = sum(1 for s in shifts if s == "NPL")
            report_data["ID Nhân viên"].append(emp_id)
            report_data["Họ Tên"].append(emp["Họ Tên"])
            report_data["Bộ phận"].append(emp["Bộ phận"])
            report_data["Ca Sáng"].append(morning)
            report_data["Ca Tối"].append(evening)
            report_data["Ca VX"].append(vx)
            report_data["Ca V6"].append(v6)
            report_data["Ca V8"].append(v8)
            report_data["PRD"].append(prd)
            report_data["AL"].append(al)
            report_data["NPL"].append(npl)
        
        df_report_detail = pd.DataFrame(report_data)
        st.dataframe(df_report_detail, use_container_width=True)
        
        if st.button("Tải báo cáo chi tiết"):
            csv = df_report_detail.to_csv(index=False)
            st.download_button(
                label="Tải báo cáo chi tiết CSV",
                data=csv,
                file_name=f"bao_cao_chi_tiet_{year}_{month}.csv",
                mime="text/csv"
            )
    else:
        st.warning("Vui lòng tạo lịch trước khi tạo báo cáo.")
