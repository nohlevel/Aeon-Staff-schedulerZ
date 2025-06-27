import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import calendar
import numpy as np
import sqlite3
from functools import lru_cache
import logging
import time
import random

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

# Hàm xóa dữ liệu lịch
def clear_schedule_data(month_days):
    st.session_state.schedule = {}
    st.session_state.manual_shifts = {}
    conn = init_db()
    c = conn.cursor()
    c.execute('DELETE FROM schedule')
    c.execute('DELETE FROM manual_shifts')
    conn.commit()
    conn.close()
    logging.info("Đã xóa toàn bộ dữ liệu lịch và ca thủ công")
    st.success("Đã xóa toàn bộ dữ liệu lịch làm việc!")
    st.rerun()

# Hàm lưu giới hạn VX hoặc MAX_GENERATIONS vào DB
def save_settings_to_db(key, value):
    conn = init_db()
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
              (key, str(value)))
    conn.commit()
    conn.close()

# Hàm tải giới hạn VX hoặc MAX_GENERATIONS từ DB
def load_setting_from_db(key, default):
    conn = init_db()
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = ?', (key,))
    result = c.fetchone()
    conn.close()
    return int(result[0]) if result else default

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
    if shift in ["PRD", "AL", "NPL", ""]:
        return None
    code = int(shift[2:4])
    start_hour = code / 2
    return start_hour

# Hàm lấy giờ kết thúc từ mã ca
@lru_cache(maxsize=10000)
def get_shift_end_hour(shift):
    if shift in ["PRD", "AL", "NPL", ""]:
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
    if len(cs_employees) < 4 and st.session_state.department_filter in ["Customer Service", "Tất cả"]:
        return False, "Cần ít nhất 4 nhân viên Customer Service để phân bổ ca bắt buộc"
    
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

# Hàm phân bổ ca cố định cho Customer Service
def assign_fixed_cs_shifts(employees, month_days, manual_shifts):
    cs_employees = [emp for emp in employees if emp["Bộ phận"] == "Customer Service"]
    if len(cs_employees) < 4:
        return False, "Cần ít nhất 4 nhân viên Customer Service để phân bổ ca cố định"
    
    required_shifts = ["V814", "V818", "V829", "V633"]  # Ưu tiên V814, V818, V829
    alternate_shifts = {"V814": "V614", "V818": "V618", "V829": "V633"}
    shift_counts = {emp["ID"]: {"14": 0, "18": 0, "29/33": 0} for emp in cs_employees}
    new_manual_shifts = manual_shifts.copy()
    assigned_shifts = 0
    
    for day in range(len(month_days)):
        available_employees = [emp["ID"] for emp in cs_employees if (emp["ID"], day) not in new_manual_shifts]
        if len(available_employees) < 4:
            continue  # Bỏ qua nếu không đủ nhân viên
            
        random.shuffle(available_employees)
        
        # Gán V814 hoặc V614
        emp_id = available_employees.pop(0)
        shift = "V814" if "V814" in st.session_state.selected_shifts else "V614"
        new_manual_shifts[(emp_id, day)] = shift
        shift_counts[emp_id]["14"] += 1
        assigned_shifts += 1
        
        # Gán V818 hoặc V618
        emp_id = available_employees.pop(0)
        shift = "V818" if "V818" in st.session_state.selected_shifts else "V618"
        new_manual_shifts[(emp_id, day)] = shift
        shift_counts[emp_id]["18"] += 1
        assigned_shifts += 1
        
        # Gán 2 ca V829 hoặc V633 (tối đa 1 V633)
        v633_assigned = False
        for _ in range(2):
            if not available_employees:
                break
            emp_id = available_employees.pop(0)
            if "V633" in st.session_state.selected_shifts and not v633_assigned:
                shift = "V633"
                v633_assigned = True
            else:
                shift = "V829" if "V829" in st.session_state.selected_shifts else "V633"
            new_manual_shifts[(emp_id, day)] = shift
            shift_counts[emp_id]["29/33"] += 1
            assigned_shifts += 1
    
    # Cân bằng số ca
    total_days = len(month_days)
    target_shifts = total_days // len(cs_employees)
    for slot in ["14", "18", "29/33"]:
        for _ in range(total_days * 10):  # Lặp nhiều lần để cân bằng
            max_emp = max(shift_counts, key=lambda x: shift_counts[x][slot])
            min_emp = min(shift_counts, key=lambda x: shift_counts[x][slot])
            if shift_counts[max_emp][slot] <= shift_counts[min_emp][slot] + 1:
                break
            # Tìm ngày để hoán đổi
            for day in range(len(month_days)):
                if (max_emp, day) in new_manual_shifts and new_manual_shifts[(max_emp, day)] in [f"V8{slot}", f"V6{slot}"]:
                    if (min_emp, day) not in new_manual_shifts:
                        new_manual_shifts[(min_emp, day)] = new_manual_shifts[(max_emp, day)]
                        del new_manual_shifts[(max_emp, day)]
                        shift_counts[max_emp][slot] -= 1
                        shift_counts[min_emp][slot] += 1
                        break
    
    return new_manual_shifts, f"Đã phân bổ {assigned_shifts} ca cố định cho Customer Service"

# Hàm tính điểm vi phạm (fitness) của lịch
def calculate_fitness(schedule, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff):
    HARD_CONSTRAINT_WEIGHT = 1_000_000
    SOFT_CONSTRAINT_WEIGHT = 1_000
    violations = 0
    violation_details = []
    
    for emp in employees:
        emp_id = emp["ID"]
        emp_schedule = schedule.get(emp_id, [''] * len(month_days))
        emp_dept = emp["Bộ phận"]
        
        # Ràng buộc cứng
        # 1. Không quá 7 ngày làm liên tục
        consecutive_days = 0
        for day in range(len(month_days)):
            shift = emp_schedule[day]
            if shift not in ["PRD", "AL", "NPL", ""]:
                consecutive_days += 1
                if consecutive_days > 7:
                    violations += HARD_CONSTRAINT_WEIGHT
                    violation_details.append(f"{emp_id}: Vượt quá 7 ngày làm liên tục tại ngày {month_days[day].strftime('%d/%m')}")
            else:
                consecutive_days = 0
        
        # 2. Không PRD/VX/V6 liên tiếp
        for day in range(1, len(month_days)):
            current_shift = emp_schedule[day]
            prev_shift = emp_schedule[day-1]
            if prev_shift and current_shift in ["PRD", "AL", "NPL"] and prev_shift in ["PRD", "AL", "NPL"]:
                violations += HARD_CONSTRAINT_WEIGHT
                violation_details.append(f"{emp_id}: PRD/AL/NPL liên tiếp ngày {month_days[day].strftime('%d/%m')}")
            if prev_shift and current_shift.startswith("VX") and prev_shift.startswith("VX"):
                violations += HARD_CONSTRAINT_WEIGHT
                violation_details.append(f"{emp_id}: Ca VX liên tiếp ngày {month_days[day].strftime('%d/%m')}")
            if prev_shift and current_shift.startswith("V6") and prev_shift.startswith("V6"):
                violations += HARD_CONSTRAINT_WEIGHT
                violation_details.append(f"{emp_id}: Ca V6 liên tiếp ngày {month_days[day].strftime('%d/%m')}")
        
        # 3. Giãn cách 12 giờ
        for day in range(1, len(month_days)):
            current_shift = emp_schedule[day]
            prev_shift = emp_schedule[day-1]
            if current_shift not in ["PRD", "AL", "NPL", ""] and prev_shift not in ["PRD", "AL", "NPL", ""]:
                current_start = get_shift_start_hour(current_shift)
                prev_end = get_shift_end_hour(prev_shift)
                if current_start is not None and prev_end is not None:
                    prev_end_hour = prev_end % 24
                    prev_end_day_offset = 1 if prev_end >= 24 else 0
                    prev_end_minutes = int((prev_end % 1) * 60)
                    current_start_hour = int(current_start)
                    current_start_minutes = int((current_start % 1) * 60)
                    current_time = month_days[day].replace(hour=current_start_hour, minute=current_start_minutes)
                    prev_time = month_days[day-1 + prev_end_day_offset].replace(hour=int(prev_end_hour), minute=prev_end_minutes)
                    time_diff = (current_time - prev_time).total_seconds() / 3600
                    if time_diff < 12:
                        violations += HARD_CONSTRAINT_WEIGHT
                        violation_details.append(f"{emp_id}: Giãn cách dưới 12 giờ ngày {month_days[day].strftime('%d/%m')}")
        
        # 4. Số ca VX = V6 và tối thiểu vx_min
        vx_count = sum(1 for s in emp_schedule if s.startswith("VX"))
        v6_count = sum(1 for s in emp_schedule if s.startswith("V6"))
        if vx_count != v6_count:
            violations += HARD_CONSTRAINT_WEIGHT * abs(vx_count - v6_count)
            violation_details.append(f"{emp_id}: Số ca VX ({vx_count}) không bằng V6 ({v6_count})")
        if vx_count < vx_min:
            violations += HARD_CONSTRAINT_WEIGHT * (vx_min - vx_count)
            violation_details.append(f"{emp_id}: Số ca VX ({vx_count}) nhỏ hơn tối thiểu ({vx_min})")
        
        # 5. PRD không vào thứ 7, chủ nhật, ngày lễ trừ khi nhập tay
        for day in range(len(month_days)):
            shift = emp_schedule[day]
            date = month_days[day]
            if (date.weekday() in [5, 6] or is_holiday(date)) and shift == "PRD" and (emp_id, day) not in st.session_state.manual_shifts:
                violations += HARD_CONSTRAINT_WEIGHT
                violation_details.append(f"{emp_id}: PRD vào thứ 7/chủ nhật/ngày lễ ngày {month_days[day].strftime('%d/%m')}")
        
        # 6. AL, NPL chỉ được nhập tay
        for day in range(len(month_days)):
            shift = emp_schedule[day]
            if shift in ["AL", "NPL"] and (emp_id, day) not in st.session_state.manual_shifts:
                violations += HARD_CONSTRAINT_WEIGHT
                violation_details.append(f"{emp_id}: Ca {shift} không nhập tay ngày {month_days[day].strftime('%d/%m')}")
        
        # 7. Số ngày PRD bằng số ngày Chủ nhật
        prd_count = sum(1 for s in emp_schedule if s == "PRD")
        if prd_count != len(sundays):
            violations += HARD_CONSTRAINT_WEIGHT * abs(prd_count - len(sundays))
            violation_details.append(f"{emp_id}: Số ngày PRD ({prd_count}) không bằng số Chủ nhật ({len(sundays)})")
        
        # 8. Ca có trong danh sách ca đã chọn (trừ ca thủ công)
        for day in range(len(month_days)):
            shift = emp_schedule[day]
            if (emp_id, day) not in st.session_state.manual_shifts and shift not in ["PRD", "AL", "NPL", ""]:
                if shift not in st.session_state.selected_shifts:
                    violations += HARD_CONSTRAINT_WEIGHT
                    violation_details.append(f"{emp_id}: Ca {shift} không trong danh sách ca đã chọn ngày {month_days[day].strftime('%d/%m')}")
        
        # Ràng buộc mềm
        # 1. Giảm thiểu PRD ngoài Chủ nhật
        non_sunday_prd = sum(1 for day, s in enumerate(emp_schedule) if s == "PRD" and day not in sundays)
        violations += SOFT_CONSTRAINT_WEIGHT * non_sunday_prd
        if non_sunday_prd > 0:
            violation_details.append(f"{emp_id}: {non_sunday_prd} ca PRD ngoài Chủ nhật")
        
        # 2. Cân bằng ca sáng-tối
        if balance_morning_evening:
            morning_count = sum(1 for s in emp_schedule if s not in ["PRD", "AL", "NPL", ""] and get_shift_start_hour(s) < 12)
            evening_count = sum(1 for s in emp_schedule if s not in ["PRD", "AL", "NPL", ""] and get_shift_start_hour(s) >= 12)
            diff = abs(morning_count - evening_count)
            if diff > max_morning_evening_diff:
                violations += SOFT_CONSTRAINT_WEIGHT * (diff - max_morning_evening_diff)
                violation_details.append(f"{emp_id}: Độ lệch ca sáng ({morning_count}) và tối ({evening_count}) vượt quá {max_morning_evening_diff}")
    
    # Ràng buộc cứng: Ca bắt buộc cho Customer Service
    cs_employees = [emp for emp in employees if emp["Bộ phận"] == "Customer Service"]
    for day in range(len(month_days)):
        cs_shifts = [schedule.get(emp["ID"], [''] * len(month_days))[day] for emp in cs_employees]
        v814_v614_count = cs_shifts.count("V814") + cs_shifts.count("V614")
        v818_v618_count = cs_shifts.count("V818") + cs_shifts.count("V618")
        v829_v633_count = cs_shifts.count("V829") + cs_shifts.count("V633")
        v633_count = cs_shifts.count("V633")
        
        if v814_v614_count != 1:
            violations += HARD_CONSTRAINT_WEIGHT * abs(v814_v614_count - 1)
            violation_details.append(f"Ngày {month_days[day].strftime('%d/%m')}: V814/V614 có {v814_v614_count} ca (cần 1)")
        if v818_v618_count != 1:
            violations += HARD_CONSTRAINT_WEIGHT * abs(v818_v618_count - 1)
            violation_details.append(f"Ngày {month_days[day].strftime('%d/%m')}: V818/V618 có {v818_v618_count} ca (cần 1)")
        if v829_v633_count != 2:
            violations += HARD_CONSTRAINT_WEIGHT * abs(v829_v633_count - 2)
            violation_details.append(f"Ngày {month_days[day].strftime('%d/%m')}: V829/V633 có {v829_v633_count} ca (cần 2)")
        if v633_count > 1:
            violations += HARD_CONSTRAINT_WEIGHT * (v633_count - 1)
            violation_details.append(f"Ngày {month_days[day].strftime('%d/%m')}: V633 có {v633_count} ca (tối đa 1)")
    
    return violations, violation_details

# Hàm khởi tạo cá thể ngẫu nhiên
def initialize_random_individual(employees, month_days, valid_shifts, manual_shifts):
    schedule = {emp["ID"]: [''] * len(month_days) for emp in employees}
    for emp in employees:
        emp_id = emp["ID"]
        shift_pool = valid_shifts
        if emp["Cấp bậc"] in ["Senior", "Manager"]:
            morning_shifts = [s for s in valid_shifts if s != "PRD" and get_shift_start_hour(s) < 12]
            if morning_shifts:
                shift_pool = morning_shifts + ["PRD"]
        for day in range(len(month_days)):
            if (emp_id, day) in manual_shifts:
                schedule[emp_id][day] = manual_shifts[(emp_id, day)]
            else:
                schedule[emp_id][day] = random.choice(shift_pool + [''])
    return schedule

# Hàm khởi tạo cá thể heuristic (ưu tiên ca CS bắt buộc)
def initialize_heuristic_individual(employees, month_days, valid_shifts, manual_shifts, sundays):
    schedule = {emp["ID"]: [''] * len(month_days) for emp in employees}
    cs_employees = [emp for emp in employees if emp["Bộ phận"] == "Customer Service"]
    required_shifts = ["V814", "V614", "V818", "V618", "V829", "V633"]
    
    for day in range(len(month_days)):
        available_cs = [emp["ID"] for emp in cs_employees if (emp["ID"], day) not in manual_shifts]
        random.shuffle(available_cs)
        
        # Gán V814 hoặc V614
        if available_cs and ("V814" in valid_shifts or "V614" in valid_shifts):
            emp_id = available_cs.pop(0)
            shift = "V814" if "V814" in valid_shifts else "V614"
            if (emp_id, day) not in manual_shifts:
                schedule[emp_id][day] = shift
        
        # Gán V818 hoặc V618
        if available_cs and ("V818" in valid_shifts or "V618" in valid_shifts):
            emp_id = available_cs.pop(0)
            shift = "V818" if "V818" in valid_shifts else "V618"
            if (emp_id, day) not in manual_shifts:
                schedule[emp_id][day] = shift
        
        # Gán V829 hoặc V633 (2 ca)
        if available_cs and ("V829" in valid_shifts or "V633" in valid_shifts):
            for _ in range(2):
                if available_cs:
                    emp_id = available_cs.pop(0)
                    shift = "V633" if "V633" in valid_shifts and schedule[emp_id][day] != "V633" else "V829"
                    if (emp_id, day) not in manual_shifts:
                        schedule[emp_id][day] = shift
    
    # Điền ngẫu nhiên các ô còn lại
    for emp in employees:
        emp_id = emp["ID"]
        shift_pool = valid_shifts
        if emp["Cấp bậc"] in ["Senior", "Manager"]:
            morning_shifts = [s for s in valid_shifts if s != "PRD" and get_shift_start_hour(s) < 12]
            if morning_shifts:
                shift_pool = morning_shifts + ["PRD"]
        for day in range(len(month_days)):
            if (emp_id, day) not in manual_shifts and not schedule[emp_id][day]:
                schedule[emp_id][day] = random.choice(shift_pool + [''])
            if day in sundays and (emp_id, day) not in manual_shifts:
                schedule[emp_id][day] = "PRD"
    
    return schedule

# Hàm crossover (trao đổi đoạn ngày)
def crossover(parent1, parent2, employees, month_days):
    child1 = {emp["ID"]: [''] * len(month_days) for emp in employees}
    child2 = {emp["ID"]: [''] * len(month_days) for emp in employees}
    crossover_point = random.randint(1, len(month_days) - 1)
    
    for emp_id in child1:
        child1[emp_id][:crossover_point] = parent1[emp_id][:crossover_point]
        child1[emp_id][crossover_point:] = parent2[emp_id][crossover_point:]
        child2[emp_id][:crossover_point] = parent2[emp_id][:crossover_point]
        child2[emp_id][crossover_point:] = parent1[emp_id][crossover_point:]
        
        # Đảm bảo giữ các ca thủ công
        for day in range(len(month_days)):
            if (emp_id, day) in st.session_state.manual_shifts:
                child1[emp_id][day] = st.session_state.manual_shifts[(emp_id, day)]
                child2[emp_id][day] = st.session_state.manual_shifts[(emp_id, day)]
    
    return child1, child2

# Hàm mutation
def mutation(schedule, employees, month_days, valid_shifts, mutation_rate=0.01):
    for emp in employees:
        emp_id = emp["ID"]
        shift_pool = valid_shifts
        if emp["Cấp bậc"] in ["Senior", "Manager"]:
            morning_shifts = [s for s in valid_shifts if s != "PRD" and get_shift_start_hour(s) < 12]
            if morning_shifts:
                shift_pool = morning_shifts + ["PRD"]
        for day in range(len(month_days)):
            if (emp_id, day) not in st.session_state.manual_shifts and random.random() < mutation_rate:
                schedule[emp_id][day] = random.choice(shift_pool + [''])
    return schedule

# Hàm local repair (Min-Conflicts)
def local_repair(schedule, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff, max_steps=100):
    valid_shifts = st.session_state.selected_shifts + ["PRD"]
    for _ in range(max_steps):
        fitness, violation_details = calculate_fitness(schedule, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff)
        if fitness == 0:
            break
        
        # Chọn ô vi phạm ngẫu nhiên
        emp_id = random.choice([emp["ID"] for emp in employees])
        day = random.randint(0, len(month_days) - 1)
        if (emp_id, day) in st.session_state.manual_shifts:
            continue
        
        current_shift = schedule[emp_id][day]
        best_shift = current_shift
        best_fitness = fitness
        
        # Thử tất cả ca hợp lệ
        emp = next(e for e in employees if e["ID"] == emp_id)
        shift_pool = valid_shifts
        if emp["Cấp bậc"] in ["Senior", "Manager"]:
            morning_shifts = [s for s in valid_shifts if s != "PRD" and get_shift_start_hour(s) < 12]
            if morning_shifts:
                shift_pool = morning_shifts + ["PRD"]
        
        for shift in shift_pool + ['']:
            if shift == current_shift:
                continue
            schedule[emp_id][day] = shift
            new_fitness, _ = calculate_fitness(schedule, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff)
            if new_fitness < best_fitness:
                best_fitness = new_fitness
                best_shift = shift
        schedule[emp_id][day] = best_shift
    
    return schedule

# Hàm Memetic Algorithm
def auto_schedule(employees, month_days, sundays, vx_min, department_filter, balance_morning_evening, max_morning_evening_diff, max_generations):
    start_time = time.time()
    logging.info(f"Bắt đầu tạo lịch với Memetic Algorithm: {len(employees)} nhân viên, {len(month_days)} ngày, bộ phận: {department_filter}, max_generations: {max_generations}")
    
    if not employees:
        logging.error("Không có nhân viên để tạo lịch")
        return {}
    
    # Lọc nhân viên theo bộ phận
    if department_filter != "Tất cả":
        employees = [emp for emp in employees if emp["Bộ phận"] == department_filter]
    
    if not employees:
        logging.error(f"Không có nhân viên thuộc bộ phận {department_filter}")
        return {}
    
    valid_shifts = st.session_state.selected_shifts + ["PRD"]
    manual_shifts = st.session_state.get("manual_shifts", {})
    
    # Thanh tiến trình
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    # Tham số Memetic Algorithm
    POPULATION_SIZE = 50
    MUTATION_RATE = 0.01
    ELITE_SIZE = 5
    TOURNAMENT_SIZE = 5
    HARD_CONSTRAINT_THRESHOLD = 0
    SOFT_CONSTRAINT_THRESHOLD = 1000
    
    # Khởi tạo population
    population = []
    for i in range(POPULATION_SIZE):
        if i < POPULATION_SIZE // 2:
            individual = initialize_random_individual(employees, month_days, valid_shifts, manual_shifts)
        else:
            individual = initialize_heuristic_individual(employees, month_days, valid_shifts, manual_shifts, sundays)
        population.append(individual)
        progress_bar.progress(min((i + 1) / POPULATION_SIZE, 0.2))
        progress_text.text(f"Khởi tạo cá thể {i + 1}/{POPULATION_SIZE}...")
    
    best_schedule = None
    best_fitness = float('inf')
    generation = 0
    
    while generation < max_generations:
        # Đánh giá fitness
        fitness_scores = []
        for i, individual in enumerate(population):
            fitness, details = calculate_fitness(individual, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff)
            fitness_scores.append((fitness, individual, details))
            if fitness < best_fitness:
                best_fitness = fitness
                best_schedule = individual
                logging.info(f"Thế hệ {generation}: Cập nhật lịch tốt nhất, fitness = {best_fitness}")
            progress_bar.progress(min(0.2 + (i + 1) / POPULATION_SIZE * 0.2, 0.4))
            progress_text.text(f"Đánh giá cá thể {i + 1}/{POPULATION_SIZE} trong thế hệ {generation + 1}...")
        
        # Kiểm tra điều kiện dừng
        if best_fitness <= HARD_CONSTRAINT_THRESHOLD + SOFT_CONSTRAINT_THRESHOLD:
            logging.info(f"Tìm thấy lịch khả thi tại thế hệ {generation}, fitness = {best_fitness}")
            break
        
        # Lựa chọn elitism
        fitness_scores.sort(key=lambda x: x[0])
        new_population = [fs[1] for fs in fitness_scores[:ELITE_SIZE]]
        
        # Tournament selection
        while len(new_population) < POPULATION_SIZE:
            tournament = random.sample(fitness_scores, TOURNAMENT_SIZE)
            winner = min(tournament, key=lambda x: x[0])[1]
            new_population.append(winner)
        
        population = new_population[:POPULATION_SIZE]
        
        # Crossover
        for i in range(ELITE_SIZE, POPULATION_SIZE, 2):
            if i + 1 < POPULATION_SIZE:
                parent1 = population[i]
                parent2 = population[i + 1]
                child1, child2 = crossover(parent1, parent2, employees, month_days)
                population[i] = child1
                population[i + 1] = child2
            progress_bar.progress(min(0.4 + (i + 1) / POPULATION_SIZE * 0.2, 0.6))
            progress_text.text(f"Thực hiện crossover {i + 1}/{POPULATION_SIZE} trong thế hệ {generation + 1}...")
        
        # Mutation
        for i in range(ELITE_SIZE, POPULATION_SIZE):
            population[i] = mutation(population[i], employees, month_days, valid_shifts, MUTATION_RATE)
            progress_bar.progress(min(0.6 + (i + 1 - ELITE_SIZE) / (POPULATION_SIZE - ELITE_SIZE) * 0.2, 0.8))
            progress_text.text(f"Thực hiện mutation {i + 1 - ELITE_SIZE}/{POPULATION_SIZE - ELITE_SIZE} trong thế hệ {generation + 1}...")
        
        # Local repair
        for i in range(ELITE_SIZE, POPULATION_SIZE):
            population[i] = local_repair(population[i], employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff)
            progress_bar.progress(min(0.8 + (i + 1 - ELITE_SIZE) / (POPULATION_SIZE - ELITE_SIZE) * 0.1, 0.9))
            progress_text.text(f"Thực hiện local repair {i + 1 - ELITE_SIZE}/{POPULATION_SIZE - ELITE_SIZE} trong thế hệ {generation + 1}...")
        
        generation += 1
        progress_bar.progress(min(0.9 + generation / max_generations * 0.1, 0.99))
        progress_text.text(f"Hoàn tất thế hệ {generation}/{max_generations}...")
    
    # Lưu lịch tốt nhất
    elapsed_time = time.time() - start_time
    if best_schedule and any(shifts for shifts in best_schedule.values()):
        save_schedule_to_db(best_schedule, month_days)
        fitness, details = calculate_fitness(best_schedule, employees, month_days, sundays, vx_min, balance_morning_evening, max_morning_evening_diff)
        shift_count = sum(len([s for s in shifts if s]) for shifts in best_schedule.values())
        logging.info(f"Kết thúc Memetic Algorithm. Fitness tốt nhất: {fitness}, tổng số ca: {shift_count}, thời gian: {elapsed_time:.2f} giây")
        if details:
            logging.info(f"Vi phạm còn lại: {'; '.join(details)}")
        progress_bar.progress(1.0)
        progress_text.text(f"Hoàn tất! Fitness tốt nhất: {fitness} trong {elapsed_time:.2f} giây")
        return best_schedule
    else:
        logging.error(f"Không tìm được lịch hợp lệ sau {max_generations} thế hệ")
        progress_bar.progress(1.0)
        progress_text.text(f"Thất bại! Không tìm được lịch hợp lệ sau {max_generations} thế hệ")
        return {}

# Hàm tính thống kê số ca mỗi tuần
def calculate_weekly_stats(schedule, filtered_employees, month_days):
    week_indices = []
    current_week = []
    
    for i, date in enumerate(month_days):
        if date.weekday() == 0:  # Thứ Hai
            if current_week:
                week_indices.append(current_week)
            current_week = [i]
        else:
            current_week.append(i)
        if i == len(month_days) - 1:  # Ngày cuối cùng
            week_indices.append(current_week)
    
    weekly_stats = {
        'work': [],
        'morning': [],
        'evening': [],
        'off': []
    }
    daily_stats = {
        'work': [0] * len(month_days),
        'morning': [0] * len(month_days),
        'evening': [0] * len(month_days),
        'off': [0] * len(month_days)
    }
    
    for day in range(len(month_days)):
        for emp in filtered_employees:
            shift = schedule.get(emp["ID"], [''] * len(month_days))[day]
            if shift in ["PRD", "AL", "NPL"]:
                daily_stats['off'][day] += 1
            elif shift:
                daily_stats['work'][day] += 1
                start_hour = get_shift_start_hour(shift)
                if start_hour is not None:
                    if start_hour < 12:
                        daily_stats['morning'][day] += 1
                    else:
                        daily_stats['evening'][day] += 1
    
    for week in week_indices:
        week_work = sum(daily_stats['work'][day] for day in week)
        week_morning = sum(daily_stats['morning'][day] for day in week)
        week_evening = sum(daily_stats['evening'][day] for day in week)
        week_off = sum(daily_stats['off'][day] for day in week)
        weekly_stats['work'].append(week_work)
        weekly_stats['morning'].append(week_morning)
        weekly_stats['evening'].append(week_evening)
        weekly_stats['off'].append(week_off)
    
    week_labels = []
    for i, week in enumerate(week_indices):
        start_date = month_days[week[0]].strftime("%d/%m")
        end_date = month_days[week[-1]].strftime("%d/%m")
        week_labels.append(f"Tuần {i+1} ({start_date}-{end_date})")
    
    return weekly_stats, daily_stats, week_labels, week_indices

# Khởi tạo trạng thái phiên
if "employees" not in st.session_state:
    st.session_state.employees = load_employees_from_db()
if "schedule" not in st.session_state:
    st.session_state.schedule = {}
if "manual_shifts" not in st.session_state:
    st.session_state.manual_shifts = {}
if "vx_min" not in st.session_state:
    st.session_state.vx_min = load_setting_from_db('vx_min', 3)
if "max_generations" not in st.session_state:
    st.session_state.max_generations = load_setting_from_db('max_generations', 10)
if "department_filter" not in st.session_state:
    st.session_state.department_filter = "Tất cả"
if "selected_shifts" not in st.session_state:
    st.session_state.selected_shifts = get_valid_shifts()
if "balance_morning_evening" not in st.session_state:
    st.session_state.balance_morning_evening = True
if "max_morning_evening_diff" not in st.session_state:
    st.session_state.max_morning_evening_diff = 4
if "show_manual_shifts" not in st.session_state:
    st.session_state.show_manual_shifts = False

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
    
    # Sắp xếp các trường điều chỉnh theo cột
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.number_input("Năm", min_value=2020, max_value=2030, value=2025, step=1)
        month = st.number_input("Tháng", min_value=1, max_value=12, value=datetime.now().month, step=1)
    with col2:
        st.session_state.vx_min = st.number_input("Số ca VX tối thiểu", min_value=1, value=st.session_state.vx_min, step=1)
        save_settings_to_db('vx_min', st.session_state.vx_min)
        st.session_state.max_generations = st.number_input("Số thế hệ tối đa", min_value=1, max_value=100, value=st.session_state.max_generations, step=1)
        save_settings_to_db('max_generations', st.session_state.max_generations)
    with col3:
        st.session_state.department_filter = st.selectbox("Bộ phận", ["Tất cả", "Cashier", "Customer Service"], 
                                                        index=["Tất cả", "Cashier", "Customer Service"].index(st.session_state.department_filter))
        st.session_state.balance_morning_evening = st.checkbox("Cân bằng ca Sáng-Tối", value=st.session_state.balance_morning_evening)
        if st.session_state.balance_morning_evening:
            st.session_state.max_morning_evening_diff = st.number_input("Độ lệch Sáng-Tối tối đa", min_value=0, max_value=10, 
                                                                     value=st.session_state.max_morning_evening_diff, step=1)
    
    all_shifts = get_valid_shifts()
    default_shifts = get_default_shifts(st.session_state.department_filter)
    st.session_state.selected_shifts = st.multiselect(
        "Chọn mã ca",
        options=all_shifts + ["PRD", "AL", "NPL"],
        default=default_shifts,
        key="shift_selector"
    )
    
    _, last_day = calendar.monthrange(year, month)
    start_date = datetime(year, month, 26)
    end_date = datetime(year, month + 1, 25) if month < 12 else datetime(year + 1, 1, 25)
    month_days = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    sundays = [i for i in range(len(month_days)) if month_days[i].weekday() == 6]
    
    if not st.session_state.manual_shifts:
        st.session_state.manual_shifts = load_manual_shifts_from_db(month_days)
    
    # Tự động hiển thị bảng nhập ca nếu có nhân viên và mã ca
    if st.session_state.employees and st.session_state.selected_shifts:
        is_feasible, reason = check_feasibility(st.session_state.employees, month_days, st.session_state.selected_shifts)
        if is_feasible:
            st.session_state.show_manual_shifts = True
        else:
            st.error(f"Không thể hiển thị bảng nhập ca: {reason}")
            logging.error(f"Kiểm tra tính khả thi thất bại: {reason}")
    
    if st.session_state.show_manual_shifts:
        st.subheader("Nhập và chỉnh sửa lịch làm việc")
        valid_shifts = get_valid_shifts() + ["PRD", "AL", "NPL"]
        columns = [f"{d.strftime('%a %d/%m')}" for d in month_days]
        manual_data = {col: [] for col in ["ID Nhân viên", "Họ Tên"] + columns}
        
        filtered_employees = st.session_state.employees if st.session_state.department_filter == "Tất cả" else [
            emp for emp in st.session_state.employees if emp["Bộ phận"] == st.session_state.department_filter
        ]
        
        invalid_cells = {}
        for emp in filtered_employees:
            emp_id = emp["ID"]
            manual_data["ID Nhân viên"].append(emp_id)
            manual_data["Họ Tên"].append(emp["Họ Tên"])
            for day, col in enumerate(columns):
                shift = st.session_state.manual_shifts.get((emp_id, day), "")
                if not shift and emp_id in st.session_state.schedule:
                    shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                manual_data[col].append(shift)
                if shift and shift not in [""] and (emp_id, day) not in st.session_state.manual_shifts:
                    temp_schedule = {emp_id: ['' if i != day else shift for i in range(len(month_days))]}
                    is_valid, errors = calculate_fitness(temp_schedule, [emp], month_days, sundays, 
                                                       st.session_state.vx_min, st.session_state.balance_morning_evening, 
                                                       st.session_state.max_morning_evening_diff)
                    if is_valid > 0:
                        invalid_cells[(emp_id, day)] = errors
        
        # Tính thống kê số ca
        weekly_stats, daily_stats, week_labels, week_indices = calculate_weekly_stats(
            st.session_state.schedule or {emp["ID"]: [''] * len(month_days) for emp in filtered_employees}, 
            filtered_employees, 
            month_days
        )
        
        # Tạo các hàng thống kê
        weekly_work_row = {col: "" for col in ["ID Nhân viên", "Họ Tên"] + columns}
        weekly_morning_row = {col: "" for col in ["ID Nhân viên", "Họ Tên"] + columns}
        weekly_evening_row = {col: "" for col in ["ID Nhân viên", "Họ Tên"] + columns}
        weekly_off_row = {col: "" for col in ["ID Nhân viên", "Họ Tên"] + columns}
        
        weekly_work_row["ID Nhân viên"] = "Tổng ca làm/tuần"
        weekly_morning_row["ID Nhân viên"] = "Tổng ca sáng/tuần"
        weekly_evening_row["ID Nhân viên"] = "Tổng ca tối/tuần"
        weekly_off_row["ID Nhân viên"] = "Tổng ca nghỉ/tuần"
        
        week_index = 0
        day_index = 0
        for i, week in enumerate(week_indices):
            for _ in week:
                if day_index < len(columns):
                    if i == week_index:
                        weekly_work_row[columns[day_index]] = str(weekly_stats['work'][i])
                        weekly_morning_row[columns[day_index]] = str(weekly_stats['morning'][i])
                        weekly_evening_row[columns[day_index]] = str(weekly_stats['evening'][i])
                        weekly_off_row[columns[day_index]] = str(weekly_stats['off'][i])
                day_index += 1
            week_index += 1
        
        daily_work_row = {"ID Nhân viên": "Tổng ca làm/ngày", "Họ Tên": ""} | {columns[i]: daily_stats['work'][i] for i in range(len(columns))}
        daily_morning_row = {"ID Nhân viên": "Tổng ca sáng/ngày", "Họ Tên": ""} | {columns[i]: daily_stats['morning'][i] for i in range(len(columns))}
        daily_evening_row = {"ID Nhân viên": "Tổng ca tối/ngày", "Họ Tên": ""} | {columns[i]: daily_stats['evening'][i] for i in range(len(columns))}
        daily_off_row = {"ID Nhân viên": "Tổng ca nghỉ/ngày", "Họ Tên": ""} | {columns[i]: daily_stats['off'][i] for i in range(len(columns))}
        
        # Tạo DataFrame
        df_manual = pd.DataFrame(manual_data)
        df_stats = pd.DataFrame([weekly_work_row, weekly_morning_row, weekly_evening_row, weekly_off_row, daily_work_row, daily_morning_row, daily_evening_row, daily_off_row])
        df_manual = pd.concat([df_stats.iloc[:4], df_manual, df_stats.iloc[4:]], ignore_index=True)
        
        st.write("Nhập và chỉnh sửa lịch làm việc (ô đỏ là ca không hợp lệ):")
        column_config = {
            "ID Nhân viên": st.column_config.TextColumn(disabled=True),
            "Họ Tên": st.column_config.TextColumn(disabled=True),
        }
        for col in columns:
            column_config[col] = st.column_config.SelectboxColumn(
                options=[""] + valid_shifts,
                default="",
                width="small",
                disabled=col in columns and (df_manual.index[-4:].to_list() + df_manual.index[:4].to_list())
            )
        
        def style_invalid_cells(df):
            def apply_style(row):
                styles = [''] * len(row)
                if row["ID Nhân viên"] in ["Tổng ca làm/ngày", "Tổng ca sáng/ngày", "Tổng ca tối/ngày", "Tổng ca nghỉ/ngày",
                                          "Tổng ca làm/tuần", "Tổng ca sáng/tuần", "Tổng ca tối/tuần", "Tổng ca nghỉ/tuần"]:
                    return ['background-color: #f0f0f0'] * len(row)
                emp_id = row["ID Nhân viên"]
                for day in range(len(columns)):
                    if (emp_id, day) in invalid_cells:
                        styles[day + 2] = 'background-color: #ffcccc'
                return styles
            return df.style.apply(apply_style, axis=1)
        
        edited_manual_df = st.data_editor(
            df_manual,
            column_config=column_config,
            hide_index=True,
            use_container_width=True,
            key="manual_shifts_editor"
        )
        
        for i, emp in enumerate(filtered_employees):
            emp_id = emp["ID"]
            for day, col in enumerate(columns):
                new_shift = edited_manual_df.iloc[i + 4][col] if col in edited_manual_df.columns else ""  # +4 vì có 4 hàng tuần
                current_shift = st.session_state.manual_shifts.get((emp_id, day), "")
                if not current_shift and emp_id in st.session_state.schedule:
                    current_shift = st.session_state.schedule.get(emp_id, [''] * len(month_days))[day]
                if new_shift != current_shift and new_shift in valid_shifts + [""]:
                    if new_shift:
                        st.session_state.manual_shifts[(emp_id, day)] = new_shift
                    elif (emp_id, day) in st.session_state.manual_shifts:
                        del st.session_state.manual_shifts[(emp_id, day)]
                    if emp_id not in st.session_state.schedule:
                        st.session_state.schedule[emp_id] = [''] * len(month_days)
                    st.session_state.schedule[emp_id][day] = new_shift
                    save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                    save_schedule_to_db(st.session_state.schedule, month_days)
        
        # Nút xóa lịch cũ
        if st.button("Xóa lịch cũ"):
            clear_schedule_data(month_days)
        
        # Nút bổ sung ca cố định
        if st.button("Bổ sung ca cố định"):
            if not st.session_state.employees:
                st.error("Vui lòng xác định nhân viên trước khi bổ sung ca cố định!")
            elif st.session_state.department_filter not in ["Customer Service", "Tất cả"]:
                st.error("Chỉ có thể bổ sung ca cố định cho bộ phận Customer Service hoặc Tất cả!")
            else:
                is_feasible, reason = check_feasibility(st.session_state.employees, month_days, st.session_state.selected_shifts)
                if not is_feasible:
                    st.error(f"Không thể bổ sung ca cố định: {reason}")
                    logging.error(f"Kiểm tra tính khả thi thất bại: {reason}")
                else:
                    new_manual_shifts, message = assign_fixed_cs_shifts(st.session_state.employees, month_days, st.session_state.manual_shifts)
                    if new_manual_shifts:
                        st.session_state.manual_shifts = new_manual_shifts
                        save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                        st.success(message)
                        logging.info(message)
                        st.rerun()
                    else:
                        st.error("Không thể bổ sung ca cố định: " + message)
                        logging.error("Không thể bổ sung ca cố định: " + message)
        
        st.subheader("Lịch làm việc")
        st.dataframe(style_invalid_cells(df_manual), use_container_width=True)
    
    if st.button("Sắp lịch tự động"):
        if not st.session_state.employees:
            st.error("Vui lòng xác định nhân viên trước khi tạo lịch!")
        elif st.session_state.department_filter != "Tất cả" and not any(emp["Bộ phận"] == st.session_state.department_filter for emp in st.session_state.employees):
            st.error(f"Không có nhân viên thuộc bộ phận {st.session_state.department_filter}!")
        elif not st.session_state.selected_shifts:
            st.error("Vui lòng chọn ít nhất một mã ca!")
        elif not st.session_state.show_manual_shifts:
            st.error("Vui lòng nhập ca đăng ký hoặc bổ sung ca cố định trước khi tạo lịch!")
        else:
            is_feasible, reason = check_feasibility(st.session_state.employees, month_days, st.session_state.selected_shifts)
            if not is_feasible:
                st.error(f"Không thể tạo lịch: {reason}")
                logging.error(f"Kiểm tra tính khả thi thất bại: {reason}")
            else:
                schedule = auto_schedule(
                    st.session_state.employees,
                    month_days,
                    sundays,
                    st.session_state.vx_min,
                    st.session_state.department_filter,
                    st.session_state.balance_morning_evening,
                    st.session_state.max_morning_evening_diff,
                    st.session_state.max_generations
                )
                if schedule and any(shifts for shifts in schedule.values()):
                    st.session_state.schedule = schedule
                    # Cập nhật manual_shifts với các ca từ lịch tự động (trừ các ca đã nhập tay trước đó)
                    shift_count = 0
                    for emp_id, shifts in schedule.items():
                        for day, shift in enumerate(shifts):
                            if shift and (emp_id, day) not in st.session_state.manual_shifts:
                                st.session_state.manual_shifts[(emp_id, day)] = shift
                                shift_count += 1
                    save_manual_shifts_to_db(st.session_state.manual_shifts, month_days)
                    save_schedule_to_db(st.session_state.schedule, month_days)
                    logging.info(f"Đã lưu {shift_count} ca vào manual_shifts và schedule")
                    st.success(f"Đã tạo lịch thành công với {shift_count} ca được phân bổ!")
                    st.rerun()  # Làm mới giao diện để hiển thị lịch
                else:
                    st.error(f"Không thể tạo lịch hợp lệ sau {st.session_state.max_generations} thế hệ. Vui lòng kiểm tra log hoặc thử tăng số thế hệ tối đa.")
                    logging.error(f"Không tạo được lịch hợp lệ. schedule: {schedule}")

# Tab 3: Báo cáo
with tab3:
    st.subheader("Báo cáo")
    if st.session_state.schedule:
        st.subheader("Lịch làm việc")
        if st.button("Tải báo cáo Lịch"):
            df_report = pd.DataFrame(st.session_state.schedule).T
            df_report.index.name = "ID Nhân viên"
            df_report.columns = [d.strftime("%d/%m") for d in month_days]
            weekly_stats, daily_stats, week_labels, week_indices = calculate_weekly_stats(st.session_state.schedule, st.session_state.employees, month_days)
            weekly_work_row = {d.strftime("%d/%m"): "" for d in month_days}
            weekly_morning_row = {d.strftime("%d/%m"): "" for d in month_days}
            weekly_evening_row = {d.strftime("%d/%m"): "" for d in month_days}
            weekly_off_row = {d.strftime("%d/%m"): "" for d in month_days}
            week_index = 0
            day_index = 0
            for i, week in enumerate(week_indices):
                for _ in week:
                    if day_index < len(month_days):
                        weekly_work_row[month_days[day_index].strftime("%d/%m")] = str(weekly_stats['work'][i]) if i == week_index else ""
                        weekly_morning_row[month_days[day_index].strftime("%d/%m")] = str(weekly_stats['morning'][i]) if i == week_index else ""
                        weekly_evening_row[month_days[day_index].strftime("%d/%m")] = str(weekly_stats['evening'][i]) if i == week_index else ""
                        weekly_off_row[month_days[day_index].strftime("%d/%m")] = str(weekly_stats['off'][i]) if i == week_index else ""
                    day_index += 1
                week_index += 1
            df_report.loc["Tổng ca làm/ngày"] = daily_stats['work']
            df_report.loc["Tổng ca sáng/ngày"] = daily_stats['morning']
            df_report.loc["Tổng ca tối/ngày"] = daily_stats['evening']
            df_report.loc["Tổng ca nghỉ/ngày"] = daily_stats['off']
            df_report.loc["Tổng ca làm/tuần"] = [weekly_work_row[d.strftime("%d/%m")] for d in month_days]
            df_report.loc["Tổng ca sáng/tuần"] = [weekly_morning_row[d.strftime("%d/%m")] for d in month_days]
            df_report.loc["Tổng ca tối/tuần"] = [weekly_evening_row[d.strftime("%d/%m")] for d in month_days]
            df_report.loc["Tổng ca nghỉ/tuần"] = [weekly_off_row[d.strftime("%d/%m")] for d in month_days]
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
        
        df_report = pd.DataFrame(report_data)
        st.dataframe(df_report, use_container_width=True)
        
        if st.button("Tải báo cáo chi tiết"):
            csv = df_report.to_csv(index=False)
            st.download_button(
                label="Tải báo cáo chi tiết CSV",
                data=csv,
                file_name=f"bao_cao_chi_tiet_{year}_{month}.csv",
                mime="text/csv"
            )
