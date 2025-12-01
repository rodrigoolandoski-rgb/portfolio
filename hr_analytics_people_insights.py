"""
Project: Python – HR Analytics & People Insights (Pandas + SQL)
Focus: KPIs for turnover, absenteeism, headcount and personnel cost
Source: Portfolio spec by Rodrigo Blasi Olandoski
"""

import pandas as pd
from sqlalchemy import create_engine


DB_URL = "postgresql+psycopg2://user:password@localhost:5432/hr_db"
engine = create_engine(DB_URL, future=True)


def load_hr_tables():
    """Load core HR tables into Pandas DataFrames."""
    employees = pd.read_sql("SELECT * FROM employees", engine)
    attendance = pd.read_sql("SELECT * FROM attendance", engine)
    return employees, attendance


def prepare_data(employees: pd.DataFrame, attendance: pd.DataFrame):
    employees["hire_date"] = pd.to_datetime(employees["hire_date"])
    employees["termination_date"] = pd.to_datetime(
        employees.get("termination_date"), errors="coerce"
    )
    attendance["date"] = pd.to_datetime(attendance["date"])
    attendance["month"] = attendance["date"].dt.to_period("M")
    return employees, attendance


def compute_headcount_monthly(employees: pd.DataFrame, attendance: pd.DataFrame):
    month_range = attendance["month"].dropna().unique()
    month_range = sorted(month_range)
    headcount_data = []

    for month in month_range:
        start = month.to_timestamp()
        end = (month + 1).to_timestamp() - pd.Timedelta(seconds=1)
        active = employees[
            (employees["hire_date"] <= end)
            & (
                employees["termination_date"].isna()
                | (employees["termination_date"] >= start)
            )
        ]
        headcount_data.append({"month": month, "headcount": len(active)})
    return pd.DataFrame(headcount_data)


def compute_absenteeism_by_department(
    employees: pd.DataFrame, attendance: pd.DataFrame
):
    attendance_emp = attendance.merge(employees, on="employee_id", how="left")
    absenteeism = (
        attendance_emp.groupby("department")
        .agg(
            total_hours_absent=("hours_absent", "sum"),
            total_hours_worked=("hours_worked", "sum"),
        )
        .reset_index()
    )
    absenteeism["absenteeism_rate"] = (
        absenteeism["total_hours_absent"]
        / (
            absenteeism["total_hours_worked"]
            + absenteeism["total_hours_absent"]
        )
    )
    return absenteeism


def compute_personnel_cost_by_department(employees: pd.DataFrame):
    return (
        employees.groupby("department")
        .agg(
            total_salary=("salary", "sum"),
            headcount=("employee_id", "count"),
        )
        .reset_index()
    )


def export_for_bi(headcount_df, absenteeism_df, personnel_cost_df, prefix: str = ""):
    headcount_df.to_csv(f"{prefix}headcount_monthly.csv", index=False)
    absenteeism_df.to_csv(f"{prefix}absenteeism_by_department.csv", index=False)
    personnel_cost_df.to_csv(f"{prefix}personnel_cost_by_department.csv", index=False)


if __name__ == "__main__":
    employees_df, attendance_df = load_hr_tables()
    employees_df, attendance_df = prepare_data(employees_df, attendance_df)
    headcount = compute_headcount_monthly(employees_df, attendance_df)
    absenteeism = compute_absenteeism_by_department(employees_df, attendance_df)
    personnel_cost = compute_personnel_cost_by_department(employees_df)
    export_for_bi(headcount, absenteeism, personnel_cost)
    print("HR analytical tables exported for BI use.")
