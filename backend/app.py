from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import pandas as pd
import numpy as np
import os
import json
import random
import queue
import threading
import time
from datetime import datetime

app = Flask(__name__)
CORS(app)

DB_PATH = "fee_management.db"

# ─── DATABASE SETUP ───────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS students (
            student_id TEXT PRIMARY KEY,
            name TEXT,
            department TEXT,
            semester INTEGER
        );

        CREATE TABLE IF NOT EXISTS fee_payments (
            payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT,
            fee_amount REAL,
            paid_amount REAL,
            balance_due REAL,
            payment_date TEXT,
            payment_mode TEXT,
            status TEXT,
            FOREIGN KEY (student_id) REFERENCES students(student_id)
        );
    """)
    conn.commit()
    conn.close()
    print("✅ Database initialized.")

def seed_data():
    conn = get_conn()
    count = pd.read_sql_query("SELECT COUNT(*) as c FROM students", conn)["c"][0]
    if count > 0:
        conn.close()
        print("ℹ️  Data already exists, skipping seed.")
        return

    np.random.seed(42)
    n = 500
    departments = ["CS", "ECE", "MBA", "Civil", "Mechanical"]
    modes = ["Online", "Cheque", "Cash", "DD"]
    statuses = ["Paid", "Pending", "Partial"]

    students = pd.DataFrame({
        "student_id": [f"STU{i:05d}" for i in range(n)],
        "name": [f"Student_{i}" for i in range(n)],
        "department": np.random.choice(departments, n),
        "semester": np.random.randint(1, 9, n),
    })

    payments = pd.DataFrame({
        "student_id": students["student_id"],
        "fee_amount": np.random.choice([45000, 55000, 60000, 70000], n),
        "paid_amount": np.random.randint(0, 70001, n),
        "payment_date": pd.date_range("2023-01-01", periods=n, freq="12h").strftime("%Y-%m-%d"),
        "payment_mode": np.random.choice(modes, n),
        "status": np.random.choice(statuses, n),
    })
    payments["paid_amount"] = payments[["paid_amount", "fee_amount"]].min(axis=1)
    payments["balance_due"] = payments["fee_amount"] - payments["paid_amount"]

    students.to_sql("students", conn, if_exists="append", index=False)
    payments.to_sql("fee_payments", conn, if_exists="append", index=False)
    conn.close()
    print(f"✅ Seeded {n} students and payments.")

# ─── ROUTES ───────────────────────────────────────────────────

@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    conn = get_conn()
    total_students = pd.read_sql_query("SELECT COUNT(*) as c FROM students", conn)["c"][0]
    total_collected = pd.read_sql_query("SELECT SUM(paid_amount) as s FROM fee_payments", conn)["s"][0]
    total_pending = pd.read_sql_query("SELECT SUM(balance_due) as s FROM fee_payments", conn)["s"][0]
    paid_count = pd.read_sql_query("SELECT COUNT(*) as c FROM fee_payments WHERE status='Paid'", conn)["c"][0]
    conn.close()
    return jsonify({
        "total_students": int(total_students),
        "total_collected": round(float(total_collected or 0), 2),
        "total_pending": round(float(total_pending or 0), 2),
        "paid_count": int(paid_count)
    })

@app.route("/api/department-summary", methods=["GET"])
def department_summary():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.department,
               COUNT(*) as total_students,
               SUM(fp.paid_amount) as total_collected,
               SUM(fp.balance_due) as total_pending
        FROM students s
        JOIN fee_payments fp ON s.student_id = fp.student_id
        GROUP BY s.department
        ORDER BY total_collected DESC
    """, conn)
    conn.close()
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/payment-status", methods=["GET"])
def payment_status():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT status, COUNT(*) as count
        FROM fee_payments
        GROUP BY status
    """, conn)
    conn.close()
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/payment-mode", methods=["GET"])
def payment_mode():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT payment_mode, COUNT(*) as count, SUM(paid_amount) as total
        FROM fee_payments
        GROUP BY payment_mode
    """, conn)
    conn.close()
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/students", methods=["GET"])
def get_students():
    dept = request.args.get("department", "")
    status = request.args.get("status", "")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    offset = (page - 1) * limit

    query = """
        SELECT s.student_id, s.name, s.department, s.semester,
               fp.fee_amount, fp.paid_amount, fp.balance_due,
               fp.payment_mode, fp.payment_date, fp.status
        FROM students s
        JOIN fee_payments fp ON s.student_id = fp.student_id
        WHERE 1=1
    """
    params = []
    if dept:
        query += " AND s.department = ?"
        params.append(dept)
    if status:
        query += " AND fp.status = ?"
        params.append(status)

    query += f" LIMIT {limit} OFFSET {offset}"

    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/students/<student_id>", methods=["GET"])
def get_student(student_id):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.student_id, s.name, s.department, s.semester,
               fp.fee_amount, fp.paid_amount, fp.balance_due,
               fp.payment_mode, fp.payment_date, fp.status
        FROM students s
        JOIN fee_payments fp ON s.student_id = fp.student_id
        WHERE s.student_id = ?
    """, conn, params=[student_id])
    conn.close()
    if df.empty:
        return jsonify({"error": "Student not found"}), 404
    return jsonify(df.to_dict(orient="records")[0])

@app.route("/api/add-payment", methods=["POST"])
def add_payment():
    data = request.json
    student_id = data.get("student_id")
    paid_amount = float(data.get("paid_amount", 0))
    payment_mode = data.get("payment_mode", "Online")

    conn = get_conn()
    existing = pd.read_sql_query(
        "SELECT * FROM fee_payments WHERE student_id=?", conn, params=[student_id]
    )
    if existing.empty:
        conn.close()
        return jsonify({"error": "Student not found"}), 404

    fee_amount = float(existing["fee_amount"].iloc[0])
    already_paid = float(existing["paid_amount"].iloc[0])
    new_paid = min(already_paid + paid_amount, fee_amount)
    balance = fee_amount - new_paid
    status = "Paid" if balance == 0 else ("Partial" if new_paid > 0 else "Pending")

    conn.execute("""
        UPDATE fee_payments
        SET paid_amount=?, balance_due=?, status=?, payment_mode=?, payment_date=?
        WHERE student_id=?
    """, (new_paid, balance, status, payment_mode, datetime.now().strftime("%Y-%m-%d"), student_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Payment updated successfully", "new_balance": balance, "status": status})

# ─── KAFKA SIMULATION ─────────────────────────────────────────

kafka_log = []

def run_kafka_simulation():
    topic = queue.Queue()
    depts = ["CS", "ECE", "MBA", "Civil", "Mechanical"]
    modes = ["Online", "Cash", "Cheque", "DD"]

    def producer():
        for i in range(10):
            msg = {
                "event_id": i + 1,
                "student_id": f"STU{random.randint(1,499):05d}",
                "department": random.choice(depts),
                "paid_amount": random.randint(5000, 70000),
                "payment_mode": random.choice(modes),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "produced"
            }
            topic.put(json.dumps(msg))
            kafka_log.append(msg)
            time.sleep(0.2)
        topic.put(None)

    def consumer():
        while True:
            msg = topic.get()
            if msg is None:
                break
            event = json.loads(msg)
            event["type"] = "consumed"
            event["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            kafka_log.append(event)

    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    t2.start()
    t1.start()
    t1.join()
    t2.join()

@app.route("/api/kafka-simulate", methods=["POST"])
def kafka_simulate():
    kafka_log.clear()
    run_kafka_simulation()
    return jsonify({"events": kafka_log, "total": len(kafka_log)})

# ─── ETL ──────────────────────────────────────────────────────

@app.route("/api/run-etl", methods=["POST"])
def run_etl():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.department,
               COUNT(*) as students,
               SUM(fp.paid_amount) as collected,
               SUM(fp.balance_due) as pending,
               AVG(fp.paid_amount) as avg_paid
        FROM students s
        JOIN fee_payments fp ON s.student_id = fp.student_id
        GROUP BY s.department
    """, conn)
    conn.close()
    return jsonify({
        "status": "ETL pipeline executed successfully",
        "rows_processed": 500,
        "summary": df.to_dict(orient="records")
    })

# ─── DATA QUALITY ─────────────────────────────────────────────

@app.route("/api/data-quality", methods=["GET"])
def data_quality():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.student_id, fp.fee_amount, fp.paid_amount, fp.balance_due, fp.status
        FROM students s JOIN fee_payments fp ON s.student_id = fp.student_id
    """, conn)
    conn.close()

    checks = {
        "No null student IDs": bool(df["student_id"].isnull().sum() == 0),
        "Fee amount positive": bool((df["fee_amount"] > 0).all()),
        "Balance non-negative": bool((df["balance_due"] >= 0).all()),
        "Valid status values": bool(df["status"].isin(["Paid","Pending","Partial"]).all()),
        "Paid <= Fee amount": bool((df["paid_amount"] <= df["fee_amount"]).all()),
    }
    passed = sum(checks.values())
    return jsonify({
        "checks": [{"name": k, "passed": v} for k, v in checks.items()],
        "score": f"{passed}/{len(checks)}",
        "all_passed": passed == len(checks)
    })

# ─── MAIN ─────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    seed_data()
    app.run(debug=True, host="0.0.0.0", port=5000)