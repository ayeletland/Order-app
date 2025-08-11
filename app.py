from __future__ import annotations

import io
import os
import glob
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_file,
    Response,
    jsonify,
)

# -----------------------------
# Config – נתיבים ושמות קבצים
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_XLSX = os.path.join(BASE_DIR, "customers.xlsx")
ITEMS_XLSX = os.path.join(BASE_DIR, "items.xlsx")
CUSTOMER_ITEMS_DIR = os.path.join(BASE_DIR, "customer_items")
ORDERS_XLSX = os.path.join(BASE_DIR, "orders.xlsx")  # אחסון ההזמנות

# עמודות חובה
REQUIRED_CUSTOMERS = {"CustomerNumber", "CustomerName", "SalesManager"}
REQUIRED_ITEMS = {"ItemCode", "ItemDescription", "Category", "SubCategory", "Domain"}

# Flask
app = Flask(__name__)

# -----------------------------
# Utils – טעינת נתונים
# -----------------------------
def _assert_columns(df: pd.DataFrame, required: set, fname: str) -> None:
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{os.path.basename(fname)} missing columns: {missing}")

def load_customers() -> pd.DataFrame:
    df = pd.read_excel(CUSTOMERS_XLSX, dtype={"CustomerNumber": str})
    _assert_columns(df, REQUIRED_CUSTOMERS, CUSTOMERS_XLSX)
    # ניקוי קל
    df["CustomerNumber"] = df["CustomerNumber"].str.strip()
    df["CustomerName"] = df["CustomerName"].astype(str).str.strip()
    df["SalesManager"] = df["SalesManager"].astype(str).str.strip()
    # הסרת כפילויות אם יש
    df = df.drop_duplicates(subset=["CustomerNumber"]).reset_index(drop=True)
    return df

def load_items() -> pd.DataFrame:
    df = pd.read_excel(ITEMS_XLSX, dtype={"ItemCode": str})
    _assert_columns(df, REQUIRED_ITEMS, ITEMS_XLSX)
    df["ItemCode"] = df["ItemCode"].str.strip()
    return df

def load_customer_allowed_items(customer_number: str) -> Optional[pd.DataFrame]:
    """
    קורא את קבצי הלקוח מתיקיית customer_items.
    תמיכה בקובץ יחיד {Cust}.xlsx או כמה קבצים בשם {Cust}_*.xlsx.
    מחזיר DF עם ItemCode, אחרת None אם אין קובץ – משמע מותר כל הפריטים.
    """
    pattern_main = os.path.join(CUSTOMER_ITEMS_DIR, f"{customer_number}.xlsx")
    pattern_multi = os.path.join(CUSTOMER_ITEMS_DIR, f"{customer_number}_*.xlsx")
    files = [*glob.glob(pattern_main), *glob.glob(pattern_multi)]
    if not files:
        return None

    frames = []
    for f in files:
        try:
            df = pd.read_excel(f, dtype={"ItemCode": str})
            # תומך באחת משתי אפשרויות שמות עמודות
            if "ItemCode" not in df.columns and "MaterialNumber" in df.columns:
                df = df.rename(columns={"MaterialNumber": "ItemCode"})
            if "ItemCode" not in df.columns:
                continue
            df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
            frames.append(df[["ItemCode"]].dropna())
        except Exception:
            continue

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True).drop_duplicates()
    return merged

# -----------------------------
# עזר לסינון לקוחות
# -----------------------------
def filter_customers(
    customers: pd.DataFrame,
    sales_manager: Optional[str],
    query: Optional[str],
) -> pd.DataFrame:
    df = customers.copy()
    if sales_manager and sales_manager.strip():
        df = df[df["SalesManager"].str.casefold() == sales_manager.strip().casefold()]

    if query and query.strip():
        q = query.strip().casefold()
        by_number = df["CustomerNumber"].str.contains(q, case=False, na=False)
        by_name = df["CustomerName"].str.casefold().str.contains(q, na=False)
        df = df[by_number | by_name]

    return df.sort_values(by=["CustomerName", "CustomerNumber"]).reset_index(drop=True)

# -----------------------------
# נתיב בריאות ל-Render
# -----------------------------
@app.get("/health")
def health() -> Tuple[str, int]:
    return "ok", 200

# -----------------------------
# דף ראשי – טופס הזמנה
# -----------------------------
@app.get("/")
def order_form():
    customers = load_customers()
    items = load_items()

    # פרמטרים מה-UI לסינון לקוחות
    sales_manager = request.args.get("sm", "").strip()
    customer_query = request.args.get("cq", "").strip()
    selected_customer = request.args.get("cid", "").strip()

    filtered_customers = filter_customers(customers, sales_manager, customer_query)

    # אם נבחר לקוח – נסנן את רשימת הפריטים לפי קבצי הלקוח
    df_items_view = items.copy()
    if selected_customer:
        allowed = load_customer_allowed_items(selected_customer)
        if allowed is not None and not allowed.empty:
            df_items_view = df_items_view.merge(allowed, on="ItemCode", how="inner")

    # הפיכת רשימות לתבנית
    sales_managers = sorted(customers["SalesManager"].dropna().unique().tolist())
    customers_list = filtered_customers[["CustomerNumber", "CustomerName"]].to_dict("records")

    # קיבוץ פריטים לפי קטגוריה/תת-קטגוריה (למיון בתבנית)
    df_items_view = df_items_view.sort_values(
        by=["Category", "SubCategory", "ItemDescription", "ItemCode"]
    ).reset_index(drop=True)

    return render_template(
        "form.html",
        sales_managers=sales_managers,
        customers=customers_list,
        selected_manager=sales_manager,
        customer_query=customer_query,
        selected_customer=selected_customer,
        items=df_items_view.to_dict("records"),
    )

# -----------------------------
# קבלת הזמנה ושמירתה
# -----------------------------
@app.post("/submit")
def submit_order():
    """
    מצפה לשדות:
    - customer_id
    - delivery_date (אופציונלי)
    - order_rows: רשימת שורות {ItemCode, Quantity} בכמות > 0
    """
    data = request.get_json(silent=True) or request.form.to_dict(flat=False)

    # תמיכה גם ב-form וגם ב-JSON
    customer_id = (data.get("customer_id") or [""])[0] if isinstance(data, dict) else ""
    delivery_date = (data.get("delivery_date") or [""])[0] if isinstance(data, dict) else ""
    order_rows = data.get("order_rows", [])

    if not customer_id:
        return jsonify({"ok": False, "error": "Missing customer_id"}), 400

    # המרת delivery_date
    ref_date = datetime.utcnow().strftime("%d%m%Y")  # Reference Date בפורמט DDMMYYYY

    # קריאת קובץ הזמנות קיים (אם יש)
    existing = []
    if os.path.exists(ORDERS_XLSX):
        try:
            existing_df = pd.read_excel(ORDERS_XLSX, dtype=str)
            existing = existing_df.to_dict("records")
        except Exception:
            existing = []

    # Order Number רץ: 1 + המקסימום הקיים
    try:
        next_order_num = (
            max([int(r.get("OrderNumber", 0)) for r in existing], default=0) + 1
        )
    except Exception:
        next_order_num = 1

    # המרת שורות להזמנה: נשמור רק כמות > 0
    rows = []
    for row in order_rows:
        try:
            code = str(row.get("ItemCode", "")).strip()
            qty = float(row.get("Quantity", 0))
        except Exception:
            continue
        if not code or qty <= 0:
            continue

        rows.append(
            {
                # שדות משתנים לפי הדרישה
                "OrderNumber": str(next_order_num),
                "CustomerNumber": str(customer_id),
                "MaterialNumber": code,
                "OrderQuantity": qty,
                "CustomerReferenceDate": ref_date,
                # שדות קבועים לדוגמת SAP – ניתן להתאים לפי הצורך
                "SalesOrderType": "ZOR",
                "SalesOrg": "1652",
                "DistributionChannel": "01",
                "Division": "01",
                "SoldToParty": str(customer_id),
                "ShipToParty": str(customer_id),
                "CustomerPOReference": "Pepperi Backup",
                "UnitOfMeasure": "CS",
                "PurchaseOrderType": "EXO",
            }
        )

    if not rows:
        return jsonify({"ok": False, "error": "No items with quantity > 0"}), 400

    # כתיבה לאקסל (מצטבר, לא מוחק)
    final_df = pd.DataFrame([*existing, *rows])
    final_df.to_excel(ORDERS_XLSX, index=False)

    return jsonify({"ok": True, "order_number": next_order_num, "rows": len(rows)})

# -----------------------------
# ייצוא קובץ הזמנות (אדמין)
# תמיכה במסננים: from_date (DDMMYYYY), to_date (DDMMYYYY)
# -----------------------------
@app.get("/export")
def export_orders():
    if not os.path.exists(ORDERS_XLSX):
        # קובץ ריק
        empty = pd.DataFrame(
            columns=[
                "OrderNumber",
                "CustomerNumber",
                "MaterialNumber",
                "OrderQuantity",
                "CustomerReferenceDate",
                "SalesOrderType",
                "SalesOrg",
                "DistributionChannel",
                "Division",
                "SoldToParty",
                "ShipToParty",
                "CustomerPOReference",
                "UnitOfMeasure",
                "PurchaseOrderType",
            ]
        )
        buf = io.BytesIO()
        empty.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f"orders_export_{datetime.utcnow():%Y%m%d_%H%M}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    df = pd.read_excel(ORDERS_XLSX, dtype=str)
    from_date = request.args.get("from_date", "").strip()
    to_date = request.args.get("to_date", "").strip()

    def _to_dt(s: str) -> Optional[datetime]:
        try:
            return datetime.strptime(s, "%d%m%Y")
        except Exception:
            return None

    if from_date or to_date:
        fdt = _to_dt(from_date) if from_date else None
        tdt = _to_dt(to_date) if to_date else None
        # ממירים את השדה במידת הצורך
        df["_dt"] = df["CustomerReferenceDate"].apply(_to_dt)
        if fdt:
            df = df[df["_dt"] >= fdt]
        if tdt:
            df = df[df["_dt"] <= tdt]
        df = df.drop(columns=["_dt"])

    # ייצוא
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    filename = f"orders_export_{datetime.utcnow():%Y%m%d_%H%M}.xlsx"
    return send_file(
        buf,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------
# הרצה ישירה (לוקאלי/Render)
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    # מאפשר להריץ גם מקומית וגם על Render ללא Gunicorn
    app.run(host="0.0.0.0", port=port)
