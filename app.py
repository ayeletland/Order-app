from __future__ import annotations

import io
import os
import glob
from datetime import datetime
from typing import Optional, Tuple, Dict, Iterable

import pandas as pd
from flask import (
    Flask, render_template, request, send_file, jsonify
)

# -----------------------------
# Config – נתיבים ושמות קבצים
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CUSTOMERS_XLSX = os.path.join(BASE_DIR, "customers.xlsx")
ITEMS_XLSX = os.path.join(BASE_DIR, "items.xlsx")
CUSTOMER_ITEMS_DIR = os.path.join(BASE_DIR, "customer_items")
ORDERS_XLSX = os.path.join(BASE_DIR, "orders.xlsx")  # אחסון ההזמנות המצטברות

# שמות עמודות "קנוניים" אחרי נרמול
CANON_CUSTOMERS = {
    "customernumber": "CustomerNumber",  # תומך גם ב-CustomerID/Number
    "customerid": "CustomerNumber",
    "customername": "CustomerName",
    "salesmanager": "SalesManager",
}
CANON_ITEMS = {
    "itemcode": "ItemCode",             # תומך גם ב-MaterialNumber
    "materialnumber": "ItemCode",
    "itemdescription": "ItemDescription",
    "category": "Category",
    "subcategory": "SubCategory",
    "domain": "Domain",
}

REQUIRED_CUSTOMERS = {"CustomerNumber", "CustomerName", "SalesManager"}
REQUIRED_ITEMS = {"ItemCode", "ItemDescription", "Category", "SubCategory", "Domain"}

# Flask
app = Flask(__name__)

# -----------------------------
# Utils
# -----------------------------
def get_arg(*names: Iterable[str], default: str = "") -> str:
    """קורא פרמטר בקשות ותומך בכמה שמות חלופיים."""
    for n in names:
        val = request.args.get(n)
        if val is not None:
            return val.strip()
    return default

def normalize_headers(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    מנרמל כותרות טבלה: lowercase, ללא רווחים/קווים תחתונים, וממפה לשמות הקנוניים.
    לדוגמה 'Customer ID' -> 'customernumber' -> 'CustomerNumber'.
    """
    raw_cols = list(df.columns)
    norm_to_raw = {}
    for c in raw_cols:
        key = str(c).strip().lower().replace(" ", "").replace("_", "")
        norm_to_raw[key] = c

    # החלפת שמות לעמודות הקנוניות אם יש התאמות
    rename_dict = {}
    for norm_key, raw_name in norm_to_raw.items():
        if norm_key in mapping:
            rename_dict[raw_name] = mapping[norm_key]
        else:
            # אם לא ידוע – השאר את השם המקורי
            rename_dict[raw_name] = raw_name

    df = df.rename(columns=rename_dict)
    return df

def assert_required(df: pd.DataFrame, required: set, fname: str) -> None:
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{os.path.basename(fname)} missing columns: {missing}")

def safe_read_excel(path: str, dtype: dict | None = None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {os.path.basename(path)}")
    # engine=openpyxl עוזר להפחית שוני בפרשנות
    return pd.read_excel(path, dtype=dtype or {}, engine="openpyxl")

# -----------------------------
# טעינת נתונים
# -----------------------------
def load_customers() -> pd.DataFrame:
    df = safe_read_excel(CUSTOMERS_XLSX, dtype=str)
    df = normalize_headers(df, CANON_CUSTOMERS)
    assert_required(df, REQUIRED_CUSTOMERS, CUSTOMERS_XLSX)

    # ניקוי
    df["CustomerNumber"] = df["CustomerNumber"].astype(str).str.strip()
    df["CustomerName"] = df["CustomerName"].astype(str).str.strip()
    df["SalesManager"] = df["SalesManager"].astype(str).str.strip()

    # הסרת כפילויות
    df = df.drop_duplicates(subset=["CustomerNumber"]).reset_index(drop=True)
    return df

def load_items() -> pd.DataFrame:
    df = safe_read_excel(ITEMS_XLSX, dtype=str)
    df = normalize_headers(df, CANON_ITEMS)
    assert_required(df, REQUIRED_ITEMS, ITEMS_XLSX)

    df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
    df["ItemDescription"] = df["ItemDescription"].astype(str).str.strip()
    df["Category"] = df["Category"].astype(str).str.strip()
    df["SubCategory"] = df["SubCategory"].astype(str).str.strip()
    df["Domain"] = df["Domain"].astype(str).str.strip()
    return df

def load_customer_allowed_items(customer_number: str) -> Optional[pd.DataFrame]:
    """
    קורא קבצי פריטים לפי לקוח מתיקיית customer_items.
    תומך:
    - {Cust}.xlsx
    - {Cust}_*.xlsx (פיצול לכמה קבצים)
    מצפה לעמודה ItemCode או MaterialNumber.
    """
    pattern_main = os.path.join(CUSTOMER_ITEMS_DIR, f"{customer_number}.xlsx")
    pattern_multi = os.path.join(CUSTOMER_ITEMS_DIR, f"{customer_number}_*.xlsx")
    files = [*glob.glob(pattern_main), *glob.glob(pattern_multi)]
    if not files:
        return None

    frames = []
    for f in files:
        try:
            df = pd.read_excel(f, dtype=str, engine="openpyxl")
            df = normalize_headers(df, {"itemcode": "ItemCode", "materialnumber": "ItemCode"})
            if "ItemCode" not in df.columns:
                continue
            df["ItemCode"] = df["ItemCode"].astype(str).str.strip()
            frames.append(df[["ItemCode"]].dropna())
        except Exception as ex:
            print(f"[WARN] could not read {os.path.basename(f)}: {ex}")

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True).drop_duplicates()
    return merged

# -----------------------------
# סינון לקוחות
# -----------------------------
def filter_customers(customers: pd.DataFrame, sales_manager: str, query: str) -> pd.DataFrame:
    df = customers.copy()

    if sales_manager:
        df = df[df["SalesManager"].str.casefold() == sales_manager.casefold()]

    if query:
        q = query.casefold()
        by_number = df["CustomerNumber"].str.contains(q, case=False, na=False)
        by_name = df["CustomerName"].str.casefold().str.contains(q, na=False)
        df = df[by_number | by_name]

    return df.sort_values(by=["CustomerName", "CustomerNumber"]).reset_index(drop=True)

# -----------------------------
# Health
# -----------------------------
@app.get("/health")
def health() -> Tuple[str, int]:
    return "ok", 200

# -----------------------------
# UI ראשי – טופס הזמנה
# -----------------------------
@app.get("/")
def order_form():
    try:
        customers = load_customers()
        items = load_items()
    except Exception as ex:
        # הודעת שגיאה ידידותית בדף הראשי במקום 500
        return f"<h3>Data error</h3><pre>{ex}</pre>", 200

    # תמיכה בשמות פרמטרים ארוכים/קצרים מה-UI
    sales_manager = get_arg("sm", "sales_manager")
    customer_query = get_arg("cq", "customer_search")
    selected_customer = get_arg("cid", "customer_id")

    filtered_customers = filter_customers(customers, sales_manager, customer_query)

    # אם נבחר לקוח – הגבל פריטים מותריים
    df_items_view = items.copy()
    if selected_customer:
        allowed = load_customer_allowed_items(selected_customer)
        if allowed is not None and not allowed.empty:
            df_items_view = df_items_view.merge(allowed, on="ItemCode", how="inner")

    # מיון ידידותי להצגה
    df_items_view = df_items_view.sort_values(
        by=["Category", "SubCategory", "ItemDescription", "ItemCode"]
    ).reset_index(drop=True)

    sales_managers = sorted(customers["SalesManager"].dropna().unique().tolist())
    customers_list = filtered_customers[["CustomerNumber", "CustomerName"]].to_dict("records")

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
    גוף הבקשה (JSON או form):
    - customer_id (חובה)
    - delivery_date (אופציונלי, לא נשמר כרגע לקובץ SAP אלא לשימוש עתידי)
    - order_rows: [{ItemCode, Quantity}, ...] (נשמרים רק עם Quantity > 0)
    """
    data = request.get_json(silent=True)
    if not data:
        # תמיכה ב-form
        data = request.form.to_dict(flat=False)
        customer_id = (data.get("customer_id") or [""])[0]
        delivery_date = (data.get("delivery_date") or [""])[0]
        order_rows = []
    else:
        customer_id = str(data.get("customer_id", "")).strip()
        delivery_date = str(data.get("delivery_date", "")).strip()
        order_rows = data.get("order_rows", [])

    if not customer_id:
        return jsonify({"ok": False, "error": "Missing customer_id"}), 400

    # Reference Date בפורמט DDMMYYYY
    ref_date = datetime.utcnow().strftime("%d%m%Y")

    # קריאת הזמנות קיימות
    existing: list[dict] = []
    if os.path.exists(ORDERS_XLSX):
        try:
            existing_df = pd.read_excel(ORDERS_XLSX, dtype=str, engine="openpyxl")
            existing = existing_df.to_dict("records")
        except Exception as ex:
            print(f"[WARN] failed reading {os.path.basename(ORDERS_XLSX)}: {ex}")

    # מספר הזמנה רץ
    try:
        next_order_num = max([int(r.get("OrderNumber", 0)) for r in existing] or [0]) + 1
    except Exception:
        next_order_num = 1

    # בניית שורות שמורות (רק Quantity > 0)
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
                # משתנה לפי הדרישה
                "OrderNumber": str(next_order_num),
                "CustomerNumber": str(customer_id),
                "MaterialNumber": code,
                "OrderQuantity": qty,
                "CustomerReferenceDate": ref_date,
                # קבועים (ניתנים לשינוי לפי הצורך)
                "SalesOrderType": "ZOR",
                "SalesOrg": "1652",
                "DistributionChannel": "01",
                "Division": "01",
                "SoldToParty": str(customer_id),
                "ShipToParty": str(customer_id),
                "CustomerPOReference": "Pepperi Backup",
                "UnitOfMeasure": "CS",
                "PurchaseOrderType": "EXO",
                # מידע שאולי נרצה להוסיף בעתיד:
                "DeliveryDate": delivery_date or "",
            }
        )

    if not rows:
        return jsonify({"ok": False, "error": "No items with quantity > 0"}), 400

    # כתיבה מצטברת (לא מוחק הזמנות עבר)
    final_df = pd.DataFrame([*existing, *rows])
    final_df.to_excel(ORDERS_XLSX, index=False, engine="openpyxl")

    return jsonify({"ok": True, "order_number": next_order_num, "rows": len(rows)})

# -----------------------------
# ייצוא הזמנות (עם סינון תאריכים אופציונלי)
# from_date/to_date בפורמט DDMMYYYY
# -----------------------------
@app.get("/export")
def export_orders():
    columns_order = [
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
        "DeliveryDate",
    ]

    if not os.path.exists(ORDERS_XLSX):
        # קובץ ריק במבנה נכון
        empty = pd.DataFrame(columns=columns_order)
        buf = io.BytesIO()
        empty.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f"orders_export_{datetime.utcnow():%Y%m%d_%H%M}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    df = pd.read_excel(ORDERS_XLSX, dtype=str, engine="openpyxl")

    # ודא שכל העמודות קיימות גם אם נוספו מאוחר יותר
    for col in columns_order:
        if col not in df.columns:
            df[col] = ""

    from_date = get_arg("from_date")
    to_date = get_arg("to_date")

    def _to_dt(s: str) -> Optional[datetime]:
        try:
            return datetime.strptime(s, "%d%m%Y")
        except Exception:
            return None

    if from_date or to_date:
        fdt = _to_dt(from_date) if from_date else None
        tdt = _to_dt(to_date) if to_date else None
        df["_dt"] = df["CustomerReferenceDate"].apply(_to_dt)
        if fdt:
            df = df[df["_dt"] >= fdt]
        if tdt:
            df = df[df["_dt"] <= tdt]
        df = df.drop(columns=["_dt"])

    df = df[columns_order]  # סדר עמודות עקבי

    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"orders_export_{datetime.utcnow():%Y%m%d_%H%M}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------
# הרצה ישירה (לוקאלי/Render)
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
