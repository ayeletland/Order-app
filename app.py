import os
import glob
import sqlite3
from datetime import datetime, date
from typing import Dict, Any, List

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, Response, abort, flash
)
import pandas as pd

# -----------------------------
# Config
# -----------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

CUSTOMERS_XLSX = os.path.join(BASE_DIR, "customers.xlsx")
ITEMS_XLSX = os.path.join(BASE_DIR, "items.xlsx")
CUSTOMER_ITEMS_DIR = os.path.join(BASE_DIR, "customer_items")

DB_PATH = os.path.join(BASE_DIR, "orders.db")

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")

EXPORT_TEMPLATE_XLSX = os.path.join(BASE_DIR, "export_template.xlsx")
EXPORT_STATE_JSON = os.path.join(BASE_DIR, "export_state.json")  # לשמירת "מאז הייצוא האחרון"

# שדות נדרשים בקבצי הנתונים
REQUIRED_CUSTOMERS = {"CustomerNumber", "CustomerName", "SalesManager"}
REQUIRED_ITEMS = {"ItemCode", "ItemName", "Domain", "Category", "SubCategory"}
REQUIRED_CUST_ITEMS = {"CustomerNumber", "ItemCode"}

# ברירת מחדל לשדות יצוא אם אין export_template.xlsx
DEFAULT_EXPORT_FIXED = {
    "DocType": "ZSTD",
    "SalesOrg": "1000",
    "DistrChannel": "10",
    "Division": "00",
    "Plant": "1000",
    "Currency": "ILS",
}
# סדר עמודות ברירת מחדל אם אין תבנית
DEFAULT_EXPORT_ORDER = [
    "OrderSerial", "ReferenceDate",
    "CustomerNumber", "ItemCode", "Quantity",
    "DocType", "SalesOrg", "DistrChannel", "Division", "Plant", "Currency"
]

# -----------------------------
# Flask
# -----------------------------
app = Flask(__name__)
app.secret_key = SECRET_KEY


# -----------------------------
# Utilities
# -----------------------------
def _assert_columns(df: pd.DataFrame, required: set, fname: str):
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{os.path.basename(fname)} missing columns: {missing}")

def _read_excel_required(path: str, required: set) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_excel(path)
    _assert_columns(df, required, path)
    return df

def load_customers() -> pd.DataFrame:
    df = _read_excel_required(CUSTOMERS_XLSX, REQUIRED_CUSTOMERS).copy()
    # סוגי עזר לחיפוש
    df["CustomerNumber"] = df["CustomerNumber"].astype(str)
    df["CustomerName_lc"] = df["CustomerName"].astype(str).str.lower()
    df["CustomerNumber_lc"] = df["CustomerNumber"].str.lower()
    return df

def load_items() -> pd.DataFrame:
    df = _read_excel_required(ITEMS_XLSX, REQUIRED_ITEMS).copy()
    df["ItemCode"] = df["ItemCode"].astype(str)
    df["ItemName"] = df["ItemName"].astype(str)
    # מיון לפי Domain → Category → SubCategory → ItemName
    df = df.sort_values(["Domain", "Category", "SubCategory", "ItemName"], kind="stable")
    # עזר לחיפוש
    df["ItemName_lc"] = df["ItemName"].str.lower()
    df["ItemCode_lc"] = df["ItemCode"].str.lower()
    return df

def load_customer_items() -> pd.DataFrame:
    if not os.path.isdir(CUSTOMER_ITEMS_DIR):
        # אם אין תיקייה — נחזיר ריק תקין
        return pd.DataFrame(columns=list(REQUIRED_CUST_ITEMS))
    frames = []
    for p in glob.glob(os.path.join(CUSTOMER_ITEMS_DIR, "*.xlsx")):
        df = pd.read_excel(p)
        _assert_columns(df, REQUIRED_CUST_ITEMS, p)
        frames.append(df[["CustomerNumber", "ItemCode"]].copy())
    if not frames:
        return pd.DataFrame(columns=list(REQUIRED_CUST_ITEMS))
    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    out["CustomerNumber"] = out["CustomerNumber"].astype(str)
    out["ItemCode"] = out["ItemCode"].astype(str)
    return out

def ensure_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_number TEXT NOT NULL,
            sales_manager TEXT,
            created_at TEXT NOT NULL,
            delivery_date TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            item_code TEXT NOT NULL,
            quantity REAL NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
    """)
    con.commit()
    con.close()

def get_con():
    return sqlite3.connect(DB_PATH)

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")

# -----------------------------
# Health
# -----------------------------
@app.route("/health")
def health():
    return "ok", 200

# -----------------------------
# Session cart helpers
# session['cart'] = { "<CustomerNumber>": { "<ItemCode>": {"qty": float, "name": str}, ... } }
# -----------------------------
def get_cart() -> Dict[str, Dict[str, Any]]:
    return session.setdefault("cart", {})

def get_customer_cart(cust_no: str) -> Dict[str, Any]:
    cart = get_cart()
    return cart.setdefault(cust_no, {})

def set_customer_cart(cust_no: str, data: Dict[str, Any]):
    cart = get_cart()
    cart[cust_no] = data
    session["cart"] = cart  # לסמן שינויים

# -----------------------------
# Export template (dynamic)
# קובץ export_template.xlsx עם עמודות: Field, DefaultValue
# -----------------------------
def load_export_template():
    if os.path.exists(EXPORT_TEMPLATE_XLSX):
        df = pd.read_excel(EXPORT_TEMPLATE_XLSX)
        if {"Field", "DefaultValue"}.issubset(df.columns):
            fields = df["Field"].astype(str).tolist()
            defaults = df.set_index("Field")["DefaultValue"].to_dict()
            return fields, defaults
    # אחרת: ברירת מחדל
    return DEFAULT_EXPORT_ORDER, DEFAULT_EXPORT_FIXED

# -----------------------------
# Main page: select customer + items view
# -----------------------------
@app.route("/", methods=["GET"])
def order_form():
    ensure_db()
    customers = load_customers()
    items = load_items()
    cust_items = load_customer_items()

    # --- לקלוט פרמטרים מה-URL
    sales_manager = request.args.get("sales_manager", "").strip()
    customer_search = (request.args.get("customer_search", "") or "").strip().lower()
    customer_id = (request.args.get("customer_id", "") or "").strip()

    show_set = request.args.get("show_set", "customer")  # 'customer' / 'all'
    q = (request.args.get("q", "") or "").strip().lower()
    domain = request.args.get("domain", "").strip()
    category = request.args.get("category", "").strip()
    subcategory = request.args.get("subcategory", "").strip()

    # --- סינון רשימת לקוחות
    cust_df = customers.copy()
    if sales_manager:
        cust_df = cust_df[cust_df["SalesManager"].astype(str) == sales_manager]
    if customer_search:
        # חיפוש גם בשם וגם במספר
        mask = cust_df["CustomerName_lc"].str.contains(customer_search) | \
               cust_df["CustomerNumber_lc"].str.contains(customer_search)
        cust_df = cust_df[mask]

    # --- רשימת פריטים להצגה (רק אם יש לקוח נבחר)
    filtered_items = pd.DataFrame(columns=items.columns)
    if customer_id:
        if show_set == "customer":
            # פריטי הלקוח
            allowed = cust_items[cust_items["CustomerNumber"] == customer_id]["ItemCode"].unique().tolist()
            filtered_items = items[items["ItemCode"].isin(allowed)].copy()
        else:
            # כל הפריטים
            filtered_items = items.copy()

        # סינונים נוספים
        if domain:
            filtered_items = filtered_items[filtered_items["Domain"] == domain]
        if category:
            filtered_items = filtered_items[filtered_items["Category"] == category]
        if subcategory:
            filtered_items = filtered_items[filtered_items["SubCategory"] == subcategory]
        if q:
            mask = filtered_items["ItemName_lc"].str.contains(q) | filtered_items["ItemCode_lc"].str.contains(q)
            filtered_items = filtered_items[mask]

        # מיון עקבי
        filtered_items = filtered_items.sort_values(
            ["Domain", "Category", "SubCategory", "ItemName"],
            kind="stable"
        )

    # cart קיים ללקוח?
    customer_cart = get_customer_cart(customer_id) if customer_id else {}

    # ערכי Domain/Category/SubCategory להצגה במסננים (לפי הקונטקסט)
    context_items = items.copy() if show_set == "all" or not customer_id else filtered_items.copy()
    domains = sorted(context_items["Domain"].dropna().unique().tolist())
    categories = sorted(context_items["Category"].dropna().unique().tolist())
    subcategories = sorted(context_items["SubCategory"].dropna().unique().tolist())

    sales_managers = sorted(customers["SalesManager"].dropna().unique().tolist())

    return render_template(
        "index.html",
        customers=cust_df,
        sales_managers=sales_managers,
        selected_sales_manager=sales_manager,
        customer_id=customer_id,
        customer_search=(request.args.get("customer_search", "") or ""),
        show_set=show_set,
        items=filtered_items,
        q=(request.args.get("q", "") or ""),
        domain_selected=domain,
        category_selected=category,
        subcategory_selected=subcategory,
        domains=domains,
        categories=categories,
        subcategories=subcategories,
        customer_cart=customer_cart
    )

# -----------------------------
# Update cart from items grid
# -----------------------------
@app.route("/update_cart", methods=["POST"])
def update_cart():
    customer_id = request.form.get("customer_id", "").strip()
    if not customer_id:
        flash("בחרי לקוח לפני הוספת פריטים", "warning")
        return redirect(url_for("order_form"))

    items_df = load_items()
    items_df = items_df.set_index("ItemCode")

    cart = get_customer_cart(customer_id)

    # כל שדות הטופס שמתחילים ב qty_
    for key, val in request.form.items():
        if not key.startswith("qty_"):
            continue
        code = key[4:]  # אחרי 'qty_'
        try:
            qty = float(val) if val.strip() != "" else 0
        except:
            qty = 0
        if qty > 0:
            name = items_df.loc[code, "ItemName"] if code in items_df.index else ""
            cart[code] = {"qty": qty, "name": name}
        else:
            # qty == 0 ==> להסיר מהעגלה
            cart.pop(code, None)

    set_customer_cart(customer_id, cart)

    # שמירה על הניווט חזרה עם אותם מסננים
    next_url = request.referrer or url_for("order_form", customer_id=customer_id)
    return redirect(next_url)

# -----------------------------
# Cart view
# -----------------------------
@app.route("/cart")
def cart_view():
    customer_id = request.args.get("customer_id", "").strip()
    if not customer_id:
        flash("לא נבחר לקוח להצגת העגלה", "warning")
        return redirect(url_for("order_form"))
    customers = load_customers()
    customer = customers[customers["CustomerNumber"] == customer_id].head(1)
    cust_name = customer["CustomerName"].iloc[0] if not customer.empty else customer_id

    cart = get_customer_cart(customer_id)
    # טבלת פריטים מלאה לשיוך שם/קטגוריות אם נרצה
    items_df = load_items().set_index("ItemCode")
    lines = []
    for code, rec in cart.items():
        row = {"ItemCode": code, "ItemName": rec.get("name", "")}
        if code in items_df.index:
            row.update({
                "Domain": items_df.loc[code, "Domain"],
                "Category": items_df.loc[code, "Category"],
                "SubCategory": items_df.loc[code, "SubCategory"],
            })
        row["Quantity"] = rec.get("qty", 0)
        lines.append(row)
    df = pd.DataFrame(lines)

    return render_template(
        "cart.html",
        customer_id=customer_id,
        customer_name=cust_name,
        lines=df.to_dict(orient="records")
    )

@app.route("/cart/remove", methods=["POST"])
def cart_remove():
    customer_id = request.form.get("customer_id", "").strip()
    item_code = request.form.get("item_code", "").strip()
    cart = get_customer_cart(customer_id)
    cart.pop(item_code, None)
    set_customer_cart(customer_id, cart)
    return redirect(url_for("cart_view", customer_id=customer_id))

@app.route("/cart/update", methods=["POST"])
def cart_update_line():
    customer_id = request.form.get("customer_id", "").strip()
    item_code = request.form.get("item_code", "").strip()
    try:
        qty = float(request.form.get("quantity", "0").strip())
    except:
        qty = 0
    cart = get_customer_cart(customer_id)
    if qty > 0:
        # עדכון/הוספה
        name = cart.get(item_code, {}).get("name", "")
        if not name:
            items_df = load_items().set_index("ItemCode")
            name = items_df.loc[item_code, "ItemName"] if item_code in items_df.index else ""
        cart[item_code] = {"qty": qty, "name": name}
    else:
        cart.pop(item_code, None)
    set_customer_cart(customer_id, cart)
    return redirect(url_for("cart_view", customer_id=customer_id))

# -----------------------------
# Submit order (persist to DB)
# -----------------------------
@app.route("/submit", methods=["POST"])
def submit_order():
    customer_id = request.form.get("customer_id", "").strip()
    sales_manager = request.form.get("sales_manager", "").strip()
    delivery_date = request.form.get("delivery_date", "").strip()  # אופציונלי (YYYY-MM-DD)

    if not customer_id:
        flash("לא נבחר לקוח", "danger")
        return redirect(url_for("order_form"))

    cart = get_customer_cart(customer_id)
    if not cart:
        flash("עגלת ההזמנה ריקה", "warning")
        return redirect(url_for("order_form", customer_id=customer_id))

    ensure_db()
    con = get_con()
    cur = con.cursor()
    created_at = now_iso()

    cur.execute(
        "INSERT INTO orders (customer_number, sales_manager, created_at, delivery_date) VALUES (?, ?, ?, ?)",
        (customer_id, sales_manager, created_at, delivery_date or None)
    )
    order_id = cur.lastrowid

    for code, rec in cart.items():
        qty = float(rec.get("qty", 0))
        if qty > 0:
            cur.execute(
                "INSERT INTO order_lines (order_id, item_code, quantity) VALUES (?, ?, ?)",
                (order_id, code, qty)
            )

    con.commit()
    con.close()

    # לרוקן עגלה ללקוח
    set_customer_cart(customer_id, {})
    flash(f"הזמנה נשמרה בהצלחה (מס׳ {order_id})", "success")
    return redirect(url_for("order_form", customer_id=customer_id))

# -----------------------------
# Admin + export
# -----------------------------
@app.route("/admin", methods=["GET", "POST"])
def admin():
    if request.method == "POST":
        pwd = request.form.get("password", "")
        if pwd == ADMIN_PASSWORD:
            session["is_admin"] = True
            return redirect(url_for("admin"))
        else:
            flash("סיסמת אדמין שגויה", "danger")
            return redirect(url_for("admin"))
    is_admin = session.get("is_admin", False)
    return render_template("admin.html", is_admin=is_admin)

def _query_orders(from_dt: str = "", to_dt: str = ""):
    ensure_db()
    con = get_con()
    q = """
    SELECT o.id as order_id, o.customer_number, o.created_at, o.delivery_date,
           l.item_code, l.quantity
    FROM orders o
    JOIN order_lines l ON l.order_id = o.id
    WHERE 1=1
    """
    params = []
    if from_dt:
        q += " AND datetime(o.created_at) >= datetime(?)"
        params.append(from_dt)
    if to_dt:
        q += " AND datetime(o.created_at) <= datetime(?)"
        params.append(to_dt)
    q += " ORDER BY o.id ASC, l.id ASC"
    df = pd.read_sql_query(q, con, params=params)
    con.close()
    return df

@app.route("/admin/export.xlsx")
def admin_export():
    if not session.get("is_admin"):
        abort(403)

    scope = request.args.get("scope", "all")  # all | range | since_last
    from_d = request.args.get("from", "").strip()
    to_d = request.args.get("to", "").strip()

    if scope == "range" and not (from_d and to_d):
        return Response("Missing from/to for range", status=400)

    # since_last (פשוט: משתמשים בזיכרון ע״י קובץ state קטן)
    last_ts = None
    if scope == "since_last":
        if os.path.exists(EXPORT_STATE_JSON):
            try:
                last_ts = pd.read_json(EXPORT_STATE_JSON).iloc[0]["last_export"]
            except Exception:
                last_ts = None

    if scope == "range":
        df = _query_orders(from_d, to_d)
    elif scope == "since_last" and last_ts:
        df = _query_orders(last_ts, "")
    else:
        df = _query_orders("", "")

    if df.empty:
        # קובץ ריק אבל תקין
        output = pd.ExcelWriter("export.xlsx", engine="openpyxl")
        pd.DataFrame().to_excel(output, index=False)
        output.close()
        with open("export.xlsx", "rb") as f:
            data = f.read()
        return Response(
            data,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=export.xlsx"}
        )

    # רק שורות Quantity > 0
    df = df[df["quantity"] > 0].copy()

    # שדות יצוא:
    fields_order, fixed_defaults = load_export_template()

    # יצירת סדרת "OrderSerial" — לכל הזמנה אותן ספרות
    # ניצור סידור לפי order_id, והמספר הרץ יהיה פשוט order_id (או מיפוי רציף)
    serial_map = {oid: i for i, oid in enumerate(sorted(df["order_id"].unique()), start=1)}
    df["OrderSerial"] = df["order_id"].map(serial_map)

    # ReferenceDate = תאריך יצירת ההזמנה (תאריך בלבד)
    df["ReferenceDate"] = pd.to_datetime(df["created_at"]).dt.date.astype(str)

    # התאמה לשמות שדות לפי המוסכם:
    df["CustomerNumber"] = df["customer_number"]
    df["ItemCode"] = df["item_code"]
    df["Quantity"] = df["quantity"]

    # נבנה מסגרת Target עם עמודות התבנית
    out = pd.DataFrame(columns=fields_order)
    # קודם נמלא את המשתנים:
    assignable = {
        "OrderSerial": df["OrderSerial"],
        "ReferenceDate": df["ReferenceDate"],
        "CustomerNumber": df["CustomerNumber"],
        "ItemCode": df["ItemCode"],
        "Quantity": df["Quantity"],
    }
    for col, series in assignable.items():
        if col in out.columns:
            out[col] = series.values

    # עכשיו נמלא קבועים מתבנית/ברירת מחדל
    for k, v in fixed_defaults.items():
        if k in out.columns:
            out[k] = v

    # במידה ויש עמודות בתבנית שלא מילאנו ולא קבועות — נשאיר ריק
    # שמירה לאקסל
    tmp_path = os.path.join(BASE_DIR, "export.xlsx")
    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        out.to_excel(writer, index=False)

    # עדכון "מאז הייצוא האחרון"
    if scope in ("all", "range", "since_last"):
        try:
            latest_ts = df["created_at"].max()
            pd.DataFrame([{"last_export": latest_ts}]).to_json(EXPORT_STATE_JSON, orient="records")
        except Exception:
            pass

    with open(tmp_path, "rb") as f:
        data = f.read()

    return Response(
        data,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=export.xlsx"}
    )

# -----------------------------
# Admin: reload data button (לסנכרון קבצים ידני)
# (כאן הטעינה היא מהדיסק בכל בקשה, כך שאין קאש; השארנו את ה-endpoint למקרה תרצי להרחיב)
# -----------------------------
@app.route("/admin/reload", methods=["POST"])
def admin_reload():
    if not session.get("is_admin"):
        abort(403)
    # מאחר ואנחנו טוענים קבצים בכל בקשה, אין מה לנקות פה. נשאיר ללוג עתידי.
    flash("Data reloaded from disk.", "info")
    return redirect(url_for("admin"))

# -----------------------------
# Run (לא בשימוש ב-Render עם gunicorn, אבל נוח להרצה מקומית)
# -----------------------------
if __name__ == "__main__":
    ensure_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
