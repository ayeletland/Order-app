import os
import io
import csv
import glob
import uuid
import json
from datetime import datetime, timezone
from functools import lru_cache

from flask import (
    Flask, request, render_template, redirect, url_for, session,
    send_file, abort, flash, jsonify
)
import pandas as pd

# ------------ קונפיג ------------
APP_SECRET = os.getenv("APP_SECRET", "change-me")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin123")  # לאבטח בפרודקשן
PORT = int(os.getenv("PORT", "10000"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

ITEMS_XLSX = os.path.join(BASE_DIR, "items.xlsx")
CUSTOMERS_XLSX = os.path.join(BASE_DIR, "customers.xlsx")
CUSTOMER_ITEMS_DIR = os.path.join(BASE_DIR, "customer_items")
EXPORT_MAPPING_XLSX = os.path.join(BASE_DIR, "export_mapping.xlsx")

# קבצי לוג/הזמנות
ORDERS_CSV = os.path.join(DATA_DIR, "orders.csv")  # כל שורה = פריט בהזמנה

# עמודות נדרשות
REQUIRED_ITEMS = {"ItemCode", "ItemName", "Domain", "Category", "SubCategory"}
REQUIRED_CUSTOMERS = {"CustomerNumber", "CustomerName", "SalesManager"}
REQUIRED_CUSTOMER_ITEMS = {"CustomerNumber", "ItemCode"}

# ------------ אפליקציה ------------
app = Flask(__name__)
app.secret_key = APP_SECRET


# ------------ עזרים ------------
def _assert_columns(df: pd.DataFrame, required: set, fname: str):
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{os.path.basename(fname)} missing columns: {missing}")


def _read_excel_safely(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {os.path.basename(path)}")
    return pd.read_excel(path, dtype=str).fillna("")


@lru_cache(maxsize=1)
def load_items() -> pd.DataFrame:
    df = _read_excel_safely(ITEMS_XLSX)

    # תאימות לאחור: אם יש ItemDescription נמפה ל-ItemName
    if "ItemName" not in df.columns and "ItemDescription" in df.columns:
        df = df.rename(columns={"ItemDescription": "ItemName"})

    _assert_columns(df, REQUIRED_ITEMS, ITEMS_XLSX)
    for c in REQUIRED_ITEMS:
        df[c] = df[c].str.strip()

    df = df.drop_duplicates(subset=["ItemCode"]).sort_values(
        ["Domain", "Category", "SubCategory", "ItemName", "ItemCode"], kind="stable"
    )
    return df.reset_index(drop=True)


@lru_cache(maxsize=1)
def load_customers() -> pd.DataFrame:
    df = _read_excel_safely(CUSTOMERS_XLSX)
    _assert_columns(df, REQUIRED_CUSTOMERS, CUSTOMERS_XLSX)
    for c in REQUIRED_CUSTOMERS:
        df[c] = df[c].str.strip()
    # לאפשר חיפוש/מיון נוח
    df["Display"] = df["CustomerNumber"] + " - " + df["CustomerName"]
    return df.drop_duplicates(subset=["CustomerNumber"]).reset_index(drop=True)


@lru_cache(maxsize=1)
def load_customer_items() -> pd.DataFrame:
    # מאחד את כל הקבצים בתקייה customer_items/
    files = sorted(glob.glob(os.path.join(CUSTOMER_ITEMS_DIR, "*.xls*")))
    if not files:
        # אם אין—נחזיר טבלה ריקה עם העמודות הנכונות
        return pd.DataFrame(columns=list(REQUIRED_CUSTOMER_ITEMS))

    frames = []
    for f in files:
        t = _read_excel_safely(f)
        # תאימות לשמות עמודות שכיחים
        rename = {}
        if "CustomerID" in t.columns and "CustomerNumber" not in t.columns:
            rename["CustomerID"] = "CustomerNumber"
        if "item_code" in t.columns and "ItemCode" not in t.columns:
            rename["item_code"] = "ItemCode"
        if rename:
            t = t.rename(columns=rename)

        _assert_columns(t, REQUIRED_CUSTOMER_ITEMS, f)
        for c in REQUIRED_CUSTOMER_ITEMS:
            t[c] = t[c].astype(str).str.strip()
        frames.append(t[list(REQUIRED_CUSTOMER_ITEMS)])

    df = pd.concat(frames, ignore_index=True).drop_duplicates()
    return df.reset_index(drop=True)


def get_cart() -> dict:
    return session.setdefault("cart", {})  # { ItemCode: {..., qty:int} }


def save_cart(cart: dict):
    session["cart"] = cart
    session.modified = True


def _prefill_qtys(items_df: pd.DataFrame, cart: dict) -> pd.DataFrame:
    items_df = items_df.copy()
    items_df["QtyInCart"] = items_df["ItemCode"].map(lambda c: cart.get(c, {}).get("qty", 0))
    return items_df


# ------------ Health ------------
@app.route("/health")
def health():
    return "ok", 200


# ------------ דף ראשי + סינון ------------
@app.route("/", methods=["GET", "POST"])
def order_form():
    customers = load_customers()
    items = load_items()
    cust_items = load_customer_items()

    # פרמטרים מהטופס/URL
    sales_manager = (request.values.get("sales_manager") or "").strip()
    customer_search = (request.values.get("customer_search") or "").strip()
    customer_id = (request.values.get("customer_id") or "").strip()
    items_scope = (request.values.get("items_scope") or "customer").strip()  # 'customer' | 'all'
    domain = (request.values.get("domain") or "").strip()
    category = (request.values.get("category") or "").strip()
    subcategory = (request.values.get("subcategory") or "").strip()
    q = (request.values.get("q") or "").strip()

    # סינון לקוחות
    customers_filtered = customers.copy()
    if sales_manager:
        customers_filtered = customers_filtered[customers_filtered["SalesManager"] == sales_manager]
    if customer_search:
        s = customer_search.lower()
        customers_filtered = customers_filtered[
            customers_filtered["CustomerName"].str.lower().str.contains(s)
            | customers_filtered["CustomerNumber"].str.contains(customer_search)
        ]

    # אם לא נבחר customer_id אבל יש רשימה אחרי סינון—נשאיר ריק. המשתמש יבחר.
    selected_customer = None
    if customer_id:
        selected = customers[customers["CustomerNumber"] == customer_id]
        if not selected.empty:
            selected_customer = selected.iloc[0].to_dict()

    # בסיס סט הפריטים
    if items_scope == "customer" and selected_customer:
        allowed = cust_items[cust_items["CustomerNumber"] == selected_customer["CustomerNumber"]]["ItemCode"]
        items_base = items[items["ItemCode"].isin(allowed)]
    else:
        items_base = items

    # סינוני דומיין/קטגוריות
    if domain:
        items_base = items_base[items_base["Domain"] == domain]
    if category:
        items_base = items_base[items_base["Category"] == category]
    if subcategory:
        items_base = items_base[items_base["SubCategory"] == subcategory]

    # חיפוש חופשי
    if q:
        qs = q.lower()
        items_base = items_base[
            items_base["ItemCode"].str.contains(q)
            | items_base["ItemName"].str.lower().str.contains(qs)
            | items_base["Domain"].str.lower().str.contains(qs)
            | items_base["Category"].str.lower().str.contains(qs)
            | items_base["SubCategory"].str.lower().str.contains(qs)
        ]

    # שמירת כמויות מהטופס (לא מאפסת סל!)
    cart = get_cart()
    if request.method == "POST":
        posted = request.form.to_dict(flat=False)
        qtys = posted.get("qty", [])
        codes = posted.get("code", [])
        for code, qty_str in zip(codes, qtys):
            try:
                qty = int(qty_str or 0)
            except ValueError:
                qty = 0
            if qty > 0:
                row = items[items["ItemCode"] == code]
                if row.empty:
                    continue
                r = row.iloc[0]
                cart[code] = {
                    "qty": qty,
                    "ItemCode": r["ItemCode"],
                    "ItemName": r["ItemName"],
                    "Domain": r["Domain"],
                    "Category": r["Category"],
                    "SubCategory": r["SubCategory"],
                }
            else:
                # אם המשתמש רוקן—נמחוק מהסל
                cart.pop(code, None)
        save_cart(cart)
        # נשארים באותו דף עם אותם פרמטרים
        return redirect(url_for("order_form",
                                sales_manager=sales_manager,
                                customer_search=customer_search,
                                customer_id=customer_id,
                                items_scope=items_scope,
                                domain=domain,
                                category=category,
                                subcategory=subcategory,
                                q=q))

    # מילוי כמויות קיימות
    items_view = _prefill_qtys(items_base, cart)

    # דרופדאונים לדומיין/קטגוריה מתבססים על הסט הנוכחי (בהתאם ל־items_scope)
    domains = sorted(items[(items_scope=="all")][["Domain"]].drop_duplicates()["Domain"]) if False else \
              sorted(items_base["Domain"].drop_duplicates())
    categories = sorted(items_base["Category"].drop_duplicates())
    subcategories = sorted(items_base["SubCategory"].drop_duplicates())

    sales_managers = sorted(load_customers()["SalesManager"].drop_duplicates())

    return render_template(
        "index.html",
        sales_managers=sales_managers,
        customers=customers_filtered.to_dict(orient="records"),
        selected_customer=selected_customer,
        items=items_view.to_dict(orient="records"),
        items_scope=items_scope,
        sales_manager=sales_manager,
        customer_search=customer_search,
        customer_id=customer_id,
        domain=domain, category=category, subcategory=subcategory, q=q,
        cart_size=sum(x.get("qty", 0) for x in cart.values()),
        domains=domains, categories=categories, subcategories=subcategories
    )


# ------------ סל ------------
@app.route("/cart", methods=["GET", "POST"])
def cart_view():
    items = load_items()
    cart = get_cart()

    if request.method == "POST":
        # עדכון כמויות/מחיקות מתוך הסל
        for code, entry in list(cart.items()):
            qty_str = request.form.get(f"qty_{code}", "")
            try:
                qty = int(qty_str or 0)
            except ValueError:
                qty = entry.get("qty", 0)
            if qty <= 0:
                cart.pop(code, None)
            else:
                cart[code]["qty"] = qty
        save_cart(cart)
        return redirect(url_for("cart_view"))

    # בניית טבלת תצוגה
    rows = []
    for code, entry in cart.items():
        r = items[items["ItemCode"] == code]
        if r.empty:
            continue
        rows.append({
            "ItemCode": code,
            "ItemName": entry["ItemName"],
            "Domain": entry["Domain"],
            "Category": entry["Category"],
            "SubCategory": entry["SubCategory"],
            "qty": entry["qty"]
        })

    rows = sorted(rows, key=lambda r: (r["Domain"], r["Category"], r["SubCategory"], r["ItemName"], r["ItemCode"]))
    return render_template("cart.html", rows=rows, cart_size=sum(x["qty"] for x in cart.values()))


# ------------ שליחת הזמנה (שומר לקובץ CSV) ------------
@app.route("/submit-order", methods=["POST"])
def submit_order():
    cart = get_cart()
    if not cart:
        flash("אין פריטים בסל.")
        return redirect(url_for("order_form"))

    customer_id = request.form.get("customer_id") or ""
    customers = load_customers()
    c = customers[customers["CustomerNumber"] == customer_id]
    if c.empty:
        flash("לקוח לא נבחר או לא תקין.")
        return redirect(url_for("cart_view"))

    cust = c.iloc[0].to_dict()
    order_id = str(uuid.uuid4())[:8].upper()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # כתיבה ל-CSV (שורה לכל פריט)
    is_new = not os.path.exists(ORDERS_CSV)
    with open(ORDERS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow([
                "OrderID", "TimestampUTC", "CustomerNumber", "CustomerName", "SalesManager",
                "ItemCode", "ItemName", "Domain", "Category", "SubCategory", "Quantity"
            ])
        for code, entry in cart.items():
            w.writerow([
                order_id, ts, cust["CustomerNumber"], cust["CustomerName"], cust["SalesManager"],
                entry["ItemCode"], entry["ItemName"], entry["Domain"], entry["Category"], entry["SubCategory"],
                entry["qty"]
            ])

    # איפוס סל
    session["cart"] = {}
    flash(f"הזמנה {order_id} נשמרה בהצלחה.")
    return redirect(url_for("order_form", customer_id=cust["CustomerNumber"]))


# ------------ ייצוא לאקסל (אדמין) ------------
def _load_export_mapping():
    if not os.path.exists(EXPORT_MAPPING_XLSX):
        # ברירת מחדל: מיפוי 1:1 לסאפ דמה
        return pd.DataFrame([
            {"Field": "OrderID", "SAPField": "DOCNUM", "Order": 1},
            {"Field": "TimestampUTC", "SAPField": "DOCDATE", "Order": 2},
            {"Field": "CustomerNumber", "SAPField": "CUSTOMER", "Order": 3},
            {"Field": "CustomerName", "SAPField": "CUSTNAME", "Order": 4},
            {"Field": "SalesManager", "SAPField": "AGENT", "Order": 5},
            {"Field": "ItemCode", "SAPField": "ITEMCODE", "Order": 6},
            {"Field": "ItemName", "SAPField": "ITEMNAME", "Order": 7},
            {"Field": "Quantity", "SAPField": "QTY", "Order": 8},
        ])
    m = _read_excel_safely(EXPORT_MAPPING_XLSX)
    m = m.rename(columns={c: c.strip() for c in m.columns})
    _assert_columns(m, {"Field", "SAPField", "Order"}, EXPORT_MAPPING_XLSX)
    return m


@app.route("/admin/export")
def admin_export():
    token = request.args.get("token", "")
    if token != ADMIN_TOKEN:
        abort(403)

    # טווח תאריכים (UTC) בפורמט YYYY-MM-DD
    date_from = request.args.get("from", "")
    date_to = request.args.get("to", "")

    if not os.path.exists(ORDERS_CSV):
        return jsonify({"message": "no orders yet"}), 200

    df = pd.read_csv(ORDERS_CSV, dtype=str).fillna("")
    # סינון לפי תאריכים אם סופק
    if date_from:
        df = df[df["TimestampUTC"] >= f"{date_from} 00:00:00"]
    if date_to:
        df = df[df["TimestampUTC"] <= f"{date_to} 23:59:59"]

    mapping = _load_export_mapping().sort_values("Order")
    # בונים טבלה לפי סדר עמודות המיפוי
    cols_in = [c for c in mapping["Field"] if c in df.columns]
    df = df[cols_in].copy()
    rename_map = dict(zip(mapping["Field"], mapping["SAPField"]))
    df = df.rename(columns=rename_map)

    # יצוא לאקסל בזיכרון
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="EXPORT", index=False)
    output.seek(0)
    filename = f"export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(output, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ------------ main ------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
