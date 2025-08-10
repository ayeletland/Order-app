from flask import Flask, render_template, request, redirect, Response, url_for
import pandas as pd
from datetime import datetime
import glob, os, io

app = Flask(__name__)

# -----------------------------
# נתיבי קבצים
# -----------------------------
ITEMS_XLSX = "items.xlsx"                  # עמודות: ItemCode, ItemDescription, Domain, Category, SubCategory
CUSTOMERS_XLSX = "customers.xlsx"          # עמודות: CustomerID, CustomerName, SalesManager (אפשרי ריק)
CUSTOMER_ITEMS_DIR = "customer_items"      # קבצי *.xlsx עם: CustomerID, ItemCode
ORDERS_CSV = "orders.csv"                  # שמירת הזמנות (דמו) - בהמשך נחליף ל-DB

SAP_CONST = {
    "Sales Order Type": "ZOR",
    "Sales Org": "1652",
    "Distribution Channel": "01",
    "Division": "01",
    "Customer PO Reference": "Pepperi Backup",
    "Unit of Measure": "CS",
    "Purchase order type": "EXO",
}

# -----------------------------
# טעינת נתונים + נירמול
# -----------------------------
def _strip_df(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def load_items():
    if not os.path.exists(ITEMS_XLSX):
        raise FileNotFoundError(f"{ITEMS_XLSX} not found")
    df = pd.read_excel(ITEMS_XLSX, dtype=str).fillna("")
    expected = {"ItemCode", "ItemDescription", "Domain", "Category", "SubCategory"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"items.xlsx missing columns: {missing}")
    return _strip_df(df, list(expected))

def load_customers():
    if not os.path.exists(CUSTOMERS_XLSX):
        raise FileNotFoundError(f"{CUSTOMERS_XLSX} not found")
    df = pd.read_excel(CUSTOMERS_XLSX, dtype=str).fillna("")
    must = {"CustomerID", "CustomerName"}
    missing = must - set(df.columns)
    if missing:
        raise ValueError(f"customers.xlsx missing columns: {missing}")
    if "SalesManager" not in df.columns:
        df["SalesManager"] = ""
    return _strip_df(df, ["CustomerID", "CustomerName", "SalesManager"])

def load_customer_items():
    if not os.path.isdir(CUSTOMER_ITEMS_DIR):
        raise FileNotFoundError(f"{CUSTOMER_ITEMS_DIR} folder not found")
    files = glob.glob(os.path.join(CUSTOMER_ITEMS_DIR, "*.xlsx"))
    if not files:
        raise FileNotFoundError(f"No Excel files found in {CUSTOMER_ITEMS_DIR}")
    frames = []
    for f in files:
        d = pd.read_excel(f, dtype=str).fillna("")
        expected = {"CustomerID", "ItemCode"}
        missing = expected - set(d.columns)
        if missing:
            raise ValueError(f"{os.path.basename(f)} missing columns: {missing}")
        d = _strip_df(d, ["CustomerID", "ItemCode"])
        frames.append(d[["CustomerID", "ItemCode"]])
    return pd.concat(frames, ignore_index=True)

def ensure_orders_csv():
    if not os.path.exists(ORDERS_CSV):
        pd.DataFrame(columns=[
            "order_id", "order_number_export", "customer_id", "created_at",
            "delivery_date", "item_code", "quantity", "uom"
        ]).to_csv(ORDERS_CSV, index=False, encoding="utf-8")

def next_order_number_export():
    ensure_orders_csv()
    df = pd.read_csv(ORDERS_CSV, dtype=str)
    if df.empty:
        return 1
    try:
        return int(pd.to_numeric(df["order_number_export"], errors="coerce").max()) + 1
    except Exception:
        return 1

def save_order(customer_id: str, delivery_date: str, line_items: list):
    """
    line_items: list of dicts {item_code:str, quantity:float}
    """
    ensure_orders_csv()
    now = datetime.utcnow()
    created_at_iso = now.isoformat()
    order_num = next_order_number_export()
    order_id = f"{int(now.timestamp())}"

    rows = []
    for li in line_items:
        rows.append({
            "order_id": order_id,
            "order_number_export": order_num,
            "customer_id": customer_id,
            "created_at": created_at_iso,
            "delivery_date": delivery_date or "",
            "item_code": li["item_code"],
            "quantity": li["quantity"],
            "uom": SAP_CONST["Unit of Measure"],
        })
    df_old = pd.read_csv(ORDERS_CSV, dtype=str) if os.path.exists(ORDERS_CSV) else pd.DataFrame()
    df_new = pd.DataFrame(rows)
    pd.concat([df_old, df_new], ignore_index=True).to_csv(ORDERS_CSV, index=False, encoding="utf-8")
    return order_num

def customers_with_display(df_customers: pd.DataFrame) -> pd.DataFrame:
    df = df_customers.copy()
    df["Display"] = df["CustomerID"].astype(str) + " – " + df["CustomerName"].astype(str)
    return df

def items_for_customer(df_items, df_customer_items, customer_id: str):
    allowed = df_customer_items[df_customer_items["CustomerID"] == str(customer_id)]
    return df_items.merge(allowed, on="ItemCode", how="inner")

# -----------------------------
# מסך ראשי
# -----------------------------
@app.route("/", methods=["GET", "POST"])
def order_form():
    admin_mode = request.args.get("admin") == "1"

    df_items = load_items()
    df_customers = load_customers()
    df_cust_items = load_customer_items()
    df_customers_disp = customers_with_display(df_customers)

    # POST = שמירת הזמנה
    if request.method == "POST":
        customer_id = (request.form.get("customer_id") or "").strip()
        delivery_date = (request.form.get("delivery_date") or "").strip()  # YYYY-MM-DD
        if not customer_id:
            return "❌ יש לבחור לקוח. <a href='/'>חזרה</a>"

        posted = request.form.to_dict(flat=True)
        line_items = []
        for k, v in posted.items():
            if not k.startswith("qty_"):
                continue
            code = k.replace("qty_", "")
            try:
                qty = float(v) if v.strip() != "" else 0.0
            except Exception:
                qty = 0.0
            if qty > 0:
                line_items.append({"item_code": code, "quantity": qty})

        if not line_items:
            return "❌ לא הוזנו כמויות מעל 0. <a href='/'>חזרה</a>"

        order_num = save_order(customer_id, delivery_date, line_items)
        return redirect(url_for("success", order=order_num, admin=("1" if admin_mode else None)))

    # GET = סינוני לקוחות
    sales_managers = sorted([sm for sm in df_customers["SalesManager"].unique() if sm])
    customer_search = (request.args.get("customer_search") or "").strip()
    sales_manager_filter = (request.args.get("sales_manager") or "").strip()
    selected_customer = (request.args.get("customer_id") or "").strip()

    df_cust_filtered = df_customers_disp.copy()

    sm_arg = sales_manager_filter.lower()
    if sm_arg:
        df_cust_filtered = df_cust_filtered[
            df_cust_filtered["SalesManager"].str.strip().str.lower() == sm_arg
        ]

    if customer_search:
        q = customer_search.lower()
        df_cust_filtered = df_cust_filtered[
            df_cust_filtered["CustomerID"].str.strip().str.lower().str.contains(q) |
            df_cust_filtered["CustomerName"].str.strip().str.lower().str.contains(q)
        ]

    # אחרי בחירת לקוח: סינון פריטים
    items_for_ui = pd.DataFrame(columns=df_items.columns)
    domains, categories, subcats = [], [], []
    if selected_customer:
        df_allowed = items_for_customer(df_items, df_cust_items, selected_customer)
        for col in ["Domain", "Category", "SubCategory"]:
            if col in df_allowed.columns:
                df_allowed[col] = df_allowed[col].astype(str).str.strip()

        domain_filter = (request.args.get("domain") or "").strip()
        category_filter = (request.args.get("category") or "").strip()
        subcat_filter = (request.args.get("subcat") or "").strip()
        item_search = (request.args.get("item_search") or "").strip().lower()

        domains = sorted([d for d in df_allowed["Domain"].unique() if d])
        if domain_filter:
            df_allowed = df_allowed[df_allowed["Domain"] == domain_filter]

        categories = sorted([c for c in df_allowed["Category"].unique() if c])
        if category_filter:
            df_allowed = df_allowed[df_allowed["Category"] == category_filter]

        subcats = sorted([s for s in df_allowed["SubCategory"].unique() if s])
        if subcat_filter:
            df_allowed = df_allowed[df_allowed["SubCategory"] == subcat_filter]

        if item_search:
            df_allowed = df_allowed[
                df_allowed["ItemCode"].str.lower().str.contains(item_search) |
                df_allowed["ItemDescription"].str.lower().str.contains(item_search)
            ]

        items_for_ui = df_allowed.copy()

    customers_list = df_cust_filtered.sort_values("CustomerID")[["CustomerID", "CustomerName", "Display"]].to_dict(orient="records")
    items_list = items_for_ui[["ItemCode", "ItemDescription", "Domain", "Category", "SubCategory"]].to_dict(orient="records")

    return render_template(
        "form.html",
        is_admin=admin_mode,
        sales_managers=sales_managers,
        customers=customers_list,
        selected_customer=selected_customer,
        items=items_list,
        domains=["(All)"] + domains if domains else [],
        categories=["(All)"] + categories if categories else [],
        subcats=["(All)"] + subcats if subcats else [],
    )

@app.route("/success")
def success():
    order = request.args.get("order")
    admin = request.args.get("admin")
    extra = "?admin=1" if admin == "1" else ""
    return f"✅ ההזמנה נשמרה (Order Number: {order}). <a href='/{extra}'>חזרה לטופס</a> | <a href='/admin/export{extra}'>📄 ייצוא ל-SAP</a>"

# -----------------------------
# ייצוא CSV במבנה SAP (Admin)
# -----------------------------
@app.route("/admin/export")
def export_sap():
    if request.args.get("admin") != "1":
        return "⛔ Admin only. הוסיפי ?admin=1 לכתובת.", 403

    ensure_orders_csv()
    if not os.path.exists(ORDERS_CSV):
        return "אין הזמנות לייצא."

    df_orders = pd.read_csv(ORDERS_CSV, dtype=str)
    if df_orders.empty:
        return "אין הזמנות לייצא."

    date_from = request.args.get("from")  # YYYY-MM-DD
    date_to   = request.args.get("to")
    customer  = request.args.get("customer")

    def to_dt(s):
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

    df_orders["created_at_dt"] = df_orders["created_at"].apply(to_dt)

    if date_from:
        try:
            dfrom = datetime.fromisoformat(date_from + "T00:00:00")
            df_orders = df_orders[df_orders["created_at_dt"] >= dfrom]
        except Exception:
            pass

    if date_to:
        try:
            dto = datetime.fromisoformat(date_to + "T23:59:59")
            df_orders = df_orders[df_orders["created_at_dt"] <= dto]
        except Exception:
            pass

    if customer:
        df_orders = df_orders[df_orders["customer_id"] == str(customer)]

    if df_orders.empty:
        return "לא נמצאו שורות להזמנה תחת הסינון שבחרת."

    def ddmmyyyy(dt):
        if not isinstance(dt, datetime):
            return ""
        return dt.strftime("%d%m%Y")

    rows = []
    for _, r in df_orders.iterrows():
        created_dt = r["created_at_dt"]
        rows.append({
            "Order Number": int(r["order_number_export"]) if str(r["order_number_export"]).isdigit() else r["order_number_export"],
            "Sales Order Type": SAP_CONST["Sales Order Type"],
            "Sales Org": SAP_CONST["Sales Org"],
            "Distribution Channel": SAP_CONST["Distribution Channel"],
            "Division": SAP_CONST["Division"],
            "Sold to Party": r["customer_id"],
            "Ship to Party": r["customer_id"],
            "Customer PO Reference": SAP_CONST["Customer PO Reference"],
            "Customer Reference Date": ddmmyyyy(created_dt) if created_dt else "",
            "Material Number": r["item_code"],
            "Order Quantity": r["quantity"],
            "Unit of Measure": SAP_CONST["Unit of Measure"],
            "Purchase order type": SAP_CONST["Purchase order type"],
        })

    df_export = pd.DataFrame(rows)
    try:
        df_export = df_export.sort_values(by=["Order Number", "Material Number"], kind="stable")
    except Exception:
        pass

    out = io.StringIO()
    df_export.to_csv(out, index=False, encoding="utf-8")
    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=sap_orders_export.csv"}
    )

# -----------------------------
# run (לריצה מקומית/Render)
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=True)
