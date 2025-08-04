from flask import Flask, render_template, request, redirect, send_file
import pandas as pd
from datetime import datetime
import os

app = Flask(__name__)

ITEMS_FILE = "items.xlsx"
ORDERS_FILE = "orders.xlsx"

items_df = pd.read_excel(ITEMS_FILE)

if not os.path.exists(ORDERS_FILE):
    pd.DataFrame(columns=["OrderID","CustomerID","ItemCode","Quantity","OrderDate"]).to_excel(ORDERS_FILE,index=False)

@app.route("/", methods=["GET","POST"])
def order_form():
    customers = items_df["CustomerID"].unique()
    if request.method == "POST":
        customer = request.form.get("customer")
        order_items = []
        for item_code in items_df[items_df["CustomerID"] == customer]["ItemCode"]:
            qty = request.form.get(f"qty_{item_code}")
            if qty and qty.isdigit() and int(qty) > 0:
                order_items.append({
                    "OrderID": int(datetime.now().timestamp()),
                    "CustomerID": customer,
                    "ItemCode": item_code,
                    "Quantity": int(qty),
                    "OrderDate": datetime.now()
                })
        if order_items:
            df_orders = pd.read_excel(ORDERS_FILE)
            df_orders = pd.concat([df_orders, pd.DataFrame(order_items)], ignore_index=True)
            df_orders.to_excel(ORDERS_FILE, index=False)
        return redirect("/success")
    default_items = items_df[items_df["CustomerID"] == customers[0]].to_dict(orient="records")
    return render_template("form.html", customers=customers, items=default_items)

@app.route("/success")
def success():
    return "✅ ההזמנה נרשמה בהצלחה! <a href='/'>חזרה לטופס</a> | <a href='/download'>📄 הורד קובץ הזמנות</a>"

@app.route("/download")
def download_orders():
    return send_file(ORDERS_FILE, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)))
