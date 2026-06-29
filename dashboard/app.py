from flask import Flask, render_template, jsonify
import sqlite3
from pathlib import Path

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "warehouse" / "retailstart.db"


def query(sql):

    conn = sqlite3.connect(DB_PATH)

    data = conn.execute(sql).fetchall()

    conn.close()

    return data


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/api/kpis")
def kpis():

    total_ventas = query("""
        SELECT ROUND(SUM(monto_total),0)
        FROM Fact_Ventas
    """)[0][0]

    total_clientes = query("""
        SELECT COUNT(*)
        FROM Dim_Cliente
    """)[0][0]

    total_productos = query("""
        SELECT COUNT(*)
        FROM Dim_Producto
    """)[0][0]

    total_transacciones = query("""
        SELECT COUNT(*)
        FROM Fact_Ventas
    """)[0][0]

    return jsonify({
        "ventas": total_ventas,
        "clientes": total_clientes,
        "productos": total_productos,
        "transacciones": total_transacciones
    })


@app.route("/api/canal")
def ventas_canal():

    data = query("""
        SELECT
            dc.canal,
            SUM(fv.monto_total)
        FROM Fact_Ventas fv
        JOIN Dim_Canal dc
        ON fv.sk_canal = dc.sk_canal
        GROUP BY dc.canal
    """)

    return jsonify([
        {
            "canal": r[0],
            "ventas": r[1]
        }
        for r in data
    ])


@app.route("/api/categoria")
def ventas_categoria():

    data = query("""
        SELECT
            dp.categoria,
            SUM(fv.monto_total)
        FROM Fact_Ventas fv
        JOIN Dim_Producto dp
        ON fv.sk_producto = dp.sk_producto
        GROUP BY dp.categoria
    """)

    return jsonify([
        {
            "categoria": r[0],
            "ventas": r[1]
        }
        for r in data
    ])


@app.route("/api/fecha")
def ventas_fecha():

    data = query("""
        SELECT
            dt.fecha,
            SUM(fv.monto_total)
        FROM Fact_Ventas fv
        JOIN Dim_Tiempo dt
        ON fv.sk_tiempo = dt.sk_tiempo
        GROUP BY dt.fecha
        ORDER BY dt.fecha
    """)

    return jsonify([
        {
            "fecha": r[0],
            "ventas": r[1]
        }
        for r in data
    ])


if __name__ == "__main__":
    app.run(debug=True)