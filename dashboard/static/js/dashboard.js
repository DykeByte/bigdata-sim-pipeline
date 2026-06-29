fetch("/api/kpis")
.then(r => r.json())
.then(data => {

    document.getElementById("ventas").innerText =
        "$" + data.ventas.toLocaleString();

    document.getElementById("clientes").innerText =
        data.clientes;

    document.getElementById("productos").innerText =
        data.productos;

    document.getElementById("transacciones").innerText =
        data.transacciones;
});

fetch("/api/canal")
.then(r => r.json())
.then(data => {

    new Chart(
        document.getElementById("canalChart"),
        {
            type: "bar",
            data: {
                labels: data.map(x => x.canal),
                datasets: [{
                    label: "Ventas por Canal",
                    data: data.map(x => x.ventas)
                }]
            }
        }
    );
});

fetch("/api/categoria")
.then(r => r.json())
.then(data => {

    new Chart(
        document.getElementById("categoriaChart"),
        {
            type: "pie",
            data: {
                labels: data.map(x => x.categoria),
                datasets: [{
                    data: data.map(x => x.ventas)
                }]
            }
        }
    );
});

fetch("/api/fecha")
.then(r => r.json())
.then(data => {

    new Chart(
        document.getElementById("fechaChart"),
        {
            type: "line",
            data: {
                labels: data.map(x => x.fecha),
                datasets: [{
                    label: "Ventas por Fecha",
                    data: data.map(x => x.ventas)
                }]
            }
        }
    );
});