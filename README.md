# 🛡️ Detector de Fraude Bancario en Tiempo Real con Apache Kafka y Spring Boot

Proyecto práctico de procesamiento de eventos en tiempo real (*stream processing*) desarrollado con **Spring Boot**, **Apache Kafka Streams** y **Docker**. 

El sistema simula el monitoreo continuo de transacciones bancarias para detectar y bloquear operaciones sospechosas en cuestión de milisegundos, demostrando en la práctica la arquitectura de Apache Kafka y el procesamiento *Stream-Native* sin necesidad de clústeres externos como Spark o Flink.

---

## 🏗️ Arquitectura y Conceptos Aplicados

```text
[Cliente / Script] 
       │  POST /api/transacciones (Key: usuarioId)
       ▼
┌──────────────────────────────────────────────────────────┐
│               Spring Boot (Productor REST)               │
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
             ┌─────────────────────────────────┐
             │ Topic: transacciones-bancarias  │
             │   (3 Particiones | Key: Hash)   │
             └────────────────┬────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│        Topología Kafka Streams (application.id)          │
│                                                          │
│  ├──► [Stateless: filter] Monto > $1,000                 │
│  │                                                       │
│  └──► [Stateful: groupByKey + Tumbling Window 1 min]     │
│        └──► count() > 3 tx ──► State Store (RocksDB)     │
└─────────────────────────────┬────────────────────────────┘
                              │
                              ▼
             ┌─────────────────────────────────┐
             │      Topic: alertas-fraude      │
             │         (3 Particiones)         │
             └────────────────┬────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────┐
│   Consumidor de Alertas (Consumer Group Independiente)   │
│   🚨 Ejecuta bloqueo preventivo y notificación en vivo   │
└──────────────────────────────────────────────────────────┘
```

### Mapeo con los fundamentos de Apache Kafka
* **Streaming vs. Colas:** Los eventos se persisten en un *log* distribuido inmutable en lugar de destruirse al primer consumo, permitiendo múltiples consumidores y reprocesamiento (*replay*).
* **Topics y Particiones:** Tanto `transacciones-bancarias` como `alertas-fraude` están configurados con **3 particiones**, habilitando el paralelismo horizontal.
* **Particionado por Clave (`Key`):** El productor envía el `usuarioId` como clave del mensaje. Esto garantiza que todas las transacciones de un mismo cliente caigan siempre en la **misma partición**, preservando su orden cronológico.
* **Operaciones *Stateless* (`filter`):** Evalúa cada transacción de forma aislada sin memoria previa para detectar montos superiores a `$1,000`.
* **Operaciones *Stateful* (`count`) y *Windowing*:** Agrupa las transacciones por usuario dentro de una ventana fija sin solapamiento (**Tumbling Window** de 1 minuto) para detectar ráfagas de más de 3 compras consecutivas.
* **State Store Embebido (RocksDB):** Almacena los conteos parciales localmente con latencia de milisegundos y los respalda automáticamente en un *Changelog Topic* para tolerancia a fallos.

---

## 📋 Requisitos Previos

* **Docker** y **Docker Compose** instalados y en ejecución.
* **Java 17** o **Java 21**.
* **Maven 3.8+** (o el *wrapper* `./mvnw` incluido en el proyecto).
* Terminal con `curl` (Linux/macOS/Git Bash) o PowerShell (Windows).

---

## 🚀 Guía de Ejecución Paso a Paso

### 1. Levantar el clúster de Kafka con Docker
Desde la raíz del proyecto, inicia el *broker* de Kafka (en modo KRaft) y la interfaz visual **Kafka UI**:

```bash
docker compose up -d
```

Verifica que ambos contenedores estén activos:

```bash
docker compose ps
```

> **Panel Visual:** Abre [http://localhost:8080](http://localhost:8080) en tu navegador para inspeccionar los *topics*, el reparto de mensajes entre las 3 particiones y los *offsets* en tiempo real.

### 2. Iniciar la aplicación Spring Boot
Ejecuta el microservicio que contiene el productor REST, la topología de Kafka Streams y el consumidor de alertas:

```bash
./mvnw spring-boot:run
```

Espera hasta ver en la consola que la aplicación inició en el puerto `8081` y que los *topics* fueron creados.

---

## 🧪 Scripts de Prueba del Flujo

### Opción A: Script automatizado en Bash (`test_fraude.sh`)

Guarda el siguiente script como `test_fraude.sh`, dale permisos de ejecución (`chmod +x test_fraude.sh`) y córrelo (`./test_fraude.sh`) para probar los tres escenarios secuencialmente:

```bash
#!/bin/bash

URL="http://localhost:8081/api/transacciones"

echo "=========================================================="
echo "1. ESCENARIO NORMAL: Transacción legítima (< \$1,000)"
echo "   Resultado esperado: Sin alertas."
echo "=========================================================="
curl -s -X POST "$URL" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d '{"usuarioId": "usuario-normal-01", "monto": 45.50, "comercio": "Cafeteria Central"}'
echo -e "\n"
sleep 2

echo "=========================================================="
echo "2. ESCENARIO FRAUDE STATELESS: Monto elevado (> \$1,000)"
echo "   Resultado esperado: 1 alerta inmediata por monto alto."
echo "=========================================================="
curl -s -X POST "$URL" \
  -H "Content-Type: application/json; charset=utf-8" \
  -d '{"usuarioId": "usuario-vip-02", "monto": 2500.00, "comercio": "Joyeria Internacional"}'
echo -e "\n"
sleep 2

echo "=========================================================="
echo "3. ESCENARIO FRAUDE STATEFUL (Tumbling Window 1 min):"
echo "   Enviando 4 transacciones seguidas para 'usuario-rafaga-03'"
echo "   Resultado esperado: Alerta al superar 3 tx en 1 minuto."
echo "=========================================================="
for i in {1..4}
do
  echo "-> Enviando transacción #$i..."
  curl -s -X POST "$URL" \
    -H "Content-Type: application/json; charset=utf-8" \
    -d "{\"usuarioId\": \"usuario-rafaga-03\", \"monto\": 25.00, \"comercio\": \"Tienda Digital #$i\"}"
  echo ""
  sleep 1
done

echo -e "\n✅ Pruebas finalizadas. Revisa la consola de Spring Boot o Kafka UI (http://localhost:8080)."
```

### Opción B: Script automatizado en PowerShell (`test_fraude.ps1`)

Si trabajas en Windows sin Git Bash, ejecuta este script en PowerShell:

```powershell
$url = "http://localhost:8081/api/transacciones"

Write-Host "1. Enviando transacción normal..." -ForegroundColor Cyan
Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" `
  -Body '{"usuarioId": "usuario-normal-01", "monto": 45.50, "comercio": "Cafeteria Central"}'
Start-Sleep -Seconds 2

Write-Host "`n2. Enviando transacción de monto alto (Fraude Stateless)..." -ForegroundColor Yellow
Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" `
  -Body '{"usuarioId": "usuario-vip-02", "monto": 2500.00, "comercio": "Joyeria Internacional"}'
Start-Sleep -Seconds 2

Write-Host "`n3. Enviando ráfaga de 4 transacciones en < 1 min (Fraude Stateful)..." -ForegroundColor Red
for ($i = 1; $i -le 4; $i++) {
    Write-Host "-> Transacción #$i"
    $body = @{
        usuarioId = "usuario-rafaga-03"
        monto     = 25.00
        comercio  = "Tienda Digital #$i"
    } | ConvertTo-Json
    Invoke-RestMethod -Uri $url -Method Post -ContentType "application/json" -Body $body
    Start-Sleep -Seconds 1
}

Write-Host "`nPruebas completadas. Revisa la consola de Spring Boot." -ForegroundColor Green
```

---

## 🔍 Comandos Útiles de Inspección en Kafka (Docker CLI)

Si deseas verificar el funcionamiento interno directamente desde la terminal del contenedor sin usar la interfaz web:

* **Listar los *topics* creados y verificar sus 3 particiones:**
  ```bash
  docker exec -it kafka-broker kafka-topics --bootstrap-server localhost:9092 --describe
  ```

* **Escuchar en vivo el *topic* `alertas-fraude` como un segundo *Consumer Group* independiente (modo `earliest`):**
  ```bash
  docker exec -it kafka-broker kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic alertas-fraude \
    --group auditoria-externa \
    --from-beginning \
    --property print.key=true \
    --property print.partition=true
  ```

* **Consultar el estado de los *Consumer Groups* y sus *offsets*:**
  ```bash
  docker exec -it kafka-broker kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --all-groups \
    --describe
  ```

---

## 🧹 Detener y Limpiar el Entorno

Para detener los contenedores y eliminar los volúmenes de datos generados durante las pruebas:

```bash
docker compose down -v
```
