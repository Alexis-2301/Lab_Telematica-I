# Laboratorio RabbitMQ: Comunicación asíncrona entre microservicios

Universidad de los Llanos – Ingeniería de Sistemas 2026-II

Dos aplicaciones de consola en Java que simulan dos microservicios independientes comunicados mediante RabbitMQ:

- **Publisher:** publica eventos `PaymentCreated` en formato JSON.
- **Consumer:** lee los eventos de la cola, los imprime y los confirma (ack) para que RabbitMQ los elimine.

```
Publisher --(payment.created)--> Exchange microservices.events --> Cola payment.created.queue --> Consumer
```

## Requisitos

- Windows 10/11
- Docker Desktop en ejecución
- JDK 21 o superior
- Apache Maven 3.9 o superior
- (Opcional) Postman, para crear los recursos y probar con la API HTTP

Verificación rápida:

```powershell
docker --version
java -version
mvn -version
```

## 1. Levantar RabbitMQ

Primera vez (PowerShell o CMD):

```powershell
docker run -d --hostname rabbit-host --name rabbitmq-lab ^
  -p 5672:5672 ^
  -p 15672:15672 ^
  rabbitmq:4-management
```

Si el contenedor ya existe:

```powershell
docker start rabbitmq-lab
```

Panel de administración: http://localhost:15672 (usuario `guest`, contraseña `guest`).

### Nombres usados

| Elemento      | Nombre                  |
|---------------|-------------------------|
| Virtual host  | `/`                     |
| Exchange      | `microservices.events` (direct, durable) |
| Cola          | `payment.created.queue` (durable) |
| Routing key   | `payment.created`       |

Los recursos se pueden crear con Postman (ver la sección siguiente). Además, ambas aplicaciones declaran exchange, cola y binding al iniciar, de forma idempotente, así que funcionan aunque no existan.

### Configuración y pruebas con Postman

Postman usa la API HTTP del plugin de administración de RabbitMQ (puerto 15672) para crear los recursos y probar el flujo sin escribir código.

**Configuración de la colección `RabbitMQ Lab`:**

- Authorization: Basic Auth, usuario `guest`, contraseña `guest`.
- Header: `Content-Type: application/json`.
- URL base: `http://localhost:15672`.
- En las URLs, `%2F` representa el virtual host `/`.

**Requests:**

| # | Acción | Método | URL (sobre `http://localhost:15672`) | Body |
|---|--------|--------|--------------------------------------|------|
| 1 | Crear exchange | PUT | `/api/exchanges/%2F/microservices.events` | `{"type": "direct", "durable": true}` |
| 2 | Crear cola | PUT | `/api/queues/%2F/payment.created.queue` | `{"durable": true}` |
| 3 | Enlazar cola al exchange | POST | `/api/bindings/%2F/e/microservices.events/q/payment.created.queue` | `{"routing_key": "payment.created"}` |
| 4 | Publicar un mensaje | POST | `/api/exchanges/%2F/microservices.events/publish` | ver abajo |
| 5 | Consumir y eliminar un mensaje | POST | `/api/queues/%2F/payment.created.queue/get` | `ackmode: ack_requeue_false` |
| 6 | Consumir sin eliminar | POST | `/api/queues/%2F/payment.created.queue/get` | `ackmode: ack_requeue_true` |
| 7 | Vaciar la cola (purge) | DELETE | `/api/queues/%2F/payment.created.queue/contents` | sin body |

Body de la Request 4 (publicar):

```json
{
  "properties": {},
  "routing_key": "payment.created",
  "payload": "{\"eventType\":\"PaymentCreated\",\"id\":\"PAY-001\",\"amount\":150000,\"status\":\"APPROVED\",\"createdAt\":\"2026-10-08T10:00:00\"}",
  "payload_encoding": "string"
}
```

Body de las Requests 5 y 6 (cambia solo `ackmode`):

```json
{
  "count": 1,
  "ackmode": "ack_requeue_false",
  "encoding": "auto",
  "truncate": 50000
}
```

**Respuestas esperadas:**

- Requests 1 a 3: `201 Created` (o `204 No Content` si el recurso ya existía con la misma configuración).
- Request 4: `{"routed": true}`. Si devuelve `false`, el binding o la routing key no coinciden.
- Request 5: devuelve el mensaje y lo elimina de la cola (simula un consumer que procesó con éxito).
- Request 6: devuelve el mensaje y lo deja en la cola; usar solo para pruebas.
- Request 7: elimina todos los mensajes en estado Ready.

**Orden sugerido:** ejecutar 1, 2 y 3 una sola vez; luego 4 para publicar, 6 para inspeccionar y 5 para consumir. Usar la 7 para limpiar la cola antes de probar las aplicaciones Java.

## 2. Compilar

Desde la carpeta del proyecto:

```powershell
mvn compile
```

## 3. Ejecutar el Consumer

En una terminal:

```powershell
mvn compile exec:java "-Dexec.mainClass=edu.unillanos.rabbitlab.consumer.Consumer"
```

Queda esperando mensajes. Se detiene con `Ctrl+C`.

## 4. Ejecutar el Publisher

En otra terminal:

```powershell
mvn compile exec:java "-Dexec.mainClass=edu.unillanos.rabbitlab.publisher.Publisher"
```

Publica 5 mensajes y termina. Si el consumer está corriendo, los verá aparecer de inmediato; si no, los mensajes quedan almacenados en la cola hasta que el consumer se inicie.

## Formato del mensaje

```json
{
  "eventType": "PaymentCreated",
  "id": "PAY-001",
  "amount": 175000,
  "status": "APPROVED",
  "createdAt": "2026-10-08T01:24:26"
}
```

Campos obligatorios: `eventType`, `id`, `status`, `createdAt`.

## Manejo de errores

- **Publisher:** si RabbitMQ no está disponible o no confirma un mensaje, muestra un mensaje claro y termina con código de error. Usa confirmaciones del broker (`confirmSelect`) y mensajes persistentes.
- **Consumer:** usa ack manual. El mensaje se elimina de la cola solo después de procesarse correctamente. Si el mensaje no es JSON válido o le falta un campo obligatorio, se descarta con `nack` sin reencolar, para evitar ciclos infinitos.
- Ambas aplicaciones informan si no pueden conectarse al broker.

## Estructura del proyecto

```
rabbitmq-lab/
├── pom.xml
├── README.md
└── src/main/java/edu/unillanos/rabbitlab/
    ├── config/RabbitConfig.java      # Conexión y nombres compartidos (infraestructura)
    ├── publisher/Publisher.java      # Microservicio que publica eventos
    └── consumer/Consumer.java        # Microservicio que consume eventos
```

La configuración de infraestructura está separada de la lógica de negocio (`buildEvent` en el publisher y `process` en el consumer).

## Prueba del flujo completo

1. Inicia el consumer.
2. Ejecuta el publisher: los 5 mensajes aparecen en la consola del consumer.
3. En http://localhost:15672 (pestaña **Queues**), `payment.created.queue` queda con 0 mensajes en Ready y 0 en Unacked.
4. Para probar el manejo de errores, publica desde Postman (Request 4) un mensaje al que le falte un campo, por ejemplo sin `status`: el consumer lo descarta e informa el motivo.

## Detener y limpiar

```powershell
docker stop rabbitmq-lab
docker rm rabbitmq-lab
```