# TP Coordinación

## Informe de diseño e implementación

### Extensiones agregadas al middleware

Se agregaron tres métodos nuevos:

- **`bind_and_consume_exchange()`** (en `MessageMiddlewareQueueRabbitMQ`): 
  Registra un consumidor adicional sobre el mismo canal que ya usa la cola de este objeto, escuchando un exchange distinto. Permite que una réplica de Sum atienda dos fuentes de eventos (datos de `input_queue` y EOFs) sin abrir una segunda conexión ni un segundo hilo.
- **`publish_to_exchange()`** (en `MessageMiddlewareQueueRabbitMQ`): 
  Publica en un exchange reutilizando el canal de la cola de este objeto, garantizando que la publicación corre en el mismo hilo que ya está consumiendo esa cola.
- **`publish_to_queue()`** 
  (agregado primero en `MessageMiddlewareExchangeRabbitMQ` y en `MessageMiddlewareQueueRabbitMQ`): declara y publica directamente en una cola normal, reutilizando el canal del exchange de este objeto.

Estos tres métodos existen por un único motivo: evitar que un proceso abra más de una conexión TCP a RabbitMQ cuando puede resolver todo con una sola, lo cual resuelve simultáneamente dos problemas: conexiones ociosas cerradas por timeout de heartbeat, y condiciones de carrera entre hilos que comparten estado de aplicación.

## 1. Escenarios

### Escenario 1 — 1 cliente, 1 réplica de cada control

Se incorporó `MessageMiddlewareQueueRabbitMQ` y `MessageMiddlewareExchangeRabbitMQ`, ya implementadas en el trabajo practico anterior.

### Escenario 2 — múltiples clientes, 1 réplica de cada control

Se agregó un `client_id` (UUID, generado una vez por conexión en) que viaja en todos los mensajes del protocolo interno. Sum pasó a indexar su estado (`amount_by_fruit`) por `client_id`. El Gateway, al recibir un resultado, filtra por `client_id != self.client_id` para descartar resultados ajenos.

### Escenario 3 — múltiples clientes, Sum replicado, 1 Aggregation

Se implementó `SUM_CONTROL_EXCHANGE`, un canal de comunicación horizontal entre
réplicas de Sum (Sum↔Sum), separado de la cola de datos, usado exclusivamente para difundir el EOF a todas las réplicas por igual. La réplica que recibe el EOF original del Gateway republica el aviso por este canal; cada réplica (incluida ella misma) hace su propio flush al recibirlo. En Aggregation se agregó un contador de avisos de fin por cliente (`eof_counter`), que solo calcula y envía el top una vez que llegaron `SUM_AMOUNT` avisos.

Se extendió el middleware(`bind_and_consume_exchange` / `publish_to_exchange`) para que una réplica de Sum consuma datos y control sobre el mismo canal, en un único hilo. Como `pika.BlockingConnection` despacha los eventos de un canal estrictamente uno a la vez, la condición de carrera queda eliminada.
Se agregó un set `completed_clients` en Aggregation y en Sum para que un aviso de fin no sea emitido por segunda vez.

### Escenario 4 — múltiples clientes, Sum y Aggregation replicados

Se particionaron los datos de Sum hacia Aggregation por hash de la fruta, de forma que cada fruta sea procesada por una única instancia.

Para el hash se optó por `zlib.crc32`, una función de checksum no criptográfica y
determinística (mismo resultado para el mismo input, en cualquier proceso). Se descartó usar una función criptográfica de `hashlib` dado que con un hash simple alcanza.

Se reescribió el archivo Join para acumular los parciales de cada cliente, contar su llegada hasta
`AGGREGATION_AMOUNT`, fusionarlos y recién entonces enviar un único resultado consolidado.

Se encontró un conexión ociosa en `AggregationFilter` (`output_queue` sin usar hasta el
final del procesamiento de cada cliente). Se resolvió con el mismo criterio aplicado en Sum: reutilizar una única conexión (`input_exchange.publish_to_queue(...)`) en vez de abrir una segunda dedicada solo a la publicación final.
Mismo incoveniente ocurrió `join/main.py`: existían dos conexiones separadas (`input_queue`, con actividad constante, y `output_queue`, sin usar hasta que un cliente completa sus `AGGREGATION_AMOUNT` parciales). Se aplicó exactamente la misma solución: se agregó `publish_to_queue` también a `MessageMiddlewareQueueRabbitMQ` y Join pasó a publicar en `results_queue` reutilizando la conexión de `input_queue` (`self.input_queue.publish_to_queue(OUTPUT_QUEUE, ...)`), sin abrir una segunda conexión.

### Escenario 5 — igual que el 4, con nombres al azar

No requirió cambios de lógica. 

---

## 2. Decisiones de diseño

### 2.1 Una sola conexión por proceso, siempre que sea posible

Cada proceso de Sum, Aggregation y Join quedó reducido, en su versión final, a una única conexión TCP a
RabbitMQ (en Sum y Join, todo el tráfico de entrada y salida pasa por la misma conexión que ya tenían
abierta para consumir; en Aggregation, `input_exchange.publish_to_queue` cumple el mismo rol). Esto
resuelve dos problemas de una sola vez: evita conexiones ociosas cerradas por timeout de heartbeat y elimina la necesidad de sincronización entre hilos.

Al no haber concurrencia interna, una réplica no puede procesar un nuevo mensaje de datos mientras ya está procesando uno. Se consideró aceptable ya que el paralelismo real del sistema proviene de tener varias réplicas, no de que cada réplica internamente sea concurrente.

### 2.2 `SUM_CONTROL_EXCHANGE` como `fanout`, no `direct`

Se implementó un exchange `fanout`, que entrega nativamente a todas las colas bindeadas sin necesitar routing keys: cada réplica se bindea sin conocer nada sobre las demás, y quien origina el aviso publica una única vez.

### 2.3 Partición por hash de la fruta

Se evaluó conscientemente que la partición debía ser por fruta, no por cliente: si se particionara por cliente, una misma fruta mandada por distintos clientes podría terminar en instancias de Aggregation distintas. Particionar por fruta garantiza que todos los aportes de una fruta dada converjan siempre en la misma instancia, que es la única forma de que esa instancia pueda calcular el total real sin necesitar una ronda de comunicación adicional entre instancias de Aggregation.

### 2.4 Idempotencia en las tres etapas (Sum, Aggregation, Join)

Durante el desarrollo ocurria el siguiente problema: un consumidor podía recibir el mismo mensaje más de una vez tras una reconexión o un `nack`. A raíz de ello, se agregó en las tres etapas un registro de "clientes ya finalizados" que descarta cualquier reprocesamiento de un flush o resultado ya enviado. Sin esta primplementación, una entrega duplicada del aviso de fin podía terminar generando un segundo resultado para un cliente que el Gateway ya había atendido y removido de su lista de clientes activos.

## 3. Cómo escala el sistema

### 3.1 Respecto a la cantidad de clientes

El sistema soporta múltiples clientes concurrentes gracias a que cada mensaje del protocolo interno viaja etiquetado con un `client_id`. Cada etapa del pipeline indexa su estado por este identificador en vez de mantener un único estado global:

- Sum acumula `amount_by_fruit` como `{client_id: {fruta: total}}`.
- Aggregation mantiene `fruit_top`, `eof_counter` y `completed_clients` por `client_id`.
- Join mantiene `partials` y `arrival_count` por `client_id`.
- El Gateway usa el `client_id` para descartar, en `deserialize_result_message`, cualquier resultado queno le pertenezca al cliente de ese handler puntual, y así entregarlo por el socket correcto.

No hay ningún límite estructural al número de clientes simultáneos más allá de los recursos físicos disponibles.

### 3.2 Respecto al volumen de datos por cliente

El diseño evita en todo momento acumular los registros crudos de un cliente en memoria — cada etapa solo retiene información ya reducida:

- **Sum nunca guarda los registros individuales**, solo un acumulador `{fruta: total}` por cliente. El consumo de memoria de una réplica de Sum es proporcional a la cantidad de frutas distintas que ese cliente mandó, no a la cantidad de registros, un dataset de 10 registros y uno de 10 millones de registros de las mismas 5 frutas ocupan, en `amount_by_fruit`, exactamente lo mismo.
- **Aggregation** mantiene, por cliente, una lista ordenada de `FruitItem`, una entrada por fruta distinta que le llega (nunca por registro), y solo conserva el resultado agregado.
- **`prefetch_count=1`** en las colas de consumo limita a un mensaje sin confirmar en vuelo por
  consumidor, evitando que una réplica acumule en su propio buffer interno una cantidad no controlada de mensajes pendientes de procesar frente a un volumen de ingesta alto.

### 3.3 Respecto a la cantidad de controles

`SUM_AMOUNT` y `AGGREGATION_AMOUNT` son parámetros conocidos de antemano. Agregar réplicas mejora el rendimiento sin duplicar trabajo gracias a dos mecanismos distintos, uno por etapa:

- **Sum escala por reparto de carga:** todas las réplicas compiten por los mismos mensajes de
  `input_queue`, sin ninguna partición previa, cualquier réplica puede procesar cualquier registro de cualquier cliente. La coordinación necesaria para que esto no rompa la correctitud es el `SUM_CONTROL_EXCHANGE`: garantiza que el EOF de un cliente llegue a todas las réplicas, no solo a la que lo recibió del Gateway, y que Aggregation espere el aporte de las `SUM_AMOUNT` réplicas antes de calcular.
- **Aggregation escala por partición:** cada fruta se enruta, por zlib.crc32(fruta) %
  AGGREGATION_AMOUNT, a una única instancia fija — nunca a todas. Esto es lo que permite que agregar más instancias de Aggregation reduzca el trabajo por instancia (menos frutas distintas le tocan a cada una) en lugar de multiplicarlo (que es lo que pasaría si cada instancia recibiera todo). Join, al recibir aportes ya particionados y disjuntos entre sí, solo necesita concatenar y ordenar una vez por cliente al completar los `AGGREGATION_AMOUNT` parciales esperados.