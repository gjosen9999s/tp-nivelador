# TP0 - Concurrencia y Comunicaciones

## 1. Introducción

El objetivo del trabajo es implementar un sistema distribuido para recibir apuestas de distintas agencias, almacenarlas y realizar un sorteo cuando una cantidad mínima de agencias hayan terminado de enviar sus datos completamente.

El sistema está compuesto por clientes desarrollados en Go y un servidor desarrollado en Python. Cada cliente representa una agencia y se conecta al servidor mediante sockets TCP para enviar sus apuestas. El servidor procesa las conexiones de forma concurrente, almacena las apuestas recibidas y coordina el momento en que se realiza el sorteo.

Para la comunicación entre clientes y el servidor se implementó un protocolo propio, incluyendo la serialización y deserialización de las apuestas y un mecanismo de confirmación de recepción de cada batch. La solución también contempla el procesamiento por lotes para limitar el uso de memoria y mecanismos de sincronización para coordinar las distintas agencias.

## 2. Arquitectura

La solución se divide en módulos con responsabilidades específicas tanto en el cliente como en el servidor.

```
                 ┌─────────────────────────┐
                 │        SERVIDOR         │
                 │                         │
                 │ Main / Server           │
                 │ Protocol                │
                 │ Safe Socket             │
                 │ Coordinator             │
                 │ Lottery                 │
                 └────────────┬────────────┘
                              │
                           TCP│
                              │
              ┌───────────────┼───────────────┐
              │               │               │
        ┌─────┴─────┐   ┌─────┴─────┐   ┌─────┴─────┐
        │  CLIENTE  │   │  CLIENTE  │   │  CLIENTE  │
        │           │   │           │   │           │
        │Main/Client│   │Main/Client│   │Main/Client│
        │   Input   │   │   Input   │   │   Input   │
        │ Protocol  │   │ Protocol  │   │ Protocol  │
        │Safe Socket│   │Safe Socket│   │Safe Socket│
        │  Domain   │   │  Domain   │   │  Domain   │
        └───────────┘   └───────────┘   └───────────┘
          Agencia 0       Agencia 1 ...   Agencia N
```


**Módulos del servidor**

- **Main / Server**: inicia el servidor, acepta conexiones y crea un handler para cada cliente.
- **Protocol**: define el formato de los mensajes y se encarga de serializar y deserializar los datos.
- **Safe Socket**: encapsula las operaciones de envío y recepción completas sobre TCP.
- **Coordinator**: concentra el estado compartido y coordina las agencias mediante el mecanismo de quorum.
- **Lottery**: almacena las apuestas en el archivo CSV temporal y determina los ganadores.

**Módulos del cliente**

- **Main / Client**: inicia el cliente y coordina el flujo general.
- **Input**: lee el archivo de apuestas de forma secuencial y construye batches.
- **Protocol**: serializa las apuestas y construye los mensajes que se envían al servidor.
- **Safe Socket**: garantiza el envío y recepción completos de los mensajes sobre TCP.
- **Domain**: contiene las estructuras de datos propias del dominio, como una apuesta.

Cada agencia establece su propia conexión TCP hacia el servidor. De esta forma, cada cliente posee un socket independiente y el tráfico de una agencia no comparte el canal de comunicación con las demás.

## 3. Protocolo de comunicación

La comunicación entre los clientes y el servidor se realiza mediante sockets TCP. Se definió un protocolo propio para determinar cómo se representan los datos que se envían y cómo se delimita cada mensaje.

Dado que en TCP se recibe un bytestream y no se conservan los límites de los mensajes enviados por el emisor, cada mensaje comienza con un campo de 4 bytes que indica la longitud de su payload.

Tanto los clientes como el servidor utilizan las operaciones SendAll y RecvAll para asegurarse de procesar la totalidad de los bytes correspondientes a cada mensaje.

### 3.1. Estructura general de un mensaje

Todos los mensajes utilizan la siguiente estructura:

```
┌────────────────────────┬──────────────────────────────┐
│ Longitud del payload   │ Payload                      │
│        4 bytes         │                              │
│       uint32 BE        │                              │
└────────────────────────┴──────────────────────────────┘
```

El campo de longitud utiliza un uint32 en formato big-endian y representa únicamente la cantidad de bytes del payload que sigue.

El receptor primero obtiene los 4 bytes correspondientes a la longitud y luego lee exactamente esa cantidad de bytes.

Este mecanismo permite reconstruir los límites de los mensajes independientemente de cómo TCP entregue los datos mediante las operaciones de lectura.

### 3.2. Serialización de una apuesta

Cada apuesta se representa mediante los siguientes campos:

```
┌────────────┬──────────────────┬──────────────────┬────────────┬──────────────────┬────────────┐
│ Agency ID  │ First Name       │ Last Name        │ DNI        │ Birthdate        │ Number     │
│  4 bytes   │ 1 byte + texto   │ 1 byte + texto   │ 4 bytes    │ 1 byte + texto   │ 4 bytes    │
└────────────┴──────────────────┴──────────────────┴────────────┴──────────────────┴────────────┘
```

Los campos numéricos se representan como uint32 en formato big-endian.

Para los campos de texto se almacena primero un byte con la longitud del texto y luego los bytes correspondientes codificados en UTF-8.

El uso de un byte para representar la longitud limita cada campo de texto a 255 bytes, lo cual resulta suficiente para los datos utilizados en el trabajo.

### 3.3. Envío de batches

Las apuestas no se envían individualmente, sino agrupadas en batches.

El payload de un mensaje de batch comienza indicando la cantidad de apuestas que contiene:

```
┌──────────────────────┬─────────────────────────────────────┐
│ Cantidad de apuestas │ Apuesta 1 │ Apuesta 2 │ ... │ N    │
│       4 bytes        │                                     │
└──────────────────────┴─────────────────────────────────────┘
```

El cliente lee el archivo de entrada de forma secuencial y arma batches de tamaño BATCH_SIZE.

Cada batch se serializa y se envía como un único mensaje.

El último batch puede contener menos apuestas que el tamaño configurado.

Después de procesar correctamente cada batch, el servidor envía un mensaje de confirmación (ACK) antes de que el cliente continúe con el siguiente batch.

De esta manera, el procesamiento se realiza de forma incremental y sin cargar la totalidad de los datos en memoria.

### 3.4. ACK y finalización del envío

El ACK forma parte del protocolo de aplicación y se utiliza para confirmar que el servidor recibió y procesó correctamente el batch.

Se representa como un mensaje sin payload: el campo de longitud en cero, es decir exactamente los 4 bytes `\x00\x00\x00\x00` y nada más.

```
┌────────────────────────┬──────────────────────────────┐
│ Longitud del payload   │ (sin payload)                │
│       4 bytes = 0      │                              │
└────────────────────────┴──────────────────────────────┘
```

El receptor lo identifica simplemente por esta característica.

La secuencia para el envío de apuestas es:

```
Cliente                              Servidor

  Batch ───────────────────────────────►
         ◄──────────────────────────── ACK

  Batch ───────────────────────────────►
         ◄──────────────────────────── ACK

  ...
```

Una vez enviado el último batch, el cliente utiliza CloseWrite() sobre la conexión TCP.

Esto indica al servidor que no quedan más datos de entrada, sin cerrar completamente la conexión, ya que el cliente todavía debe permanecer conectado para recibir los ganadores.

No se envía la cantidad total de batches. El final del envío se determina mediante el cierre de la mitad de escritura de la conexión.

### 3.5. Flujo de intercambio de información

El protocolo no define un campo de tipo: hay un solo formato de mensaje (longitud + payload) y lo que varía es el contenido y el sentido de lo que viaja.

- El **cliente** solo envía batches, siempre hacia el servidor.
- El **servidor** responde a cada batch con un **ACK** (payload vacío).
- Cuando el sorteo se habilita, el servidor envía a cada cliente sus **ganadores**, uno por mensaje.

| Mensaje | Sentido | Contenido |
|---|---|---|
| BATCH | Cliente → Servidor | Cantidad de apuestas + apuestas serializadas |
| ACK | Servidor → Cliente | Payload vacío (longitud 0) |
| WINNER | Servidor → Cliente | Una apuesta ganadora serializada |

Los tres usan exactamente el mismo framing por longitud. Para la distinción entre ACK y WINNER no se requiere ningún tag. Como se indicó, el ACK es un mensaje cuyo payload mide 0 bytes y WINNER es un mensaje cuyo payload es una apuesta serializada.

### 3.6. Envío de ganadores

Una vez que una cantidad mínima de agencias finalizaron el envío, se habilita el sorteo y el servidor envía los ganadores a cada cliente que finalizaron.

Cada ganador se envía como un mensaje independiente utilizando el mismo formato general:

```
┌────────────────────────┬───────────────────────┐
│ Longitud del payload   │ Apuesta ganadora      │
│        4 bytes         │ serializada           │
└────────────────────────┴───────────────────────┘
```

El servidor envía a cada cliente únicamente los ganadores correspondientes a su agencia.

El cliente continúa leyendo mensajes hasta recibir el fin de la conexión por parte del servidor.

En resumen, hay dos decisiones centrales en el protocolo:

- Utilizar un prefijo de longitud para delimitar los mensajes, debido a que TCP proporciona un flujo de bytes y no conserva las fronteras de los mensajes.
- Utilizar CloseWrite() para indicar el final del envío de apuestas, evitando tener que comunicar previamente la cantidad total de batches.

## 4. Concurrencia y sincronización

El servidor crea un thread por cada conexión recibida.

Los threads procesan las agencias de forma concurrente y comparten un Coordinator, que concentra el estado necesario para coordinar el procesamiento.

### 4.1. Acceso concurrente al almacenamiento

Las apuestas recibidas de las distintas agencias se acumulan en un archivo CSV temporal.

Como varios handlers pueden recibir y almacenar apuestas simultáneamente, las operaciones de escritura se protegen mediante un `threading.Lock`.

De esta manera, dos threads no pueden modificar simultáneamente el estado protegido del almacenamiento.

### 4.2. Coordinación mediante quorum

El sistema debe esperar a que una cantidad mínima de agencias (el quorum) haya terminado de enviar sus apuestas antes de realizar el sorteo.

Para esto, el Coordinator mantiene un conjunto (`set`) con las agencias que finalizaron su envío.

Cuando un handler termina de recibir las apuestas de una agencia:

1. Registra la agencia en el conjunto de agencias finalizadas.
2. Comprueba si se alcanzó el quorum.
3. Si todavía no se alcanzó, espera mediante una `Condition`.
4. Si se completa el quorum, habilita el sorteo y notifica a los demás threads.
5. Los handlers que estaban esperando se despiertan y continúan con el procesamiento.

El uso de un `set` permite representar explícitamente qué agencias finalizaron y evita contabilizar dos veces una misma agencia.

La `Condition` permite que los threads esperen sin mantener ocupado el lock mientras no se haya alcanzado el quorum.

### 4.3. Estado compartido

El estado mutable compartido se concentra dentro del Coordinator.

Los principales mecanismos utilizados son:

- `Lock` para proteger el acceso concurrente al almacenamiento.
- `Condition` para coordinar la finalización de las agencias.
- `set` para registrar las agencias que finalizaron.
- Un flag de estado para indicar que se alcanzó el quorum o que se inició el shutdown.

Los handlers no necesitan compartir directamente información entre ellos, sino que utilizan el Coordinator como punto central de coordinación.

El cliente, por su parte, es monohilo y no requiere mecanismos de sincronización.

## 5. Manejo de finalización y SIGTERM

La solución contempla la terminación controlada del sistema mediante SIGTERM.

### 5.1. Servidor

El `accept` del servidor utiliza un timeout de un segundo para poder revisar periódicamente el estado de terminación, incluso cuando no llegan nuevas conexiones.

Al recibir SIGTERM, se ejecuta el proceso de shutdown del Coordinator, que actualiza el estado compartido utilizado también por los threads que puedan estar esperando en el mecanismo de quorum. Esto permite que los handlers que estén esperando puedan salir de la espera y finalizar.

Luego se espera durante un tiempo acotado (3 segundos) a que los threads terminen normalmente y envíen sus ganadores. Si algún thread permanece bloqueado, por ejemplo porque un cliente mantiene abierta una conexión sin enviar datos, se cierra su socket para desbloquear la operación de recepción.

Finalmente se realiza `join` de los threads para garantizar que hayan finalizado antes de que termine el proceso principal.

De esta manera, los threads son limpiados explícitamente durante el shutdown y no quedan ejecutándose cuando finaliza el hilo principal, con un tiempo de cierre conocido y menor al que docker espera antes del SIGKILL.

### 5.2. Cliente

Ante la recepción de SIGTERM, el cliente cierra la conexión y finaliza su ejecución con código 0.

## 6. Decisiones de diseño

**Protocolo binario propio**: se implementó una serialización propia en lugar de utilizar formatos como JSON. Permite definir explícitamente el formato de los mensajes, reducir el tamaño de los datos transmitidos y trabaja directamente sobre el concepto de serialización y framing del trabajo.

**Framing mediante longitud**: como TCP no conserva límites de mensajes, se agregó un encabezado de longitud de 4 bytes antes de cada payload. El receptor determina exactamente cuántos bytes debe leer para reconstruir cada mensaje.

**Procesamiento por batches**: las apuestas se procesan en batches en lugar de cargar todo el archivo en memoria. El cliente lee el archivo secuencialmente y reutiliza el espacio reservado para el batch, de modo que el consumo de memoria no crece con la cantidad total de apuestas.

**ACK por batch**: el servidor confirma cada batch antes de que el cliente continúe con el siguiente. Mantiene sincronizado el avance de ambos extremos y permite conocer desde el cliente que el servidor procesó correctamente los datos. El ACK pertenece al protocolo de aplicación y no a las confirmaciones internas de TCP.

**Cierre de la mitad de escritura**: se utiliza CloseWrite() para indicar que la agencia terminó de enviar apuestas, manteniendo abierta la conexión para recibir posteriormente los ganadores. Evita agregar un mensaje adicional para indicar el final del archivo.

**Concurrencia mediante threads**: el servidor utiliza un thread independiente por conexión. Varias agencias pueden enviar sus apuestas simultáneamente sin que el procesamiento de una conexión bloquee la recepción de las demás.

**Coordinación mediante quorum**: el sorteo se coordina por quorum de agencias finalizadas, con una `Condition` para evitar espera activa y permitir que los threads permanezcan bloqueados hasta que exista una condición que les permita continuar.

**Almacenamiento temporal**: el storage se implementa con un archivo CSV temporal que vive durante la ejecución del servidor. No se incorporó persistencia permanente porque no se identificó como requerimiento del presente trabajo.

**Reintentos**: no hay retransmisión de batches ni mecanismos de recuperación de sesión; la confiabilidad de entrega la da TCP y el ACK solo confirma el procesamiento a nivel de aplicación. El cliente sí reintenta la conexión inicial varias veces antes de rendirse.

**Ausencia de autenticación**: no se incorporó autenticación de clientes, ya que no forma parte del alcance requerido para este trabajo.

## 7. Consideraciones sobre errores

Las operaciones de comunicación pueden procesar menos bytes de los solicitados en una única operación.

Por este motivo, el envío y la recepción de mensajes no dependen de que una única llamada a `send` o `recv` procese la totalidad de los datos: `SendAll` continúa enviando hasta completar el buffer solicitado y `RecvAll` continúa recibiendo hasta obtener exactamente la cantidad de bytes requerida.

Una finalización de conexión antes de recibir todos los bytes esperados se considera un mensaje incompleto y se trata como un error de protocolo.

En cambio, la finalización normal de la conexión cuando ya no existen mensajes pendientes se utiliza como mecanismo válido para indicar el fin del envío (servidor) o el final de los ganadores (cliente).

## 8. Conclusión

El trabajo implementa un sistema distribuido compuesto por múltiples clientes y un servidor que se comunican mediante sockets TCP utilizando un protocolo binario propio.

Las decisiones de diseño buscan mantener separadas las responsabilidades de comunicación, protocolo, dominio, almacenamiento y coordinación, facilitando tanto el razonamiento sobre la solución como su mantenimiento y extensión.