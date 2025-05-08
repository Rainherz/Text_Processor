import threading
from time import sleep
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, flash
import amqpstorm
from amqpstorm import Message
import os
import datetime
import json
from urllib.parse import urlparse

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Configuración de CloudAMQP
CLOUDAMQP_URL = os.environ.get('CLOUDAMQP_URL', 'amqps://tnluigbk:x9gWN83qzJ3CIZjiKKAyg327wKNb9eA1@porpoise.rmq.cloudamqp.com/tnluigbk')
url = urlparse(CLOUDAMQP_URL)

# Extraer componentes de la URL
RABBIT_HOST = url.hostname
RABBIT_USER = url.username
RABBIT_PASSWORD = url.password
RABBIT_VHOST = url.path[1:] if url.path else '%2f'
RABBIT_PORT = 5671  # Puerto para TLS
RABBIT_SSL = True   # Habilitar SSL para conexión segura
RPC_QUEUE = 'rpc_queue'

# Estado global del servidor
SERVER_STATUS = {
    "running": False,
    "processed_messages": 0,
    "errors": 0
}

# Cliente RPC
class RpcClient(object):
    """Asynchronous Rpc client."""
    def __init__(self, host, username, password, rpc_queue, vhost, port=5671, ssl=True):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.vhost = vhost
        self.port = port
        self.ssl = ssl
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        """Open Connection."""
        try:
            # Crear conexión con soporte SSL/TLS
            self.connection = amqpstorm.Connection(
                self.host, 
                self.username,
                self.password,
                virtual_host=self.vhost,
                port=self.port,
                ssl=self.ssl
            )
            self.channel = self.connection.channel()
            self.channel.queue.declare(self.rpc_queue)
            result = self.channel.queue.declare(exclusive=True)
            self.callback_queue = result['queue']
            self.channel.basic.consume(self._on_response, no_ack=True,
                                    queue=self.callback_queue)
            self._create_process_thread()
            return True
        except Exception as e:
            print(f"Error al conectar con RabbitMQ: {str(e)}")
            return False

    def _create_process_thread(self):
        """Create a thread responsible for consuming messages in response
         to RPC requests.
        """
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        """Process Data Events using the Process Thread."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """On Response store the message with the correlation id in a local
         dictionary.
        """
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        try:
            # Create the Message object.
            message = Message.create(self.channel, payload)
            message.reply_to = self.callback_queue
            # Create an entry in our local dictionary, using the automatically
            # generated correlation_id as our key.
            self.queue[message.correlation_id] = None
            # Publish the RPC request.
            message.publish(routing_key=self.rpc_queue)
            # Return the Unique ID used to identify the request.
            return message.correlation_id
        except Exception as e:
            print(f"Error al enviar solicitud: {str(e)}")
            return None

    def is_connected(self):
        """Verificar si el cliente está conectado a RabbitMQ."""
        return self.connection and self.connection.is_open

# Servidor RPC
class TextProcessingServer(object):
    """Servidor RPC para operaciones de procesamiento de texto."""
    
    def __init__(self, host, username, password, rpc_queue, vhost, port=5671, ssl=True):
        self.host = host
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.vhost = vhost
        self.port = port
        self.ssl = ssl
        self.connection = None
        self.channel = None
        
    def start(self):
        """Iniciar el servidor RPC."""
        try:
            # Crear conexión con soporte SSL/TLS
            self.connection = amqpstorm.Connection(
                self.host, 
                self.username,
                self.password,
                virtual_host=self.vhost,
                port=self.port,
                ssl=self.ssl
            )
            # Crear canal
            self.channel = self.connection.channel()
            # Declarar la cola de solicitudes
            self.channel.queue.declare(self.rpc_queue)
            # Establecer QoS
            self.channel.basic.qos(prefetch_count=1)
            # Comenzar a consumir mensajes
            self.channel.basic.consume(self._process_request, self.rpc_queue)
            
            print(f"[x] Servidor de Procesamiento de Texto iniciado. Esperando mensajes en la cola '{self.rpc_queue}'...")
            SERVER_STATUS["running"] = True
            
            # Iniciar el consumo de mensajes
            self.channel.start_consuming()
            
        except Exception as e:
            SERVER_STATUS["running"] = False
            SERVER_STATUS["errors"] += 1
            print(f"Error en el servidor RPC: {str(e)}")
            
    def _process_request(self, message):
        """Procesar solicitudes RPC entrantes."""
        # Extraer el cuerpo del mensaje (payload)
        payload = message.body
        print(f"[.] Solicitud recibida: {payload}")
        
        # Procesar el texto según el comando
        response = self._process_text(payload)
        
        # Crear un mensaje de respuesta
        response_message = Message.create(
            message.channel,
            response
        )
        
        # Establecer el correlation_id y responder a la cola de callback apropiada
        response_message.correlation_id = message.correlation_id
        response_message.properties['delivery_mode'] = 2  # Hacer el mensaje persistente
        
        # Publicar el mensaje de respuesta
        response_message.publish(routing_key=message.reply_to)
        print(f"[x] Respuesta enviada: {response}")
        
        # Confirmar el mensaje
        message.ack()
        
        # Actualizar estadísticas
        SERVER_STATUS["processed_messages"] += 1
        
    def _process_text(self, payload):
        """Procesar el texto según el comando proporcionado."""
        # Formato esperado: "comando:texto"
        try:
            # Dividir el payload en comando y texto
            parts = payload.split(':', 1)
            
            if len(parts) < 2:
                return "ERROR: Formato inválido. Se espera 'comando:texto'"
            
            comando = parts[0].lower()
            texto = parts[1]
            
            # Procesar según el comando
            if comando == "mayusculas":
                return texto.upper()
            elif comando == "minusculas":
                return texto.lower()
            elif comando == "invertir":
                return texto[::-1]
            elif comando == "longitud":
                return str(len(texto))
            elif comando == "capitalizar":
                return texto.capitalize()
            elif comando == "titulo":
                return texto.title()
            elif comando == "intercambiar_caso":
                return texto.swapcase()
            elif comando == "contar_palabras":
                return str(len(texto.split()))
            elif comando == "recortar":
                return texto.strip()
            elif comando == "ayuda":
                return "Comandos disponibles: mayusculas, minusculas, invertir, longitud, capitalizar, titulo, intercambiar_caso, contar_palabras, recortar"
            else:
                return f"ERROR: Comando desconocido '{comando}'"
        except Exception as e:
            return f"ERROR: {str(e)}"

# Iniciar el servidor RPC en un hilo separado
def start_rpc_server():
    server = TextProcessingServer(
        RABBIT_HOST, 
        RABBIT_USER, 
        RABBIT_PASSWORD, 
        RPC_QUEUE,
        RABBIT_VHOST,
        RABBIT_PORT,
        RABBIT_SSL
    )
    try:
        server.start()
    except Exception as e:
        print(f"Error en el servidor RPC: {str(e)}")
        SERVER_STATUS["running"] = False

# Crear e iniciar el hilo del servidor
rpc_server_thread = threading.Thread(target=start_rpc_server)
rpc_server_thread.daemon = True
rpc_server_thread.start()

# Crear cliente RPC
RPC_CLIENT = RpcClient(
    RABBIT_HOST, 
    RABBIT_USER, 
    RABBIT_PASSWORD, 
    RPC_QUEUE, 
    RABBIT_VHOST,
    RABBIT_PORT,
    RABBIT_SSL
)

# Lista de operaciones disponibles
TEXT_OPERATIONS = [
    {"id": "mayusculas", "name": "Convertir a MAYÚSCULAS", "icon": "arrow-up-square", "description": "Convierte todo el texto a mayúsculas"},
    {"id": "minusculas", "name": "Convertir a minúsculas", "icon": "arrow-down-square", "description": "Convierte todo el texto a minúsculas"},
    {"id": "invertir", "name": "Invertir texto", "icon": "arrow-left-right", "description": "Invierte el orden de los caracteres del texto"},
    {"id": "longitud", "name": "Longitud del texto", "icon": "rulers", "description": "Cuenta el número de caracteres en el texto"},
    {"id": "capitalizar", "name": "Capitalizar", "icon": "type-bold", "description": "Convierte a mayúscula la primera letra del texto"},
    {"id": "titulo", "name": "Formato título", "icon": "card-heading", "description": "Convierte a mayúscula la primera letra de cada palabra"},
    {"id": "intercambiar_caso", "name": "Intercambiar caso", "icon": "arrow-down-up", "description": "Invierte mayúsculas/minúsculas"},
    {"id": "contar_palabras", "name": "Contar palabras", "icon": "list-ol", "description": "Cuenta el número de palabras en el texto"},
    {"id": "recortar", "name": "Recortar espacios", "icon": "scissors", "description": "Elimina espacios al inicio y final del texto"}
]

# Función para guardar historial
def save_history(operation, input_text, result):
    if 'history' not in session:
        session['history'] = []
    
    history_item = {
        'timestamp': datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        'operation': operation,
        'input_text': input_text,
        'result': result
    }
    
    session['history'] = [history_item] + session['history'][:19]  # Guardar los últimos 20
    session.modified = True

@app.route('/')
def index():
    """Página principal."""
    connected = RPC_CLIENT.is_connected()
    server_running = SERVER_STATUS["running"]
    return render_template('index.html', 
                          operations=TEXT_OPERATIONS, 
                          connected=connected and server_running,
                          history=session.get('history', []))

@app.route('/about')
def about():
    """Página acerca de."""
    return render_template('about.html')

@app.route('/process', methods=['POST'])
def process_text():
    """Procesar texto vía RPC."""
    operation = request.form.get('operation')
    text = request.form.get('text')
    
    if not operation or not text:
        flash('Por favor, completa todos los campos', 'danger')
        return redirect(url_for('index'))
    
    # Verificar conexión
    if not RPC_CLIENT.is_connected():
        # Intentar reconectar
        if not RPC_CLIENT.open():
            flash('Error: No se pudo conectar con el servidor RPC', 'danger')
            return redirect(url_for('index'))
    
    # Preparar payload
    payload = f"{operation}:{text}"
    
    # Enviar solicitud RPC
    corr_id = RPC_CLIENT.send_request(payload)
    
    if not corr_id:
        flash('Error al enviar la solicitud', 'danger')
        return redirect(url_for('index'))
    
    # Esperar respuesta (con timeout)
    max_wait = 100  # 10 segundos máximo
    counter = 0
    
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)
        counter += 1
        if counter >= max_wait:
            flash('Tiempo de espera agotado. No se recibió respuesta del servidor', 'warning')
            return redirect(url_for('index'))
    
    # Obtener resultado
    result = RPC_CLIENT.queue[corr_id]
    
    # Encontrar el nombre descriptivo de la operación
    operation_name = operation
    for op in TEXT_OPERATIONS:
        if op['id'] == operation:
            operation_name = op['name']
            break
    
    # Guardar en historial
    save_history(operation_name, text, result)
    
    # Si es AJAX, devolver JSON
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({
            'success': True,
            'operation': operation_name,
            'result': result
        })
    
    # Si no es AJAX, redireccionar con mensaje
    flash(f'Operación completada: {operation_name}', 'success')
    return redirect(url_for('index'))

@app.route('/clear-history', methods=['POST'])
def clear_history():
    """Borrar historial de operaciones."""
    session.pop('history', None)
    flash('Historial eliminado', 'info')
    return redirect(url_for('index'))

@app.route('/health')
def health_check():
    """Verificar estado de la aplicación y conexión RPC."""
    return jsonify({
        'app': 'ok',
        'rpc_connected': RPC_CLIENT.is_connected(),
        'server_running': SERVER_STATUS["running"],
        'processed_messages': SERVER_STATUS["processed_messages"]
    })

@app.route('/status')
def status():
    """Página de estado detallado."""
    return jsonify({
        'client': {
            'connected': RPC_CLIENT.is_connected(),
            'host': RABBIT_HOST,
            'queue': RPC_QUEUE
        },
        'server': SERVER_STATUS,
        'rabbit': {
            'host': RABBIT_HOST,
            'vhost': RABBIT_VHOST,
            'port': RABBIT_PORT,
            'ssl': RABBIT_SSL
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
