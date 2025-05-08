
import threading
import time
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
HEARTBEAT_INTERVAL = 30  # Reducir el intervalo de heartbeat a 30 segundos

# Estado global
APP_STATUS = {
    "client_connected": False,
    "server_running": False,
    "processed_messages": 0,
    "errors": 0,
    "last_error": None,
    "last_reconnect": None
}

# Clase de Cliente RPC mejorada
class RpcClient(object):
    def __init__(self, host, username, password, rpc_queue, vhost, port=5671, ssl=True, heartbeat=30):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.vhost = vhost
        self.port = port
        self.ssl = ssl
        self.heartbeat = heartbeat
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.consumer_thread = None
        self.heartbeat_thread = None
        self.should_reconnect = True
        self.open()

    def open(self):
        """Abrir conexión con manejo de errores y reconexión"""
        if self.connection and self.connection.is_open:
            return True
        
        try:
            print(f"[Cliente] Conectando a RabbitMQ: {self.host}:{self.port}")
            # Configurar conexión con tiempo de heartbeat reducido
            self.connection = amqpstorm.Connection(
                self.host, 
                self.username,
                self.password,
                virtual_host=self.vhost,
                port=self.port,
                ssl=self.ssl,
                heartbeat=self.heartbeat
            )
            
            self.channel = self.connection.channel()
            # Asegurar que la cola RPC exista
            self.channel.queue.declare(self.rpc_queue)
            
            # Crear cola de respuestas exclusiva
            result = self.channel.queue.declare(exclusive=True)
            self.callback_queue = result['queue']
            
            # Configurar consumidor
            self.channel.basic.consume(self._on_response, no_ack=True,
                                      queue=self.callback_queue)
            
            # Iniciar hilo de consumo
            self._create_process_thread()
            
            # Iniciar hilo de heartbeat
            self._create_heartbeat_thread()
            
            print(f"[Cliente] Conectado exitosamente a RabbitMQ")
            APP_STATUS["client_connected"] = True
            APP_STATUS["last_reconnect"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return True
        except Exception as e:
            APP_STATUS["client_connected"] = False
            APP_STATUS["errors"] += 1
            APP_STATUS["last_error"] = str(e)
            print(f"[Cliente] Error al conectar con RabbitMQ: {str(e)}")
            return False

    def _create_process_thread(self):
        """Crear hilo para procesar mensajes"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            return
            
        self.consumer_thread = threading.Thread(target=self._process_data_events)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

    def _create_heartbeat_thread(self):
        """Crear hilo para enviar heartbeats"""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            return
            
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()  # CORREGIDO: era self.heartbeat.start()

    def _process_data_events(self):
        """Procesar eventos con manejo de errores y reconexión"""
        while self.should_reconnect:
            try:
                if self.connection and self.connection.is_open:
                    self.channel.start_consuming(to_tuple=False)
            except Exception as e:
                APP_STATUS["errors"] += 1
                APP_STATUS["last_error"] = str(e)
                print(f"[Cliente] Error en hilo de consumo: {str(e)}")
                APP_STATUS["client_connected"] = False
                
                # Intentar reconectar
                time.sleep(5)  # Esperar antes de reconectar
                self._reconnect()

    def _heartbeat_loop(self):
        """Mantener la conexión viva enviando un comando liviano periódicamente"""
        while self.should_reconnect:
            try:
                if self.connection and self.connection.is_open:
                    # En lugar de send_heartbeat(), usamos un comando liviano
                    # para mantener la conexión activa
                    self.channel.basic.publish(
                        body='',
                        exchange='',
                        routing_key='',
                        properties={
                            'delivery_mode': 1  # No persistente
                        }
                    )
            except Exception as e:
                APP_STATUS["errors"] += 1
                APP_STATUS["last_error"] = f"Heartbeat error: {str(e)}"
                print(f"[Cliente] Error en heartbeat: {str(e)}")
                
                # Intentar reconectar si el error es de conexión
                if self.connection and not self.connection.is_open:
                    self._reconnect()
                
            # Dormir por menos tiempo que el intervalo de heartbeat
            time.sleep(self.heartbeat // 2)

    def _reconnect(self):
        """Intentar reconectar al servidor RabbitMQ"""
        if not self.should_reconnect:
            return
            
        print("[Cliente] Intentando reconectar...")
        
        # Cerrar conexiones existentes
        try:
            if self.connection:
                self.connection.close()
        except:
            pass
            
        # Intentar reconectar
        self.open()

    def _on_response(self, message):
        """Manejar respuestas"""
        try:
            self.queue[message.correlation_id] = message.body
        except Exception as e:
            APP_STATUS["errors"] += 1
            APP_STATUS["last_error"] = str(e)
            print(f"[Cliente] Error en manejador de respuestas: {str(e)}")

    def send_request(self, payload):
        """Enviar solicitud con manejo de errores"""
        try:
            # Verificar y reconectar si es necesario
            if not self.connection or not self.connection.is_open:
                if not self.open():
                    return None
                    
            # Crear mensaje
            message = Message.create(self.channel, payload)
            message.reply_to = self.callback_queue
            
            # Crear entrada en diccionario
            self.queue[message.correlation_id] = None
            
            # Publicar solicitud
            message.publish(routing_key=self.rpc_queue)
            
            return message.correlation_id
        except Exception as e:
            APP_STATUS["errors"] += 1
            APP_STATUS["last_error"] = str(e)
            print(f"[Cliente] Error al enviar solicitud: {str(e)}")
            
            # Intentar reconectar
            self._reconnect()
            return None

    def is_connected(self):
        """Verificar conexión"""
        return self.connection and self.connection.is_open

    def close(self):
        """Cerrar conexión"""
        self.should_reconnect = False
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
        APP_STATUS["client_connected"] = False


# Clase de Servidor RPC mejorada
class TextProcessingServer(object):
    def __init__(self, host, username, password, rpc_queue, vhost, port=5671, ssl=True, heartbeat=30):
        self.host = host
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.vhost = vhost
        self.port = port
        self.ssl = ssl
        self.heartbeat = heartbeat
        self.connection = None
        self.channel = None
        self.should_reconnect = True
        self.heartbeat_thread = None
        
    def start(self):
        """Iniciar servidor con manejo de errores"""
        while self.should_reconnect:
            try:
                print(f"[Servidor] Conectando a RabbitMQ: {self.host}:{self.port}")
                # Crear conexión con heartbeat reducido
                self.connection = amqpstorm.Connection(
                    self.host, 
                    self.username,
                    self.password,
                    virtual_host=self.vhost,
                    port=self.port,
                    ssl=self.ssl,
                    heartbeat=self.heartbeat
                )
                
                # Crear canal
                self.channel = self.connection.channel()
                
                # Declarar cola
                self.channel.queue.declare(self.rpc_queue)
                
                # Configurar QoS
                self.channel.basic.qos(prefetch_count=1)
                
                # Iniciar hilo de heartbeat
                self._create_heartbeat_thread()
                
                # Configurar consumidor
                self.channel.basic.consume(self._process_request, self.rpc_queue)
                
                print(f"[Servidor] Iniciado. Esperando mensajes en '{self.rpc_queue}'")
                APP_STATUS["server_running"] = True
                
                # Iniciar consumo
                self.channel.start_consuming()
                
            except KeyboardInterrupt:
                print("[Servidor] Detenido por el usuario")
                self.should_reconnect = False
                break
                
            except Exception as e:
                APP_STATUS["server_running"] = False
                APP_STATUS["errors"] += 1
                APP_STATUS["last_error"] = str(e)
                print(f"[Servidor] Error: {str(e)}")
                
                # Limpiar conexiones
                try:
                    if self.connection:
                        self.connection.close()
                except:
                    pass
                    
                # Esperar antes de reconectar
                print("[Servidor] Esperando para reconectar...")
                time.sleep(5)
                
    def _create_heartbeat_thread(self):
        """Crear hilo para enviar heartbeats"""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            return
            
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
                
    def _heartbeat_loop(self):
        """Mantener la conexión viva enviando un comando liviano periódicamente"""
        while self.should_reconnect:
            try:
                if self.connection and self.connection.is_open:
                    # En lugar de send_heartbeat(), usamos un comando liviano
                    # para mantener la conexión activa
                    self.channel.basic.publish(
                        body='',
                        exchange='',
                        routing_key='',
                        properties={
                            'delivery_mode': 1  # No persistente
                        }
                    )
            except Exception as e:
                APP_STATUS["errors"] += 1
                APP_STATUS["last_error"] = f"Servidor heartbeat error: {str(e)}"
                print(f"[Servidor] Error en heartbeat: {str(e)}")
                
            # Dormir por menos tiempo que el intervalo de heartbeat
            time.sleep(self.heartbeat // 2)
        
    def _process_request(self, message):
        """Procesar solicitudes con manejo de errores"""
        try:
            # Extraer payload
            payload = message.body
            print(f"[Servidor] Solicitud recibida: {payload}")
            
            # Procesar texto
            response = self._process_text(payload)
            
            # Crear respuesta
            response_message = Message.create(
                message.channel,
                response
            )
            
            # Configurar propiedades
            response_message.correlation_id = message.correlation_id
            response_message.properties['delivery_mode'] = 2
            
            # Publicar respuesta
            response_message.publish(routing_key=message.reply_to)
            print(f"[Servidor] Respuesta enviada: {response}")
            
            # Confirmar mensaje
            message.ack()
            
            # Actualizar estadísticas
            APP_STATUS["processed_messages"] += 1
            
        except Exception as e:
            APP_STATUS["errors"] += 1
            APP_STATUS["last_error"] = str(e)
            print(f"[Servidor] Error procesando solicitud: {str(e)}")
            
            # Intentar confirmar el mensaje
            try:
                message.ack()
            except:
                pass
        
    def _process_text(self, payload):
        """Procesar texto según comando"""
        try:
            # Dividir payload
            parts = payload.split(':', 1)
            
            if len(parts) < 2:
                return "ERROR: Formato inválido. Se espera 'comando:texto'"
            
            comando = parts[0].lower()
            texto = parts[1]
            
            # Procesar según comando
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
    
    session['history'] = [history_item] + session['history'][:19]
    session.modified = True

# Iniciar servidor en hilo separado
def start_rpc_server():
    server = TextProcessingServer(
        RABBIT_HOST, 
        RABBIT_USER, 
        RABBIT_PASSWORD, 
        RPC_QUEUE,
        RABBIT_VHOST,
        RABBIT_PORT,
        RABBIT_SSL,
        HEARTBEAT_INTERVAL
    )
    try:
        server.start()
    except Exception as e:
        APP_STATUS["server_running"] = False
        APP_STATUS["errors"] += 1
        APP_STATUS["last_error"] = str(e)
        print(f"[Servidor] Error fatal: {str(e)}")

# Crear e iniciar hilo de servidor
print("[App] Iniciando servidor RPC en hilo separado...")
rpc_server_thread = threading.Thread(target=start_rpc_server)
rpc_server_thread.daemon = True
rpc_server_thread.start()

# Crear cliente RPC
print("[App] Creando cliente RPC...")
RPC_CLIENT = RpcClient(
    RABBIT_HOST, 
    RABBIT_USER, 
    RABBIT_PASSWORD, 
    RPC_QUEUE, 
    RABBIT_VHOST,
    RABBIT_PORT,
    RABBIT_SSL,
    HEARTBEAT_INTERVAL
)

# Rutas de la aplicación
@app.route('/')
def index():
    """Página principal"""
    connected = APP_STATUS["client_connected"] and APP_STATUS["server_running"]
    return render_template('index.html', 
                          operations=TEXT_OPERATIONS, 
                          connected=connected,
                          history=session.get('history', []))

@app.route('/about')
def about():
    """Página acerca de"""
    return render_template('about.html')

@app.route('/process', methods=['POST'])
def process_text():
    """Procesar texto vía RPC"""
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
    
    # Enviar solicitud
    corr_id = RPC_CLIENT.send_request(payload)
    
    if not corr_id:
        flash('Error al enviar la solicitud', 'danger')
        return redirect(url_for('index'))
    
    # Esperar respuesta con timeout
    max_wait = 100  # 10 segundos
    counter = 0
    
    while counter < max_wait:
        if corr_id in RPC_CLIENT.queue and RPC_CLIENT.queue[corr_id] is not None:
            break
        sleep(0.1)
        counter += 1
    
    # Verificar timeout
    if counter >= max_wait or RPC_CLIENT.queue.get(corr_id) is None:
        flash('Tiempo de espera agotado. No se recibió respuesta del servidor', 'warning')
        return redirect(url_for('index'))
    
    # Obtener resultado
    result = RPC_CLIENT.queue[corr_id]
    
    # Obtener nombre de operación
    operation_name = operation
    for op in TEXT_OPERATIONS:
        if op['id'] == operation:
            operation_name = op['name']
            break
    
    # Guardar historial
    save_history(operation_name, text, result)
    
    # Responder según tipo de solicitud
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return jsonify({
            'success': True,
            'operation': operation_name,
            'result': result
        })
    
    flash(f'Operación completada: {operation_name}', 'success')
    return redirect(url_for('index'))

@app.route('/clear-history', methods=['POST'])
def clear_history():
    """Borrar historial"""
    session.pop('history', None)
    flash('Historial eliminado', 'info')
    return redirect(url_for('index'))

@app.route('/health')
def health_check():
    """Verificar salud"""
    return jsonify({
        'app': 'ok',
        'client_connected': APP_STATUS["client_connected"],
        'server_running': APP_STATUS["server_running"],
        'processed_messages': APP_STATUS["processed_messages"]
    })

@app.route('/status')
def status():
    """Estado detallado"""
    return jsonify({
        'status': APP_STATUS,
        'rabbit': {
            'host': RABBIT_HOST,
            'vhost': RABBIT_VHOST,
            'port': RABBIT_PORT,
            'ssl': RABBIT_SSL,
            'heartbeat': HEARTBEAT_INTERVAL
        },
        'queue': RPC_QUEUE
    })

@app.route('/ping')
def ping():
    """Endpoint simple para mantener la aplicación activa"""
    return "pong"

# Iniciar aplicación
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"[App] Iniciando aplicación web en puerto {port}")
    app.run(host='0.0.0.0', port=port)
