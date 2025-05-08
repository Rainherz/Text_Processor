import os
from urllib.parse import urlparse
import amqpstorm
from amqpstorm import Message

# Obtener URL de CloudAMQP desde variable de entorno
amqp_url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')

# Parsear URL para extraer componentes
url = urlparse(amqp_url)

# Configuración de RabbitMQ
host = url.hostname
username = url.username
password = url.password
vhost = url.path[1:] if url.path else '%2f'
rpc_queue = 'rpc_queue'


class TextProcessingServer(object):
    """Servidor RPC para operaciones de procesamiento de texto."""
    
    def __init__(self, host, username, password, rpc_queue, vhost):
        self.host = host
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.vhost = vhost
        self.connection = None
        self.channel = None
        
    def start(self):
        """Iniciar el servidor RPC."""
        # Crear conexión
        self.connection = amqpstorm.Connection(
            self.host, 
            self.username, 
            self.password,
            virtual_host=self.vhost
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
        self.channel.start_consuming()
        
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


if __name__ == "__main__":
    # Crear e iniciar el servidor
    server = TextProcessingServer(host, username, password, rpc_queue, vhost)
    try:
        server.start()
    except KeyboardInterrupt:
            print("Servidor detenido por el usuario")
