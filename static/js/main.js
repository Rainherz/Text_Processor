/**
 * TextPro RPC - Cliente de Procesamiento de Texto
 * JavaScript principal para la aplicación web
 */

// Manejar el cambio de tema (Oscuro/Claro)
document.addEventListener('DOMContentLoaded', () => {
    // Comprobar preferencia guardada
    const savedTheme = localStorage.getItem('theme');
    if (savedTheme) {
        document.documentElement.setAttribute('data-bs-theme', savedTheme);
        updateThemeIcon(savedTheme);
    } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        // Usar preferencia del sistema
        document.documentElement.setAttribute('data-bs-theme', 'dark');
        updateThemeIcon('dark');
    }
    
    // Escuchar cambios en el botón de tema
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }
    
    // Verificar estado del servidor cada 30 segundos
    setInterval(checkServerStatus, 30000);
    
    // Añadir efectos de hover y focus a tarjetas
    addCardInteractivity();
    
    // Mostrar u ocultar elementos basados en el estado de conexión
    updateUIBasedOnConnection();
});

/**
 * Alternar entre temas claro y oscuro
 */
function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-bs-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    
    // Animar la transición
    document.body.style.opacity = '0.9';
    
    setTimeout(() => {
        document.documentElement.setAttribute('data-bs-theme', newTheme);
        localStorage.setItem('theme', newTheme);
        updateThemeIcon(newTheme);
        document.body.style.opacity = '1';
    }, 200);
}

/**
 * Actualizar el icono del botón de tema
 * @param {string} theme - 'dark' o 'light'
 */
function updateThemeIcon(theme) {
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.innerHTML = theme === 'dark' 
            ? '<i class="bi bi-sun"></i>' 
            : '<i class="bi bi-moon-stars"></i>';
    }
}

/**
 * Verificar el estado del servidor RPC
 */
function checkServerStatus() {
    fetch('/health')
        .then(response => response.json())
        .then(data => {
            updateConnectionStatus(data.rpc_connected);
        })
        .catch(error => {
            console.error('Error verificando estado:', error);
            updateConnectionStatus(false);
        });
}

/**
 * Actualizar indicadores de estado de conexión
 * @param {boolean} connected - Estado de conexión 
 */
function updateConnectionStatus(connected) {
    const indicator = document.querySelector('.status-indicator');
    const statusText = document.querySelector('.status-indicator + div p');
    const processBtn = document.getElementById('process-btn');
    
    if (indicator && statusText) {
        if (connected) {
            indicator.className = 'status-indicator me-3 connected';
            indicator.innerHTML = '<i class="bi bi-cloud-check"></i>';
            statusText.textContent = 'Conectado y funcionando';
            if (processBtn) processBtn.disabled = false;
        } else {
            indicator.className = 'status-indicator me-3 disconnected';
            indicator.innerHTML = '<i class="bi bi-cloud-slash"></i>';
            statusText.textContent = 'Desconectado';
            if (processBtn) processBtn.disabled = true;
        }
    }
}

/**
 * Añadir interactividad a las tarjetas
 */
function addCardInteractivity() {
    const cards = document.querySelectorAll('.card');
    
    cards.forEach(card => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-5px)';
        });
        
        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0)';
        });
        
        card.addEventListener('focus', function() {
            this.style.transform = 'translateY(-5px)';
        });
        
        card.addEventListener('blur', function() {
            this.style.transform = 'translateY(0)';
        });
    });
}

/**
 * Actualizar UI basado en estado de conexión
 */
function updateUIBasedOnConnection() {
    const connected = document.querySelector('.status-indicator.connected') !== null;
    const processBtn = document.getElementById('process-btn');
    
    if (processBtn) {
        processBtn.disabled = !connected;
    }
    
    if (!connected) {
        const warningMsg = document.createElement('div');
        warningMsg.className = 'alert alert-warning';
        warningMsg.innerHTML = '<i class="bi bi-exclamation-triangle"></i> Servidor RPC no disponible. Por favor verifica la conexión.';
        
        const form = document.getElementById('text-form');
        if (form && !document.querySelector('#text-form .alert')) {
            form.prepend(warningMsg);
        }
    }
}

/**
 * Mostrar el resultado de una operación
 * @param {string} operation - Nombre de la operación
 * @param {string} result - Resultado de la operación
 */
function showResult(operation, result) {
    const resultOperation = document.getElementById('result-operation');
    const resultContent = document.getElementById('result-content');
    
    if (resultOperation && resultContent) {
        resultOperation.textContent = operation;
        resultContent.textContent = result;
        
        // Ocultar formulario y mostrar resultado
        document.querySelector('.main-card').style.display = 'none';
        
        const resultCard = document.querySelector('.result-card');
        resultCard.style.display = 'block';
        resultCard.style.animation = 'fadeIn 0.5s ease';
    }
}

/**
 * Copiar texto al portapapeles
 * @param {string} text - Texto a copiar
 * @param {HTMLElement} button - Botón que se presionó
 */
function copyToClipboard(text, button) {
    navigator.clipboard.writeText(text)
        .then(() => {
            const originalHTML = button.innerHTML;
            button.innerHTML = '<i class="bi bi-check"></i> Copiado';
            
            setTimeout(() => {
                button.innerHTML = originalHTML;
            }, 2000);
        })
        .catch(err => {
            console.error('Error al copiar: ', err);
            alert('No se pudo copiar el texto');
        });
}

/**
 * Animar elementos cuando aparecen en el viewport
 */
function animateOnScroll() {
    const elementsToAnimate = document.querySelectorAll('.animate-on-scroll');
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                observer.unobserve(entry.target);
            }
        });
    }, { threshold: 0.2 });
    
    elementsToAnimate.forEach(element => {
        observer.observe(element);
    });
}

// Inicializar animaciones si hay elementos para animar
if (document.querySelector('.animate-on-scroll')) {
    animateOnScroll();
    window.addEventListener('scroll', animateOnScroll);
}