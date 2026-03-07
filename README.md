# 🚀 Setup — Git & GitHub Tutorial

Guía para poner en marcha el tutorial desde cero en Windows.

---

## Requisitos previos

Asegúrate de tener instalado:

- [Python 3.13+](https://www.python.org/downloads/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — gestor de entornos y dependencias
- [Git](https://git-scm.com/downloads)
- [GitHub CLI (`gh`)](https://cli.github.com/)

---

## 1 · Autenticarse en GitHub

```bash
gh auth login
```

Selecciona **GitHub.com → HTTPS → Login with a web browser** y sigue las instrucciones.

Verifica que todo está correcto:

```bash
gh auth status
```

---

## 2 · Crear la carpeta de trabajo & clona el repo

```bash
mkdir git_tutorial
cd git_tutorial
```

```bash
gh repo clone OEJ32/hands-on_git_tutorial

```

Quitamos el repositorio original para después apuntar a nuestro repo 
```bash
git remote remove origin
```

---

## 3 · Copiar el notebook

Coloca el fichero `git_tutorial.py` dentro de `git_tutorial/`.

---

## 4 · Crear el fichero `.env`

En la misma carpeta que el notebook, crea un fichero llamado `.env`:

```
GH_USERNAME=tu_usuario_de_github
```

> ⚠️ Sustituye `tu_usuario_de_github` por tu usuario real de GitHub.

---

## 5 · Crear el entorno virtual e instalar dependencias

```bash
-- NO ES NECESARIO, ESTÁ YA EL pyproject.toml (si no, sería necesario) -- uv init
uv sync 
uv venv .venv
.venv\Scripts\activate
```

Te saldrá un popu abajo a la derecha si usas VSC, dale a Sí. Esto asegura que el intérprete del .venv

---

## 6 · Lanzar el notebook

```bash
marimo run git_tutorial.py
```

O en modo edición si quieres ver el código:

```bash
marimo edit git_tutorial.py
```

O instala la extensión, y después abre el notebook con el icono verde al lado del botón de "Play" arriba a la derecha en VSC
<img width="1920" height="1008" alt="image" src="https://github.com/user-attachments/assets/57f5c3ac-d99e-45a7-b441-b8ee169dc504" />


---

## 7 · Ejecutar el tutorial

Ejecuta las celdas **en orden de arriba a abajo**. Cada celda muestra el comando git que se está ejecutando y su salida real.

Cuando llegues a la **sección 11 (Pull Request)**, el notebook abrirá una PR en GitHub automáticamente y te pedirá que:

1. Abras la URL que aparece en pantalla
2. Pulses **"Merge pull request"** en GitHub
3. Vuelvas al notebook y re-ejecutes la celda de verificación

---

## Estructura de ficheros esperada

```
git_tutorial/
├── .env                  ← tu GH_USERNAME
├── .venv/                ← entorno virtual
├── git_tutorial.py       ← el notebook
└── sandbox/              ← se crea automáticamente al ejecutar
```

> `sandbox/` se borra y recrea cada vez que relanzas el notebook, garantizando un historial limpio en cada ejecución.

---

## Solución de problemas

**`gh: command not found`**
Instala la GitHub CLI desde [cli.github.com](https://cli.github.com/) y reinicia la terminal.

**`GH_USERNAME no encontrado`**
Comprueba que el fichero `.env` está en la misma carpeta que `git_tutorial.py` y que no tiene espacios alrededor del `=`.

**El log de git muestra commits de otros repos**
Asegúrate de que estás usando la versión actualizada del notebook. El `REPO` debe apuntar a `sandbox/`, nunca a la carpeta raíz.

**Error al hacer push (`remote: Repository not found`)**
Verifica que `gh auth status` muestra tu usuario correcto y que el token tiene permisos de escritura (`repo`).
