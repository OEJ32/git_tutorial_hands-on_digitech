# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.20.2",
#     "python-dotenv>=1.2.2",
#     "pyzmq>=27.1.0",
# ]
# ///

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium", app_title="Git & GitHub — Tutorial CLI")


@app.cell
def _():
    import os
    # In ipynb, simply <<!uv init>>. No magic commands here
    # os.system("uv init")
    # os.system("uv venv .venv")
    # os.system("source .venv\Scripts\activate")  # En Linux/Mac: source .venv/bin/activate
    return (os,)


@app.cell
def _imports(os):
    import marimo as mo
    import subprocess, tempfile
    from pathlib import Path

    REPO: Path = Path()

    def git(cmd, cwd=None):
        env = os.environ.copy()
        env["GIT_AUTHOR_NAME"]     = "Alumno"
        env["GIT_AUTHOR_EMAIL"]    = "alumno@curso.es"
        env["GIT_COMMITTER_NAME"]  = "Alumno"
        env["GIT_COMMITTER_EMAIL"] = "alumno@curso.es"
        env["GIT_TERMINAL_PROMPT"] = "0"
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            cwd=str(cwd or REPO), env=env
        )
        out = (r.stdout + r.stderr).strip()
        return out

    def show(cmd, cwd=None):
        """Run a command and return a markdown code block with the output."""
        out = git(cmd, cwd=cwd)
        return mo.md(f"```\n$ {cmd}\n{out}\n```")

    return REPO, git, mo, show


@app.cell
def _(show):
    show(cmd="git init")
    return


@app.cell
def _(show):
    show(cmd='uv add python-dotenv')
    return


@app.cell
def _(os, show):
    from dotenv import load_dotenv
    load_dotenv()
    GH_USERNAME = os.getenv("GH_USERNAME")
    REPO_NAME ='teaching_git'
    show(cmd='git remote -v')
    return GH_USERNAME, REPO_NAME


@app.cell
def _(GH_USERNAME, REPO_NAME, git, show):
    def create_remote_repo(GH_USERNAME, show):
        # Verificamos si 'origin' ya existe en la configuración de remotos
        check_remote = git("git remote")
    
        if "origin" not in check_remote:
            # Si no existe, lo añadimos por primera vez
            show(cmd=f"git remote add origin https://github.com/{GH_USERNAME}/{REPO_NAME}.git")
            print("Configurado 'origin' por primera vez.")
        else:
            # Si ya existe, simplemente actualizamos la URL (por si cambió)
            show(cmd=f"git remote set-url origin https://github.com/{GH_USERNAME}/{REPO_NAME}.git")
            print("'origin' ya existía, URL actualizada correctamente.")
    
        return

    create_remote_repo(GH_USERNAME, show)
    return


@app.cell
def _(show):
    # Finalmente, confirmamos el estado final
    show(cmd="git remote -v")
    return


@app.cell
def _(git):
    def create_remote_repo(GH_USERNAME, show):
        # Verificamos si existe el remote antes de intentar crearlo
        check_remote = git("git remote")
        if "origin" not in check_remote:
            # Solo creamos si no existe
            show(cmd=f"gh repo create git_tutorial_1 --private --source=. --push")
        else:
            print("El repositorio remoto ya está configurado.")
        return

    return


@app.cell
def _(GH_USERNAME, show):
    show(cmd=f'git remote set-url origin https://github.com/{GH_USERNAME}/git_test.git')
    return


@app.cell
def _(show):
    show(cmd='git remote -v')
    return


@app.cell
def _(GH_USERNAME, show):
    show(cmd=f"git remote add origin https://github.com/{GH_USERNAME}/git_test.git")
    return


@app.cell
def _(mo):
    mo.md("""
    # 🌿 Git & GitHub — Tutorial Interactivo

    Este notebook crea un repositorio Git **real** desde cero y ejecuta cada
    comando en tu máquina. Sigue las secciones en orden.

    ---
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1 · Crear el repositorio

    `git init` convierte una carpeta normal en un repositorio Git.
    Crea una carpeta oculta `.git/` que guarda todo el historial.
    """)
    return


@app.cell
def _(show):
    show("git init")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 2 · Primer commit

    El ciclo básico de Git tiene tres pasos:

    ```
    editar archivo  →  git add  →  git commit
    ```

    Primero creamos dos ficheros:
    """)
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "README.md").write_text("# Mi Proyecto\n\nProyecto de ejemplo del curso.\n")
    (REPO / "main.py").write_text('def main():\n    print("Hello from git-tutorial for Digitech!")\n\nif __name__ == "__main__":\n\t    main()')

    show("dir")
    return


@app.cell
def _(mo):
    mo.md("""
    Git aún no los rastrea. Comprobamos el estado:
    """)
    return


@app.cell
def _(show):
    show("git status")
    return


@app.cell
def _(mo):
    mo.md("""
    Los ficheros aparecen en rojo como **Untracked**. Los añadimos al
    *staging area* con `git add`:
    """)
    return


@app.cell
def _(show):
    show("git add README.md main.py")
    return


@app.cell
def _(show):
    show("git status")
    return


@app.cell
def _(mo):
    mo.md("""
    Ahora aparecen en verde: están **staged**, listos para el commit.
    Guardamos el snapshot:
    """)
    return


@app.cell
def _(show):
    show('git commit -m "feat: add README and initial main.py"')
    return


@app.cell
def _(show):
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 3 · Construir historial

    Añadimos más ficheros para tener un historial con el que practicar.
    """)
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "requirements.txt").write_text("fastapi>=0.100\nuvicorn\nhttpx\n")
    show("git add requirements.txt && git commit -m \"chore: add requirements.txt\"")
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / ".gitignore").write_text("__pycache__/\n*.pyc\n.venv/\n.env\n")
    show("git add .gitignore && git commit -m \"chore: add .gitignore\"'")
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "main.py").write_text(
        "from fastapi import FastAPI\n\napp = FastAPI()\n\n"
        "@app.get('/')\ndef root():\n    return {'message': 'Hola mundo!'}\n"
    )
    show("git add main.py && git commit -m \"feat: migrate app to FastAPI\"")
    return


@app.cell
def _(show):
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    > **Convención de mensajes:** usa prefijos como `feat:`, `fix:`,
    > `chore:`, `docs:` para que el historial sea legible de un vistazo.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 4 · Explorar el historial

    `git log` tiene muchas opciones útiles:
    """)
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(show):
    show("git show HEAD --stat")
    return


@app.cell
def _(mo):
    mo.md("""
    Para ver exactamente qué cambió en un fichero entre dos commits:
    """)
    return


@app.cell
def _(show):
    show("git diff HEAD~1 HEAD -- main.py")
    return


@app.cell
def _(mo):
    mo.md("""
    Las líneas en **`-`** son las que se eliminaron y las de **`+`** las añadidas.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 5 · Ramas — `git branch` · `git checkout`

    Una rama es un puntero a un commit. Trabajar en ramas mantiene `main`
    estable mientras desarrollas nuevas funcionalidades.
    """)
    return


@app.cell
def _(mo):
    mo.mermaid("""
    %%{init: { 'theme': 'base', 'themeVariables': { 'primaryColor': '#fff', 'edgeLabelBackground':'#ffffff', 'tertiaryColor': '#fff'}}}%%
    gitGraph
        commit id: "A"
        commit id: "B"
        commit id: "C"
        commit id: "D"
        branch feature
        checkout feature
        commit id: "E"
        commit id: "F"
    """)
    return


@app.cell
def _(show):
    show("git checkout -b feature/health-endpoint")
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "health.py").write_text(
        "from app import app\nfrom datetime import datetime\n\n"
        "@app.get('/health')\ndef health():\n    return {'status': 'ok', 'time': str(datetime.now())}\n"
    )
    show("git add health.py && git commit -m 'feat: add /health endpoint'")
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "health.py").write_text(
        "from app import app\nfrom datetime import datetime\n\n"
        "@app.get('/health')\ndef health():\n    return {'status': 'ok', 'version': '1.0.0', 'time': str(datetime.now())}\n"
    )
    show("git add health.py && git commit -m 'feat: add version to health response'")
    ## Por qué falla???
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(mo):
    mo.md("""
    Volvemos a `main` — los commits de la rama no están aquí todavía:
    """)
    return


@app.cell
def _(show):
    show("git checkout main && dir")
    return


@app.cell
def _(show):
    show("git branch -v")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 6 · Coger la rama de un compañero

    Simulamos que existe un **origin** remoto y que un compañero ha subido
    su rama. Nosotros la descargamos y continuamos su trabajo.
    """)
    return


@app.cell
def _(REPO: "Path", git, show):
    # Crear origin bare y clonar como "compañero B"
    origin = REPO.parent / "origin.git"
    alumno_b = REPO.parent / "alumno-b"
    git(f"git clone --bare {REPO} {origin}", cwd=REPO.parent)
    git(f"git clone {origin} {alumno_b}", cwd=REPO.parent)
    git("git config user.name 'Compañero B'", cwd=alumno_b)
    git("git config user.email 'b@curso.es'", cwd=alumno_b)

    # El compañero B crea su rama y la sube
    git("git checkout -b feature/login", cwd=alumno_b)
    (alumno_b / "login.py").write_text(
        "from app import app\n\n"
        "@app.post('/login')\ndef login(username: str, password: str):\n"
        "    if username == 'admin' and password == '1234':\n"
        "        return {'token': 'abc123'}\n"
        "    return {'error': 'invalid credentials'}\n"
    )
    git("git add login.py", cwd=alumno_b)
    git("git commit -m 'feat: add /login endpoint'", cwd=alumno_b)
    git("git push origin feature/login", cwd=alumno_b)

    # Nosotros conectamos nuestro repo al origin
    git(f"git remote add origin {origin}", cwd=REPO)
    show("git remote -v")
    return


@app.cell
def _(mo):

    mo.mermaid("""
    graph TD
        subgraph "Tu Computadora (Local)"
            LocalBranch[Rama feature_1]
            StagingArea[Staging Area / Index]
        end

        subgraph "Servidor Central (Remote: origin)"
            RemoteMain[origin/main]
            RemoteStaging[origin/staging]
            RemoteFeature[origin/feature_1]
        end

        LocalBranch -- "git add" --> StagingArea
        StagingArea -- "git commit" --> LocalBranch
        LocalBranch -. "git push" .-> RemoteFeature

        %% Relación de remotes
        Remotes[Remotes: Lista de servidores] --> origin
    """)
    return


@app.cell
def _(show):
    show("git fetch origin")
    return


@app.cell
def _(show):
    show("git branch -r")
    return


@app.cell
def _(mo):
    mo.md("""
    La rama del compañero está en `origin/feature/login`. La creamos
    localmente vinculada al remoto:
    """)
    return


@app.cell
def _(show):
    show("git checkout -b feature/login origin/feature/login")
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(mo):
    mo.md("""
    > **`git fetch`** descarga los cambios remotos sin tocar tu working directory.
    > **`git pull`** es `fetch` + `merge` automático. Usa `fetch` cuando quieras
    > revisar qué llegó antes de integrarlo.
    """)
    return


@app.cell
def _(show):
    show("git checkout main")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 7 · Fusionar ramas — `git merge`

    Integramos `feature/health-endpoint` en `main`. El flag `--no-ff` crea
    un commit de merge explícito aunque sea posible el fast-forward.
    """)
    return


@app.cell
def _(show):
    show("git merge feature/health-endpoint --no-ff -m 'merge: integrate health endpoint'")
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(show):
    show("dir")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 8 · Deshacer commits locales — `git reset`

    `reset` mueve el puntero `HEAD` hacia atrás. Tiene tres modos:

    | Modo | HEAD | Stage | Working dir |
    |------|------|-------|-------------|
    | `--soft` | ✅ mueve | sin cambios (staged) | sin cambios |
    | `--mixed` *(default)* | ✅ mueve | limpia stage | sin cambios |
    | `--hard` | ✅ mueve | limpia stage | **⚠️ borra cambios** |

    Hacemos un commit malo para practicar:
    """)
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "error.py").write_text("raise RuntimeError('oops')\n")
    show("git add error.py && git commit -m \"feat: this is a mistake\"")
    return


@app.cell
def _(show):
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    **`--soft`**: deshace el commit pero deja los cambios en stage.
    Útil para reescribir el mensaje o dividir el commit:
    """)
    return


@app.cell
def _(show):
    show("git reset --soft HEAD~1")
    return


@app.cell
def _(show):
    show("git status")
    return


@app.cell
def _(mo):
    mo.md("""
    El fichero sigue en stage. Lo volvemos a commitear para el siguiente demo:
    """)
    return


@app.cell
def _(show):
    show("git commit -m \"feat: mistake restored for demo'\"")
    return


@app.cell
def _(show):
    show(cmd="git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    **`--hard`**: deshace el commit **y borra los cambios** del working directory.
    Úsalo sólo en commits que no hayas subido a nadie:
    """)
    return


@app.cell
def _(show):
    show("git reset --hard HEAD~1")
    return


@app.cell
def _(show):
    show("git log --oneline && ls")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 9 · Deshacer commits publicados — `git revert`

    `revert` **no reescribe** el historial: crea un nuevo commit que invierte
    los cambios. Es seguro usarlo en commits que ya están en `origin`.

    Introducimos un bug en producción:
    """)
    return


@app.cell
def _(REPO: "Path", show):
    content = (REPO / "main.py").read_text()
    (REPO / "main.py").write_text(content + "\n# BUG: rompe producción\nraise RuntimeError('critical error')\n")
    show("git add main.py && git commit -m \"fix: hotfix (introduced bug by mistake)\"")
    return


@app.cell
def _(show):
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    Revertimos el último commit sin tocar el historial anterior:
    """)
    return


@app.cell
def _(show):
    show("git revert HEAD --no-edit")
    return


@app.cell
def _(show):
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    El historial crece en vez de cambiar: el commit del bug sigue ahí,
    y ahora hay uno nuevo que lo deshace. Cualquier compañero que ya hubiera
    hecho `pull` puede hacer `pull` de nuevo sin conflictos.

    > ⚠️ Usa **`reset`** para commits locales que nadie ha descargado.
    > Usa **`revert`** para commits que ya están en `origin`.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 10 · Push a producción vía Pull Request

    El flujo profesional nunca hace push directamente a `main`. En su lugar:

    ```
    1. git checkout -b feature/mi-cambio
    2. commits en la rama
    3. git push origin feature/mi-cambio
    4. Abrir Pull Request en GitHub
    5. Code review + aprobación
    6. Merge a main desde GitHub
    7. git pull origin main
    ```

    Preparamos la rama final con la documentación:
    """)
    return


@app.cell
def _(show):
    show("git checkout -b feature/add-docs")
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "DOCS.md").write_text(
        "# Documentación de la API\n\n"
        "## Endpoints\n\n"
        "- `GET /` — bienvenida\n"
        "- `GET /health` — estado del servicio\n"
        "- `POST /login` — autenticación\n\n"
        "## Instalación\n\n"
        "```bash\nuv sync\nuvicorn app:app --reload\n```\n"
    )
    show("git add DOCS.md && git commit -m \"docs: add API documentation\"")
    return


@app.cell
def _(show):
    show("git push origin feature/add-docs")
    return


@app.cell
def _(mo):
    mo.md("""
    En este punto, en **GitHub**:
    **"Compare & pull request"**. Abres el PR, describes el cambio,
    asignas revisores, y esperás la aprobación.

    Simulamos el merge del PR (en GitHub es un click):
    """)
    return


@app.cell
def _(show):
    show("git checkout main")
    return


@app.cell
def _(show):
    show('git merge feature/add-docs --no-ff -m "Merge pull request #1: docs: add API documentation"')
    return


@app.cell
def _(show):
    show("git push origin main")
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## ✅ Resumen

    Has completado el flujo completo de Git:

    | Comando | Para qué sirve |
    |---------|---------------|
    | `git init` | Inicializar un repositorio |
    | `git add` | Pasar cambios al staging area |
    | `git commit -m` | Guardar un snapshot |
    | `git log --oneline --graph` | Ver el historial |
    | `git checkout -b` | Crear y cambiar de rama |
    | `git merge --no-ff` | Fusionar una rama |
    | `git fetch` / `git pull` | Descargar cambios remotos |
    | `git push` | Subir cambios al remote |
    | `git reset --soft/--hard` | Deshacer commits locales |
    | `git revert` | Deshacer commits publicados |

    ### Flujo del día a día

    ```bash
    git pull origin main          # sincroniza antes de empezar
    git checkout -b feature/xxx   # crea tu rama
    # ... edita ficheros ...
    git add .
    git commit -m "feat: ..."
    git push origin feature/xxx   # sube la rama
    # abre PR en GitHub → review → merge
    git checkout main
    git pull origin main          # sincroniza el merge
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
