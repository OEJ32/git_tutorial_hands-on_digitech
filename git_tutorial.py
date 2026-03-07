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
def _imports():
    import os
    import marimo as mo
    import subprocess
    import shutil
    from pathlib import Path

    # ── Sandbox aislado ────────────────────────────────────────────────────────
    # El tutorial siempre trabaja en una subcarpeta limpia llamada 'sandbox/'
    # dentro del directorio del notebook, NUNCA en el repo padre.
    # Si ya existe de una ejecución anterior, la borramos para empezar desde cero.
    _notebook_dir = Path(__file__).parent.resolve()
    REPO: Path = _notebook_dir / "sandbox"

    def _force_rmtree(path):
        """shutil.rmtree seguro en Windows: los ficheros .git son read-only."""
        import stat
        def _on_error(func, p, exc):
            os.chmod(p, stat.S_IWRITE)
            func(p)
        shutil.rmtree(path, onexc=_on_error)

    if REPO.exists():
        _force_rmtree(REPO)
    REPO.mkdir(parents=True)

    # También limpiamos los artefactos del "compañero B" de ejecuciones anteriores
    for _leftover in ["origin.git", "alumno-b"]:
        _p = _notebook_dir / _leftover
        if _p.exists():
            _force_rmtree(_p)
    # ──────────────────────────────────────────────────────────────────────────

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
        """Run a git/shell command and return a marimo markdown code block."""
        out = git(cmd, cwd=cwd)
        return mo.md(f"```\n$ {cmd}\n{out}\n```")

    return Path, REPO, git, mo, os, show, subprocess


@app.cell
def _(mo):
    mo.md("""
    # 🌿 Git & GitHub — Tutorial Interactivo

    Este notebook crea un repositorio Git **real** desde cero y ejecuta cada
    comando en tu máquina. Sigue las secciones en orden.

    ### Antes de empezar, necesitas:
    - **`gh` CLI** instalado y autenticado → `gh auth login`
    - Un fichero **`.env`** en la misma carpeta que este notebook con:

    ```
    GH_USERNAME=tu_usuario_de_github
    ```

    ---
    """)
    return


@app.cell
def _(Path, os):
    from dotenv import load_dotenv

    dotenv_path = Path(__file__).parent.resolve() / ".env"
    load_dotenv(dotenv_path=dotenv_path)

    GH_USERNAME = os.getenv("GH_USERNAME")
    REPO_NAME = "git_tutorial_hands-on"

    assert GH_USERNAME, (
        f"❌ No se encontró GH_USERNAME en {dotenv_path}. "
        "Crea el fichero .env con GH_USERNAME=tu_usuario"
    )

    print(f"✅ GitHub user: {GH_USERNAME}")
    print(f"✅ Repo name  : {REPO_NAME}")
    return GH_USERNAME, REPO_NAME


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 1 · Crear el repositorio local

    `git init` convierte una carpeta normal en un repositorio Git.
    Crea una carpeta oculta `.git/` que guarda todo el historial.
    """)
    return


@app.cell
def _(show):
    show("git init")
    return


@app.cell
def _(show):
    # Se debería haber ejecutado antes, pero por si acaso, aseguramos que no hay un remoto "origin" configurado
    show("git remote remove origin")
    return


@app.cell
def _(show):
    show("git branch -M main")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 2 · Crear el repositorio remoto en GitHub

    Usamos la **GitHub CLI (`gh`)** para crear el repo directamente desde
    la terminal sin salir del notebook.
    """)
    return


@app.cell
def _(GH_USERNAME, REPO_NAME, git, mo, os, subprocess):
    # Comprobamos si el repo ya existe en GitHub para ser idempotente
    check = git(f"gh repo view {GH_USERNAME}/{REPO_NAME} --json name -q .name")

    if check.strip() == REPO_NAME:
        result_create = f"ℹ️  El repo '{REPO_NAME}' ya existe en GitHub — no se vuelve a crear."
    else:
        # Lista de argumentos: evita problemas de quoting en Windows
        r = subprocess.run(
            ["gh", "repo", "create", REPO_NAME, "--public",
             "--description", "Repo del tutorial de Git"],
            capture_output=True, text=True, env=os.environ.copy()
        )
        out = (r.stdout + r.stderr).strip()
        result_create = out if out else f"✅ Repo '{REPO_NAME}' creado en GitHub."

    mo.md(f"```\n{result_create}\n```")
    return


@app.cell
def _(GH_USERNAME, REPO_NAME, git, mo):
    # Vinculamos el remote 'origin' (idempotente)
    existing_remote = git("git remote")

    if "origin" in existing_remote.splitlines():
        git(f"git remote set-url origin https://github.com/{GH_USERNAME}/{REPO_NAME}.git")
        msg_remote = "ℹ️  'origin' ya existía — URL actualizada."
    else:
        git(f"git remote add origin https://github.com/{GH_USERNAME}/{REPO_NAME}.git")
        msg_remote = "✅ Remote 'origin' añadido."

    mo.md(f"```\n{msg_remote}\n$ git remote -v\n{git('git remote -v')}\n```")
    return (msg_remote,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 3 · Primer commit

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
    (REPO / "main.py").write_text(
        'def main():\n    print("Hello from git-tutorial for Digitech!")\n\n'
        'if __name__ == "__main__":\n    main()\n'
    )
    show("git status")
    return


@app.cell
def _(mo):
    mo.md("""
    Git aún no los rastrea. Los añadimos al *staging area* con `git add`:
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
    ## 4 · Construir historial

    Añadimos más ficheros para tener un historial con el que practicar.
    """)
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "requirements.txt").write_text("fastapi>=0.100\nuvicorn\nhttpx\n")
    show('git add requirements.txt && git commit -m "chore: add requirements.txt"')
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / ".gitignore").write_text("__pycache__/\n*.pyc\n.venv/\n.env\n")
    show('git add .gitignore && git commit -m "chore: add .gitignore"')
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "main.py").write_text(
        "from fastapi import FastAPI\n\napp = FastAPI()\n\n"
        "@app.get('/')\ndef root():\n    return {'message': 'Hola mundo!'}\n"
    )
    show('git add main.py && git commit -m "feat: migrate app to FastAPI"')
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
    ## 5 · Explorar el historial

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
    Las líneas en **`-`** son las eliminadas y las de **`+`** las añadidas.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 6 · Ramas — `git branch` · `git checkout`

    Una rama es un puntero a un commit. Trabajar en ramas mantiene `main`
    estable mientras desarrollas nuevas funcionalidades.
    """)
    return


@app.cell
def _(mo):
    mo.mermaid("""
    %%{init: { 'theme': 'base' }}%%
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
    show('git add health.py && git commit -m "feat: add /health endpoint"')
    return


@app.cell
def _(REPO: "Path", show):
    (REPO / "health.py").write_text(
        "from app import app\nfrom datetime import datetime\n\n"
        "@app.get('/health')\ndef health():\n    return {'status': 'ok', 'version': '1.0.0', 'time': str(datetime.now())}\n"
    )
    show('git add health.py && git commit -m "feat: add version to health response"')
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
    show("git checkout main")
    return


@app.cell
def _(show):
    show("git branch -v")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 7 · Coger la rama de un compañero

    Simulamos que un **compañero** ha subido su rama al remoto.
    Nosotros la descargamos y continuamos su trabajo.
    """)
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
            RemoteFeature[origin/feature_1]
        end
        LocalBranch -- "git add" --> StagingArea
        StagingArea -- "git commit" --> LocalBranch
        LocalBranch -. "git push" .-> RemoteFeature
    """)
    return


@app.cell
def _(REPO: "Path", git, mo):
    # Simulamos al compañero B usando un clon bare local
    origin_path = REPO.parent / "origin.git"
    alumno_b_path = REPO.parent / "alumno-b"

    git(f"git clone --bare {REPO} {origin_path}", cwd=REPO.parent)
    git(f"git clone {origin_path} {alumno_b_path}", cwd=REPO.parent)
    git("git config user.name Companero-B", cwd=alumno_b_path)
    git("git config user.email b@curso.es", cwd=alumno_b_path)
    git("git checkout -b feature/login", cwd=alumno_b_path)
    (alumno_b_path / "login.py").write_text(
        "from app import app\n\n"
        "@app.post('/login')\ndef login(username: str, password: str):\n"
        "    if username == 'admin' and password == '1234':\n"
        "        return {'token': 'abc123'}\n"
        "    return {'error': 'invalid credentials'}\n"
    )
    git("git add login.py", cwd=alumno_b_path)
    git('git commit -m "feat: add /login endpoint"', cwd=alumno_b_path)
    git("git push origin feature/login", cwd=alumno_b_path)

    # Añadimos el bare como remote "local-origin" para no pisar el GitHub origin
    git(f"git remote add local-origin {origin_path}")
    msg_b = "✅ Compañero B creado y rama feature/login subida al origin local."
    mo.md(f"```\n{msg_b}\n```")
    return


@app.cell
def _(show):
    show("git fetch local-origin")
    return


@app.cell
def _(show):
    show("git branch -r")
    return


@app.cell
def _(mo):
    mo.md("""
    La rama del compañero está en `local-origin/feature/login`. La creamos localmente:
    """)
    return


@app.cell
def _(show):
    show("git checkout -b feature/login local-origin/feature/login")
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
    ## 8 · Fusionar ramas — `git merge`

    Integramos `feature/health-endpoint` en `main`. El flag `--no-ff` crea
    un commit de merge explícito aunque sea posible el fast-forward.
    """)
    return


@app.cell
def _(show):
    show('git merge feature/health-endpoint --no-ff -m "merge: integrate health endpoint"')
    return


@app.cell
def _(show):
    show("git log --oneline --graph --all")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 9 · Deshacer commits locales — `git reset`

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
    show('git add error.py && git commit -m "feat: this is a mistake"')
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
    show('git commit -m "feat: mistake restored for demo"')
    return


@app.cell
def _(show):
    show("git log --oneline")
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
    show("git log --oneline")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 10 · Deshacer commits publicados — `git revert`

    `revert` **no reescribe** el historial: crea un nuevo commit que invierte
    los cambios. Es seguro usarlo en commits que ya están en `origin`.

    Introducimos un bug en producción:
    """)
    return


@app.cell
def _(REPO: "Path", show):
    content = (REPO / "main.py").read_text()
    (REPO / "main.py").write_text(
        content + "\n# BUG: rompe producción\nraise RuntimeError('critical error')\n"
    )
    show('git add main.py && git commit -m "fix: hotfix (introduced bug by mistake)"')
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
    ## 11 · Push a producción vía Pull Request

    El flujo profesional **nunca hace push directamente a `main`**. En su lugar:

    ```
    1. git checkout -b feature/mi-cambio
    2. commits en la rama
    3. git push origin feature/mi-cambio
    4. Abrir Pull Request en GitHub
    5. Code review + aprobación
    6. ✅ Tú haces el merge en GitHub
    7. git pull origin main
    ```
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
    show('git add DOCS.md && git commit -m "docs: add API documentation"')
    return


@app.cell
def _(git, mo, msg_remote):
    # msg_remote is consumed only to force marimo to run section 2 (remote setup) first.
    _ = msg_remote
    # Subimos main (base) y la rama en orden garantizado antes de crear la PR.
    # Ambos en la misma celda para que el DAG de marimo los secuencie correctamente.
    out_main   = git("git push origin main --force-with-lease")
    out_branch = git("git push origin feature/add-docs")
    mo.md(
        f"```\n$ git push origin main\n{out_main}\n\n"
        f"$ git push origin feature/add-docs\n{out_branch}\n```"
    )
    return


@app.cell
def _(GH_USERNAME, REPO: "Path", REPO_NAME, mo, os, subprocess):
    # Lista de argumentos: evita problemas de quoting en Windows
    r_pr = subprocess.run(
        [
            "gh", "pr", "create",
            "--repo",  f"{GH_USERNAME}/{REPO_NAME}",
            "--base",  "main",
            "--head",  "feature/add-docs",
            "--title", "docs: add API documentation",
            "--body",  "Añade DOCS.md con la descripción de los endpoints y las instrucciones de instalación.",
        ],
        capture_output=True, text=True,
        cwd=str(REPO), env=os.environ.copy()
    )
    pr_out = (r_pr.stdout + r_pr.stderr).strip()
    mo.md(f"```\n{pr_out}\n```")
    return


@app.cell
def _(mo):
    mo.md("""
    ### 🎯 Tu turno — Debes hacer el merge tú mismo

    1. Abre la URL de la PR que aparece arriba en tu navegador
    2. Revisa los cambios en la pestaña **"Files changed"**
    3. Pulsa **"Merge pull request"** → **"Confirm merge"**
    4. Una vez mergeada, vuelve aquí y ejecuta la siguiente celda
    """)
    return


@app.cell
def _(GH_USERNAME, REPO_NAME, mo, os, subprocess):
    # Lista de argumentos: evita quoting de '.[0].number' en Windows cmd.exe
    r_check = subprocess.run(
        [
            "gh", "pr", "list",
            "--repo",  f"{GH_USERNAME}/{REPO_NAME}",
            "--head",  "feature/add-docs",
            "--state", "merged",
            "--json",  "number",
            "-q",      ".[0].number",
        ],
        capture_output=True, text=True, env=os.environ.copy()
    )
    pr_state = (r_check.stdout + r_check.stderr).strip()

    if pr_state:
        status_msg = f"✅ PR #{pr_state} mergeada correctamente. ¡Bien hecho!"
    else:
        status_msg = "⏳ La PR todavía no está mergeada. Hazlo en GitHub y vuelve a ejecutar esta celda."

    mo.md(f"> {status_msg}")
    return


@app.cell
def _(mo):
    mo.md("""
    Sincronizamos nuestro `main` local con el merge que acabas de hacer en GitHub:
    """)
    return


@app.cell
def _(show):
    show("git checkout main && git pull origin main")
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

    Has completado el flujo completo de Git + GitHub:

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
    | `gh repo create` | Crear repo en GitHub desde CLI |
    | `gh pr create` | Abrir una Pull Request desde CLI |

    ### Flujo del día a día

    ```bash
    git pull origin main          # sincroniza antes de empezar
    git checkout -b feature/xxx   # crea tu rama
    # ... edita ficheros ...
    git add .
    git commit -m "feat: ..."
    git push origin feature/xxx   # sube la rama
    # abre PR en GitHub → review → merge (tú o tu equipo)
    git checkout main
    git pull origin main          # sincroniza el merge
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
