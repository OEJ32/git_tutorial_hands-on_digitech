# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.20.2",
#     "duckdb>=0.9",
#     "pandas>=2.0",
#     "ollama>=0.1",
#     "python-dotenv>=1.0",
# ]
# ///

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  SESIÓN PRÁCTICA — PIPELINE DE EMAILS CON BUGS                         ║
# ║  Hay exactamente 8 bugs escondidos en este notebook.                   ║
# ║  Encuéntralos, corrígelos y explica en voz alta por qué cada uno falla.║
# ╚══════════════════════════════════════════════════════════════════════════╝

import marimo

__generated_with = "0.23.0"
app = marimo.App(width="full", app_title="Pipeline Challenge — Find the Bugs")


@app.cell
def _():
    import os, sys, json, re, logging, io
    from pathlib import Path
    from datetime import datetime, timezone
    import marimo as mo
    import pandas as pd
    import duckdb

    MOCK_EMAILS = [
        {"id":"msg_001","threadId":"msg_001","labelIds":["INBOX","UNREAD"],
         "snippet":"I have asked for a refund three times now...",
         "internalDate":"1740038100000",
         "payload":{"headers":[
             {"name":"Subject","value":"URGENT: REFUND REQUEST - ORDER #998822"},
             {"name":"From","value":"angry.customer@example.com"},
             {"name":"To","value":"support@techcompany.com"},
             {"name":"Date","value":"Thu, 20 Feb 2026 09:15:00 +0000"}]},
         "body_text_content":"I have asked for a refund three times now! Your service is not working as advertised. I demand my money back immediately or I will contact my bank."},
        {"id":"msg_002","threadId":"msg_002","labelIds":["INBOX","UNREAD"],
         "snippet":"I cannot believe how buggy this software is...",
         "internalDate":"1740124200000",
         "payload":{"headers":[
             {"name":"Subject","value":"Complete waste of time - Cancel my account"},
             {"name":"From","value":"dissatisfied@corp.net"},
             {"name":"To","value":"support@techcompany.com"},
             {"name":"Date","value":"Fri, 21 Feb 2026 14:30:00 +0000"}]},
         "body_text_content":"I cannot believe how buggy this software is. I've lost 3 hours of work. Cancel my subscription NOW. I expect a confirmation within the hour."},
        {"id":"msg_003","threadId":"msg_003","labelIds":["INBOX"],
         "snippet":"Just following up on my previous email...",
         "internalDate":"1739606400000",
         "payload":{"headers":[
             {"name":"Subject","value":"Re: Partnership Proposal"},
             {"name":"From","value":"partner@business.com"},
             {"name":"To","value":"sales@techcompany.com"},
             {"name":"Date","value":"Sun, 15 Feb 2026 10:00:00 +0000"}]},
         "body_text_content":"Hi Sarah,\n\nJust following up on my previous email from last week. Have you had a chance to review the proposal?\n\n> On Mon, Feb 10, 2026 at 9:00 AM, Sarah wrote:\n> > Thanks, we will review it.\n"},
        {"id":"msg_004","threadId":"msg_004","labelIds":["INBOX","UNREAD"],
         "snippet":"I haven't heard back regarding the login issue...",
         "internalDate":"1739177100000",
         "payload":{"headers":[
             {"name":"Subject","value":"Checking in on ticket #5544"},
             {"name":"From","value":"john.doe@users.com"},
             {"name":"To","value":"support@techcompany.com"},
             {"name":"Date","value":"Mon, 10 Feb 2026 08:45:00 +0000"}]},
         "body_text_content":"Hello,\n\nI haven't heard back regarding the login issue I reported. Is there any update? It's been 5 days.\n\nBest,\nJohn"},
        {"id":"msg_005","threadId":"msg_005","labelIds":["INBOX"],
         "snippet":"I really like the new design!...",
         "internalDate":"1740220800000",
         "payload":{"headers":[
             {"name":"Subject","value":"Feedback on the new dashboard prototype"},
             {"name":"From","value":"beta.tester@innovate.io"},
             {"name":"To","value":"product@techcompany.com"},
             {"name":"Date","value":"Sat, 22 Feb 2026 11:20:00 +0000"}]},
         "body_text_content":"Hey team,\n\nI really like the new design! However, it would be great if we could export the charts to CSV. Is that something you can add to the roadmap?\n\nCheers,"},
    ]

    MOCK_ANALYSIS = [
        {"id":"msg_001","sentiment":"very_negative","sentiment_score":0.95,"topic":"Complaint","confidence":0.96,"date_parsed":"2026-02-20T09:15:00","subject":"URGENT: REFUND REQUEST","from_addr":"angry.customer@example.com"},
        {"id":"msg_002","sentiment":"very_negative","sentiment_score":0.92,"topic":"Complaint","confidence":0.94,"date_parsed":"2026-02-21T14:30:00","subject":"Complete waste of time","from_addr":"dissatisfied@corp.net"},
        {"id":"msg_003","sentiment":"neutral","sentiment_score":0.50,"topic":"Sales","confidence":0.78,"date_parsed":"2026-02-15T10:00:00","subject":"Re: Partnership Proposal","from_addr":"partner@business.com"},
        {"id":"msg_004","sentiment":"negative","sentiment_score":0.70,"topic":"Complaint","confidence":0.82,"date_parsed":"2026-02-10T08:45:00","subject":"Checking in on ticket #5544","from_addr":"john.doe@users.com"},
        {"id":"msg_005","sentiment":"positive","sentiment_score":0.20,"topic":"New Feature Request","confidence":0.85,"date_parsed":"2026-02-22T11:20:00","subject":"Feedback on dashboard","from_addr":"beta.tester@innovate.io"},
    ]
    return MOCK_EMAILS, datetime, duckdb, json, mo, pd, re, timezone


@app.cell
def _(mo):
    mo.md("""
    # 🐛 Pipeline Challenge — Encuentra los 8 bugs

    Este notebook tiene **8 errores** distribuidos en 4 secciones.
    Cada uno toca un concepto distinto que hemos trabajado en clase.

    | Sección | Concepto que se evalúa |
    |---------|----------------------|
    | A · API y JSON | Parseo de respuestas HTTP |
    | B · RegExp | Patrones y flags |
    | C · DuckDB + Marimo DAG | SQL y reactividad |
    | D · Ollama + Logging | LLM local y niveles de log |

    > **Instrucciones:** ejecuta cada celda. Cuando algo falle (o no funcione como esperas),
    > localiza el bug, corrígelo y explica en voz alta por qué era incorrecto.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🌐 Sección A — API y parseo de JSON
    """)
    return


@app.cell
def _(MOCK_EMAILS, json, mo):

    def parse_email_metadata(email: dict) -> dict:
        """Extrae los metadatos relevantes de un objeto email de la API."""

        # BUG 1 ─────────────────────────────────────────────────────────────
        # La API devuelve los headers como una LISTA de dicts: 
        # [{"name": "Subject", "value": "..."}, {"name": "From", "value": "..."}, ...]
        # El código de abajo accede a ellos como si fueran un dict directo.
        # ¿Qué pasará cuando intentemos hacer headers["Subject"]?

        headers = email["payload"]["headers"]

        return {
            "id":      email["id"],
            "subject": headers["Subject"],
            "from":    headers["From"],
            "date":    headers["Date"],
            "internal_date_ms": int(email["internalDate"]),
        }

    # Probamos con el primer email
    result = parse_email_metadata(MOCK_EMAILS[0])
    mo.md(f"✅ Metadatos extraídos:\n```json\n{json.dumps(result, indent=2)}\n```")
    return


@app.cell
def _(MOCK_EMAILS, datetime, mo, timezone):

    def get_age_days(email: dict) -> float:
        """Calcula cuántos días lleva esperando este email."""

        # BUG 2 ─────────────────────────────────────────────────────────────
        # internalDate viene de la API como string de milisegundos.
        # El código hace la conversión... pero algo está mal en las unidades.
        # Pista: ¿cuántos milisegundos hay en un segundo?

        internal_date_ms = int(email["internalDate"])
        email_datetime   = datetime.fromtimestamp(internal_date_ms / 1_000, tz=timezone.utc)
        now              = datetime.now(tz=timezone.utc)
        age_seconds      = (now - email_datetime).total_seconds()
        age_days         = age_seconds

        return age_days

    ages = [(e["id"], round(get_age_days(e), 1)) for e in MOCK_EMAILS]

    if all(a > 1000 for _, a in ages):
        mo.callout(
            mo.md(f"⚠️ Los emails tienen entre {min(a for _,a in ages):,.0f} y {max(a for _,a in ages):,.0f} días de antigüedad.\n\n"
                  "Eso son más de **2 años**. ¿Es eso posible para emails de febrero 2026?\n\n"
                  "¿Qué unidades espera `datetime.fromtimestamp()`?"),
            kind="warn"
        )
    else:
        mo.md(f"✅ Antigüedades calculadas: {ages}")
    return (ages,)


@app.cell
def _(ages):
    ages
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🧹 Sección B — RegExp: limpieza de emails
    """)
    return


@app.cell
def _(MOCK_EMAILS, mo, re):

    # BUG 3 ─────────────────────────────────────────────────────────────────
    # El email msg_003 tiene líneas citadas que empiezan por ">".
    # El patrón _RE_QUOTED debería eliminarlas, pero no lo hace.
    # Pista: ¿qué significa "^" en una regex? ¿Qué flag necesitas para que
    # "^" match el inicio de CADA LÍNEA en lugar del inicio del string entero?

    _RE_QUOTED = re.compile(r"^>+.*$")           # ← falta un flag
    _RE_URL    = re.compile(r"https?://\S+", re.IGNORECASE)
    _RE_EMAIL  = re.compile(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}")
    _RE_WS     = re.compile(r"[ \t]+")

    def clean_body(raw: str) -> str:
        t = raw or ""
        t = _RE_QUOTED.sub("", t)
        t = _RE_URL.sub("[URL]", t)
        t = _RE_EMAIL.sub("[EMAIL]", t)
        t = _RE_WS.sub(" ", t)
        return t.strip()

    # msg_003 tiene líneas "> On Mon, Feb 10..." — deberían desaparecer
    email_003 = next(e for e in MOCK_EMAILS if e["id"] == "msg_003")
    raw_body  = email_003["body_text_content"]
    cleaned   = clean_body(raw_body)

    still_has_quotes = any(line.strip().startswith(">") for line in cleaned.splitlines())

    mo.vstack([
        mo.md(f"**Original:**\n```\n{raw_body}\n```"),
        mo.md(f"**Cleaned:**\n```\n{cleaned}\n```"),
        mo.callout(
            mo.md("⚠️ Las líneas `>` siguen ahí. El patrón no las eliminó.\n\n¿Qué flag le falta al `re.compile()`?"),
            kind="warn"
        ) if still_has_quotes else mo.callout(mo.md("✅ Limpieza correcta."), kind="success"),
    ])
    return (clean_body,)


@app.cell
def _(MOCK_EMAILS, mo):
    # Esta celda define un widget y lo devuelve para que la siguiente lo use.
    # Observa que las variables NO empiezan por "_" — así Marimo puede
    # pasarlas entre celdas.

    email_selector = mo.ui.dropdown(
        options=[e["id"] for e in MOCK_EMAILS],
        value="msg_003",
        label="Email a inspeccionar",
    )
    email_selector
    return


@app.cell
def _(MOCK_EMAILS, clean_body, mo):
    # BUG 4 ─────────────────────────────────────────────────────────────────
    # Esta celda depende de `email_selector` para saber qué email mostrar.
    # Pero cuando cambias el selector, el resultado NO se actualiza.
    # Pista: ¿cómo sabe Marimo que esta celda depende de la anterior?
    # ¿Está `email_selector` en la firma de la función?

    # (mira la línea `def _(mo, MOCK_EMAILS, clean_body, email_selector):` arriba)
    # Ahora mira el cuerpo de la función — ¿realmente se USA email_selector.value?

    selected_em = next(e for e in MOCK_EMAILS if e["id"] == "msg_003")  # ← siempre msg_003
    raw  = selected_em["body_text_content"]
    cln  = clean_body(raw)

    mo.hstack([
        mo.md(f"**Before ({len(raw)} chars):**\n```\n{raw}\n```"),
        mo.md(f"**After ({len(cln)} chars):**\n```\n{cln}\n```"),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🦆 Sección C — DuckDB en memoria y el DAG de Marimo
    """)
    return


@app.cell
def _(MOCK_EMAILS, clean_body, duckdb, json, pd):

    _rows = []
    for _e in MOCK_EMAILS:
        _h = {h["name"]: h["value"] for h in _e["payload"]["headers"]}
        _br = _e["body_text_content"]
        _bc = clean_body(_br)
        _rows.append({
            "id":         _e["id"],
            "from_addr":  _h.get("From", ""),
            "subject":    _h.get("Subject", ""),
            "date_raw":   _h.get("Date", ""),
            "label_ids":  json.dumps(_e["labelIds"]),
            "body_raw":   _br,
            "body_clean": _bc,
            "word_count": len(_bc.split()),
        })

    _df_emails = pd.DataFrame(_rows)
    db_con = duckdb.connect()                              # in-memory DuckDB
    db_con.execute("CREATE TABLE emails AS SELECT * FROM db_con")

    # Devolvemos la conexión para que otras celdas puedan usarla
    return


if __name__ == "__main__":
    app.run()
