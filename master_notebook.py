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

import marimo

__generated_with = "0.22.4"
app = marimo.App(
    width="full",
    app_title="Email Pipeline — Master Notebook (2h)",
)


@app.cell
def _():
    # uv run marimo edit master_notebook.py
    # Inspect graphs and built-in functionalities in the web
    return


@app.cell
def _():
    import os, sys, json, re, logging, io, random
    from pathlib import Path
    from datetime import datetime, timezone
    from email.utils import parsedate_to_datetime
    import marimo as mo
    import pandas as pd

    import duckdb

    MOCK_EMAILS = [
        {"id": "msg_001",
            "threadId": "msg_001", "labelIds": ["INBOX", "UNREAD"],"snippet": "I have asked for a refund...",
            "internalDate": "1740038100000","payload": {
                "headers": [
                    {"name": "Subject", "value": "URGENT: REFUND REQUEST - ORDER #998822"},
                    {"name": "From", "value": "angry.customer@example.com"},
                    {"name": "To", "value": "support@techcompany.com"},
                    {"name": "Date", "value": "Thu, 20 Feb 2026 09:15:00 +0000"}
                ]
            },
            "body_text_content": """
            <div><b>URGENT:</b> Please &nbsp; read this!</div>

            I have asked for a refund three times now! Your service is not working.
            I expect a call at +1 (555) 010-9988 or email me at admin@personal.net.

            Check my status here: https://portal.techcompany.com/orders/998822

            > On Mon, Feb 10, 2026 at 9:00 AM, Support wrote:
            > > We are looking into your request.

            ---------- Forwarded message ---------
            From: Billing <billing@techcompany.com>
            Date: Feb 09, 2026
            Subject: Invoice #998822

            -- 
            Sent from my iPhone

            This email and any attachments are confidential and intended solely for the use of the individual...
            """
        },
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
        {"id":"msg_006","threadId":"msg_006","labelIds":["INBOX"],
         "snippet":"I love the app but my eyes hurt at night...",
         "internalDate":"1740009600000",
         "payload":{"headers":[
             {"name":"Subject","value":"Idea: Dark Mode"},
             {"name":"From","value":"night.owl@creative.com"},
             {"name":"To","value":"feedback@techcompany.com"},
             {"name":"Date","value":"Wed, 19 Feb 2026 23:00:00 +0000"}]},
         "body_text_content":"I love the app but my eyes hurt at night. Please add a dark mode option!\n\nSent from my iPhone"},
        {"id":"msg_007","threadId":"msg_007","labelIds":["INBOX","UNREAD"],
         "snippet":"Steps to reproduce: 1. Go to settings...",
         "internalDate":"1740232800000",
         "payload":{"headers":[
             {"name":"Subject","value":"Bug: 500 Error when uploading avatar"},
             {"name":"From","value":"qa.external@testing.com"},
             {"name":"To","value":"bugs@techcompany.com"},
             {"name":"Date","value":"Sat, 22 Feb 2026 15:00:00 +0000"}]},
         "body_text_content":"Steps to reproduce:\n1. Go to settings\n2. Click upload avatar\n3. Select image.png\n4. Error 500 appears.\n\nPlease fix this as we cannot update our profiles."},
        {"id":"msg_008","threadId":"msg_008","labelIds":["INBOX","UNREAD"],
         "snippet":"The app crashes immediately after the splash screen...",
         "internalDate":"1740069900000",
         "payload":{"headers":[
             {"name":"Subject","value":"Crash on startup (Windows 11)"},
             {"name":"From","value":"win.user@legacy.com"},
             {"name":"To","value":"support@techcompany.com"},
             {"name":"Date","value":"Thu, 20 Feb 2026 16:45:00 +0000"}]},
         "body_text_content":"The app crashes immediately after the splash screen on my Windows 11 machine.\n\n[LOG CONTENT REDACTED]"},
        {"id":"msg_009","threadId":"msg_009","labelIds":["INBOX"],
         "snippet":"We are interested in the enterprise plan...",
         "internalDate":"1739875800000",
         "payload":{"headers":[
             {"name":"Subject","value":"Question about Enterprise pricing"},
             {"name":"From","value":"cto@bigcorp.com"},
             {"name":"To","value":"sales@techcompany.com"},
             {"name":"Date","value":"Tue, 18 Feb 2026 09:30:00 +0000"}]},
         "body_text_content":"Hi,\n\nWe are interested in the enterprise plan. Do you offer volume discounts for 50+ seats?\n\nThanks,"},
        {"id":"msg_010","threadId":"msg_010","labelIds":["INBOX"],
         "snippet":"We would like to invite you to speak...",
         "internalDate":"1737806400000",
         "payload":{"headers":[
             {"name":"Subject","value":"Invitation: Tech Summit 2026"},
             {"name":"From","value":"organizer@techsummit.com"},
             {"name":"To","value":"info@techcompany.com"},
             {"name":"Date","value":"Sat, 25 Jan 2026 12:00:00 +0000"}]},
         "body_text_content":"Dear Team,\n\nWe would like to invite you to speak at the upcoming Tech Summit.\n\nRegister here: https://techsummit.com/register"},
    ]

    MOCK_ANALYSIS = [
        {"id":"msg_001","sentiment":"very_negative","sentiment_score":0.95,"topic":"Complaint","confidence":0.96,"date_parsed":"2026-02-20T09:15:00","subject":"URGENT: REFUND REQUEST - ORDER #998822","from_addr":"angry.customer@example.com","summary":"Customer demands refund after repeated ignored requests."},
        {"id":"msg_002","sentiment":"very_negative","sentiment_score":0.92,"topic":"Complaint","confidence":0.94,"date_parsed":"2026-02-21T14:30:00","subject":"Complete waste of time - Cancel my account","from_addr":"dissatisfied@corp.net","summary":"User cancels account due to recurring bugs."},
        {"id":"msg_003","sentiment":"neutral","sentiment_score":0.50,"topic":"Sales","confidence":0.78,"date_parsed":"2026-02-15T10:00:00","subject":"Re: Partnership Proposal","from_addr":"partner@business.com","summary":"Partner follows up on unanswered proposal."},
        {"id":"msg_004","sentiment":"negative","sentiment_score":0.70,"topic":"Complaint","confidence":0.82,"date_parsed":"2026-02-10T08:45:00","subject":"Checking in on ticket #5544","from_addr":"john.doe@users.com","summary":"User has received no response to login issue."},
        {"id":"msg_005","sentiment":"positive","sentiment_score":0.20,"topic":"New Feature Request","confidence":0.85,"date_parsed":"2026-02-22T11:20:00","subject":"Feedback on dashboard prototype","from_addr":"beta.tester@innovate.io","summary":"User praises design and requests CSV export."},
        {"id":"msg_006","sentiment":"positive","sentiment_score":0.25,"topic":"New Feature Request","confidence":0.88,"date_parsed":"2026-02-19T23:00:00","subject":"Idea: Dark Mode","from_addr":"night.owl@creative.com","summary":"User requests dark mode feature."},
        {"id":"msg_007","sentiment":"negative","sentiment_score":0.68,"topic":"Bug","confidence":0.93,"date_parsed":"2026-02-22T15:00:00","subject":"Bug: 500 Error when uploading avatar","from_addr":"qa.external@testing.com","summary":"QA tester reports 500 error on avatar upload."},
        {"id":"msg_008","sentiment":"negative","sentiment_score":0.72,"topic":"Bug","confidence":0.91,"date_parsed":"2026-02-20T16:45:00","subject":"Crash on startup (Windows 11)","from_addr":"win.user@legacy.com","summary":"App crashes on Windows 11 after splash screen."},
        {"id":"msg_009","sentiment":"neutral","sentiment_score":0.45,"topic":"Sales","confidence":0.80,"date_parsed":"2026-02-18T09:30:00","subject":"Question about Enterprise pricing","from_addr":"cto@bigcorp.com","summary":"CTO inquires about enterprise pricing and SLA."},
        {"id":"msg_010","sentiment":"positive","sentiment_score":0.15,"topic":"Other","confidence":0.75,"date_parsed":"2026-01-25T12:00:00","subject":"Invitation: Tech Summit 2026","from_addr":"organizer@techsummit.com","summary":"Organiser invites company to speak at Tech Summit."},
    ]

    def clean_body(raw):
        t = raw or ""
        t = re.sub(r"<[^>]+>", " ", t, flags=re.DOTALL)
        t = re.sub(r"&(?:[a-z]+|#\d+|#x[0-9a-f]+);", " ", t, flags=re.IGNORECASE)
        t = re.sub(r"^>+.*$", "", t, flags=re.MULTILINE)
        t = re.sub(r"-{3,}\s*(forwarded|original)\s+message\s*-{3,}.*", "", t, flags=re.IGNORECASE|re.DOTALL)
        t = re.sub(r"\n[-\u2013_]{2,}\s*\n.*", "", t, flags=re.DOTALL)
        t = re.sub(r"(this\s+e.?mail|confidentiality\s+notice|disclaimer).*", "", t, flags=re.IGNORECASE|re.DOTALL)
        t = re.sub(r"https?://\S+|www\.\S+", "[URL]", t, flags=re.IGNORECASE)
        t = re.sub(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", "[EMAIL]", t)
        t = re.sub(r"\+?[\d\s\-().]{7,15}", "[PHONE]", t)
        t = re.sub(r"[ \t]+", " ", t)
        return re.sub(r"\n{3,}", "\n\n", t).strip()

    return (
        MOCK_ANALYSIS,
        MOCK_EMAILS,
        clean_body,
        datetime,
        duckdb,
        io,
        json,
        logging,
        mo,
        parsedate_to_datetime,
        pd,
        random,
        re,
        timezone,
    )


@app.cell
def _(mo):
    mo.md("""
    # 🔬 Email Pipeline — Master Interactive Notebook
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 📡 Section 1 — JSON Explorer
    Pick an email and a view mode. The annotation panel explains every field.
    """)
    return


@app.cell
def _(MOCK_EMAILS, mo):
    sel_email = mo.ui.dropdown(
        options=[e["id"] for e in MOCK_EMAILS],
        value="msg_001",
        label="📧 Select email",
        searchable=True
    )
    sel_email
    return (sel_email,)


@app.cell
def _(mo):
    json_view = mo.ui.radio(
        options={
            "🗂 Top-level fields (no payload)":              "top",
            "📬 payload.headers — the email envelope":       "headers",
            "📝 body_text_content — raw body text":          "body",
            "🔄 Flattened meta — what 01_fetch saves to disk": "flat",
            "🌳 Full tree including payload":                 "full",
        },
        value="🗂 Top-level fields (no payload)",
        label="View mode",
    )
    json_indent = mo.ui.slider(0, 8, value=2, step=1, label="JSON indent")
    mo.hstack([json_view, json_indent])
    return json_indent, json_view


@app.cell
def _(MOCK_EMAILS, json, json_indent, json_view, mo, sel_email):
    em = next(e for e in MOCK_EMAILS if e["id"] == sel_email.value)
    hdrs_em = {h["name"]: h["value"] for h in em["payload"]["headers"]}
    ind = int(json_indent.value)

    ANNOTATIONS = {
        "top": (
            {k: v for k, v in em.items() if k != "payload"},
            """**Top-level fields returned by the Gmail API:**
    - `id` — unique message ID; used as the filename stem on disk (`msg_001.meta.json`)
    - `threadId` — groups all replies in the same conversation; usually equals `id` in our mock
    - `labelIds` — Gmail's label system (`INBOX`, `UNREAD`, `SENT`…). Not folders — an email can have many labels simultaneously.
    - `snippet` — 100-char preview Gmail shows in the list view (pre-computed server-side)
    - `internalDate` — Unix timestamp **in milliseconds** (÷1000 → seconds, then `datetime.fromtimestamp()`)
    - `body_text_content` — our custom shortcut field. In the real Gmail API the body is base64url-encoded inside `payload.parts[0].body.data`"""
        ),
        "headers": (
            hdrs_em,
            """**Email headers** are key-value pairs defined by RFC 2822. They travel invisibly with every email.

    - `Subject` — the subject line (can be empty or spoofed)
    - `From` — sender display name + address. **Not authenticated** — trivially spoofable. Authentication is via SPF/DKIM/DMARC records.
    - `To` — primary recipient(s)
    - `Date` — RFC 2822 date string: `"Thu, 20 Feb 2026 09:15:00 +0000"`. Parsed with `parsedate_to_datetime()`.
    - `Message-ID` — globally unique ID assigned by the **sending** mail server, e.g. `<msg_001@mock.mail>`

    In `01_fetch_emails.py` this list becomes a dict via:
    ```python
    headers = {h["name"]: h["value"] for h in email["payload"]["headers"]}
    ```"""
        ),
        "body": (
            em["body_text_content"],
            """**body_text_content** is a convenience field we added to the mock API.

    In the **real Gmail API** the body lives at:
    `payload.parts[0].body.data` — encoded in **base64url**

    To decode it:
    ```python
    import base64
    raw = base64.urlsafe_b64decode(data + "==").decode("utf-8")
    ```

    Notice what this body contains that **Step 2 will clean out**:
    - Quoted reply lines starting with `>`
    - Signature block after `--`
    - URLs, email addresses, phone numbers"""
        ),
        "flat": (
            {"id":em["id"],"thread_id":em["threadId"],"label_ids":em["labelIds"],
             "snippet":em["snippet"],"internal_date_ms":int(em["internalDate"]),
             "from":hdrs_em.get("From",""),"to":hdrs_em.get("To",""),
             "subject":hdrs_em.get("Subject",""),"date":hdrs_em.get("Date",""),
             "message_id":hdrs_em.get("Message-ID","")},
            """**This is exactly what `01_fetch_emails.py` writes to `data/raw/msg_NNN.meta.json`.**

    Design decisions visible here:
    - Headers list is **flattened** to a dict for easier downstream access
    - `internalDate` string → `int()` cast (Gmail sends it as a string — a quirk of their API)
    - `body_text_content` is saved **separately** to `msg_NNN.txt` — not in this file
    - Only fields actually used downstream are kept; the raw `payload` tree is discarded

    This separation means you can re-clean bodies (Step 2) without re-fetching from the API."""
        ),
        "full": (
            em,
            """**Full API response including the `payload` MIME tree.**

    `payload` structure:
    - `mimeType` — `text/plain`, `text/html`, or `multipart/mixed` (with attachments)
    - `headers` — the headers list we already saw
    - `parts` — sub-parts for multipart emails (text body + HTML version + attachments)
    - `body.data` — base64url-encoded content (empty in our mock; we use `body_text_content`)

    For a real multipart email with attachment, `parts` would be:
    ```
    parts[0] → mimeType: text/plain   (the body you read)
    parts[1] → mimeType: text/html    (the HTML version)
    parts[2] → mimeType: application/pdf  (an attachment)
    ```"""
        ),
    }

    data_j, note_j = ANNOTATIONS[json_view.value]
    rendered_j = data_j if isinstance(data_j, str) else json.dumps(data_j, indent=ind, ensure_ascii=False)

    mo.hstack([
        mo.md(f"```json\n{rendered_j}\n```"),
        mo.callout(mo.md(note_j), kind="info"),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ### 1b · Field drill-down — compare one field across all emails
    """)
    return


@app.cell
def _(mo):
    field_pick = mo.ui.dropdown(
        options=["id","threadId","labelIds","snippet","internalDate",
                 "Subject","From","To","Date","body_text_content (first 100 chars)"],
        value="Subject",
        label="Field to inspect across all 10 emails",
        # Hovrer over the methods/functions to see extra parameters to see if any serves your purpose
        # searchable=True,
    )
    field_pick
    return (field_pick,)


@app.cell
def _(MOCK_EMAILS, field_pick, mo, pd):
    rows_fd = []
    for e_fd in MOCK_EMAILS:
        h_fd = {h["name"]: h["value"] for h in e_fd["payload"]["headers"]}
        f = field_pick.value
        if f in ("Subject","From","To","Date"):
            val = h_fd.get(f, "")
        elif f == "body_text_content (first 100 chars)":
            val = e_fd["body_text_content"][:100] + ("…" if len(e_fd["body_text_content"]) > 100 else "")
        else:
            val = str(e_fd.get(f, ""))
        rows_fd.append({"email_id": e_fd["id"], f: val})
    mo.ui.table(pd.DataFrame(rows_fd), selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🧹 Section 2 — RegExp Cleaning
    Turn each cleaning step on/off independently to see exactly what it removes.
    """)
    return


@app.cell
def _(MOCK_EMAILS, mo):
    sel_email_re = mo.ui.dropdown(
        options=[e["id"] for e in MOCK_EMAILS],
        value="msg_001",
        label="Email to clean  (msg_001 — most interesting)",
    )
    sel_email_re
    return (sel_email_re,)


@app.cell
def _(mo):
    t_html   = mo.ui.switch(label="1 · Strip HTML tags",           value=False)
    t_entity = mo.ui.switch(label="2 · Strip HTML entities",       value=False)
    t_quoted = mo.ui.switch(label="3 · Remove quoted lines (>)",   value=False)
    t_fwd    = mo.ui.switch(label="4 · Remove forwarded block",    value=False)
    t_sig    = mo.ui.switch(label="5 · Remove signature (--)",     value=False)
    t_disc   = mo.ui.switch(label="6 · Remove disclaimer",         value=False)
    t_url    = mo.ui.switch(label="7 · URLs → [URL]",              value=False)
    t_email  = mo.ui.switch(label="8 · Emails → [EMAIL]",          value=False)
    t_phone  = mo.ui.switch(label="9 · Phones → [PHONE]",          value=False)
    t_ws     = mo.ui.switch(label="10 · Collapse whitespace",      value=False)
    mo.vstack([
        mo.hstack([t_html, t_entity, t_quoted, t_fwd, t_disc]),
        mo.hstack([t_sig, t_url, t_email, t_phone, t_ws]),
    ])
    return (
        t_disc,
        t_email,
        t_entity,
        t_fwd,
        t_html,
        t_phone,
        t_quoted,
        t_sig,
        t_url,
        t_ws,
    )


@app.cell
def _(
    MOCK_EMAILS,
    mo,
    re,
    sel_email_re,
    t_disc,
    t_email,
    t_entity,
    t_fwd,
    t_html,
    t_phone,
    t_quoted,
    t_sig,
    t_url,
    t_ws,
):

    RE_HTML   = re.compile(r"<[^>]+>",    re.DOTALL)
    RE_ENT    = re.compile(r"&(?:[a-z]+|#\d+|#x[0-9a-f]+);", re.IGNORECASE)
    RE_QUOT   = re.compile(r"^\s+>+.*$",     re.MULTILINE)
    RE_FWD    = re.compile(r"-{3,}\s*(forwarded|original)\s+message\s*-{3,}.*", re.IGNORECASE|re.DOTALL)
    RE_SIG    = re.compile(r"\n[-\u2013_]{2,}\s*\n.*", re.DOTALL)
    RE_DISC   = re.compile(r"(this\s+e.?mail|confidentiality\s+notice|disclaimer).*", re.IGNORECASE|re.DOTALL)
    RE_URL    = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
    RE_EMAILP = re.compile(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}")
    RE_PHONE = re.compile(r"(?<![\w+])\+?[\d\(\)][\d\s().-]{6,24}(?![\w\d])")

    em_re = next(e for e in MOCK_EMAILS if e["id"] == sel_email_re.value)
    raw_re = em_re["body_text_content"]
    steps_log = []
    text_re = raw_re

    if t_html.value:   text_re = RE_HTML.sub(" ", text_re);        steps_log.append("HTML tags")
    if t_entity.value: text_re = RE_ENT.sub(" ", text_re);         steps_log.append("HTML entities")
    if t_quoted.value: text_re = RE_QUOT.sub("", text_re);         steps_log.append("Quoted lines")
    if t_fwd.value:    text_re = RE_FWD.sub("", text_re);          steps_log.append("Forwarded block")
    if t_sig.value:    text_re = RE_SIG.sub("", text_re);          steps_log.append("Signature")
    if t_disc.value:   text_re = RE_DISC.sub("", text_re);         steps_log.append("Disclaimer")
    if t_url.value:    text_re = RE_URL.sub("[URL]", text_re);     steps_log.append("URLs")
    if t_email.value:  text_re = RE_EMAILP.sub("[EMAIL]", text_re);steps_log.append("Emails")
    if t_phone.value:  text_re = RE_PHONE.sub("[PHONE]", text_re); steps_log.append("Phones")
    if t_ws.value:
        text_re = re.sub(r"[ \t]+", " ", text_re)
        text_re = re.sub(r"\n{3,}", "\n\n", text_re)
        steps_log.append("Whitespace")
    text_re = text_re.strip()

    reduction = 100 * (1 - len(text_re) / max(len(raw_re), 1))
    mo.vstack([
        mo.hstack([
            mo.stat(label="Chars before", value=str(len(raw_re))),
            mo.stat(label="Chars after",  value=str(len(text_re))),
            mo.stat(label="Reduction",    value=f"{reduction:.0f}%"),
            mo.stat(label="Words before", value=str(len(raw_re.split()))),
            mo.stat(label="Words after",  value=str(len(text_re.split()))),
        ]),
        mo.md(f"**Active steps:** {', '.join(steps_log) if steps_log else 'none — no cleaning applied'}"),
        mo.hstack([
            mo.md(f"**BEFORE:**\n```\n{raw_re}\n```"),
            mo.md(f"**AFTER:**\n```\n{text_re}\n```"),
        ]),
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### *Why the ">" still there???*

    RE_QUOT = re.compile(r"^\s*>+.*$", re.MULTILINE) # multiline, search for all the lines, and also, max # of characters
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### 2b · Batch stats across all 10 emails
    """)
    return


@app.cell
def _(MOCK_EMAILS, clean_body, mo, pd):
    rows_batch = []
    for eb in MOCK_EMAILS:
        hb = {h["name"]: h["value"] for h in eb["payload"]["headers"]}
        rb = eb["body_text_content"]
        cb = clean_body(rb)
        rows_batch.append({"id":eb["id"],"subject":hb.get("Subject","")[:35],
                           "raw_chars":len(rb),"clean_chars":len(cb),
                           "reduction_%":round(100*(1-len(cb)/max(len(rb),1)),1),
                           "raw_words":len(rb.split()),"clean_words":len(cb.split())})
    mo.ui.table(pd.DataFrame(rows_batch), selection=None)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🦆 Section 3 — DuckDB Live SQL
    """)
    return


@app.cell
def _(MOCK_EMAILS, clean_body, duckdb, json, parsedate_to_datetime, pd):
    def _safe_date(s):
        try: return parsedate_to_datetime(s)
        except: return None

    _rows3 = []
    for _e3 in MOCK_EMAILS:
        _h3 = {h["name"]: h["value"] for h in _e3["payload"]["headers"]}
        _br = _e3["body_text_content"]
        _bc = clean_body(_br)
        _rows3.append({
            "id":_e3["id"],"thread_id":_e3["threadId"],
            "from_addr":_h3.get("From",""),"to_addr":_h3.get("To",""),
            "subject":_h3.get("Subject",""),"date_raw":_h3.get("Date",""),
            "date_parsed":_safe_date(_h3.get("Date","")),
            "label_ids":json.dumps(_e3["labelIds"]),
            "internal_date_ms":int(_e3["internalDate"]),
            "body_raw":_br,"body_clean":_bc,
            "word_count":len(_bc.split()),
            "has_attachment":"ATTACHMENT" in _e3["labelIds"],
        })
    _df_emails = pd.DataFrame(_rows3)
    con3 = duckdb.connect()
    con3.register("_df_emails", _df_emails)
    con3.execute("CREATE TABLE emails AS SELECT * FROM _df_emails")
    return (con3,)


@app.cell
def _(mo):
    sql_query = mo.ui.text_area(
        label="SQL (table: `emails`) — edit and the result updates live",
        value="SELECT id, from_addr, subject, word_count\nFROM emails\nORDER BY word_count DESC",
        rows=5,
    )
    sql_query
    return (sql_query,)


@app.cell
def _(con3, mo, sql_query):
    try:
        res_df = con3.execute(sql_query.value).df()
        mo.vstack([mo.stat(label="Rows", value=str(len(res_df))),
                   mo.ui.table(res_df, selection=None)])
    except Exception as _e_sql:
        mo.callout(mo.md(f"❌ SQL error: `{_e_sql}`"), kind="danger")
    return


@app.cell
def _(mo):
    mo.md("""
    ### 3b · DuckDB superpower — query a Pandas DataFrame as a SQL table

    ```python
    import duckdb, pandas as pd

    df = pd.DataFrame([{"name": "Alice", "score": 95}, {"name": "Bob", "score": 72}])

    # DuckDB sees the variable `df` directly — no CREATE TABLE needed
    result = duckdb.execute("SELECT * FROM df WHERE score > 80").df()
    ```

    The pipeline uses this in `02_clean_emails.py`:
    ```python
    con.execute("DELETE FROM emails WHERE id IN (SELECT id FROM df)")
    con.execute("INSERT INTO emails SELECT * FROM df")
    # `df` is a Pandas DataFrame — DuckDB reads it automatically
    ```
    """)
    return


@app.cell
def _(mo):
    col_pick = mo.ui.dropdown(
        options=["id","from_addr","to_addr","subject","date_raw","date_parsed",
                 "label_ids","word_count","has_attachment","body_raw","body_clean"],
        value="body_clean", label="Column",
    )
    row_pick = mo.ui.slider(0, 9, value=0, step=1, label="Row index")
    mo.hstack([col_pick, row_pick])
    return col_pick, row_pick


@app.cell
def _(col_pick, con3, mo, row_pick):
    _v = con3.execute(f"SELECT {col_pick.value} FROM emails LIMIT 1 OFFSET {int(row_pick.value)}").fetchone()[0] ## Fetch as tuple/list-like, get first element (value)
    _id = con3.execute(f"SELECT id FROM emails LIMIT 1 OFFSET {int(row_pick.value)}").fetchone()[0]
    mo.md(f"**Row {int(row_pick.value)}** (`{_id}`) → `{col_pick.value}`:\n```\n{_v}\n```\nType: `{type(_v).__name__}` · Length: `{len(str(_v or ''))}`")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🎯 Section 4 — Scoring Lab
    Every slider reshapes the entire priority queue **live**.

    ```
    score = sentiment_weight + topic_weight + min(age_days × age_multiplier, max_age_bonus) − (1 − confidence) × 5
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    #### Sentiment weights — how much each emotional tone contributes to urgency
    """)
    return


@app.cell
def _(mo):
    w_vn  = mo.ui.slider(0, 80, value=40, step=5,  label="very_negative")
    w_neg = mo.ui.slider(0, 60, value=25, step=5,  label="negative")
    w_neu = mo.ui.slider(0, 30, value=10, step=5,  label="neutral")
    w_pos = mo.ui.slider(0, 20, value=5,  step=5,  label="positive")
    w_vp  = mo.ui.slider(0, 10, value=0,  step=1,  label="very_positive")
    mo.hstack([w_vn, w_neg, w_neu, w_pos, w_vp])
    return w_neg, w_neu, w_pos, w_vn, w_vp


@app.cell
def _(mo):
    mo.md("""
    #### Topic weights — how much each email category contributes to urgency
    """)
    return


@app.cell
def _(mo):
    w_comp = mo.ui.slider(0, 60, value=30, step=5, label="Complaint")
    w_bug  = mo.ui.slider(0, 60, value=25, step=5, label="Bug")
    w_sale = mo.ui.slider(0, 60, value=20, step=5, label="Sales")
    w_feat = mo.ui.slider(0, 30, value=10, step=5, label="New Feature Request")
    w_oth  = mo.ui.slider(0, 20, value=5,  step=5, label="Other")
    mo.hstack([w_comp, w_bug, w_sale, w_feat, w_oth])
    return w_bug, w_comp, w_feat, w_oth, w_sale


@app.cell
def _(mo):
    mo.md("""
    #### Age bonus & tier thresholds
    """)
    return


@app.cell
def _(mo):
    age_mult = mo.ui.slider(0.0, 2.0, value=0.5, step=0.1, label="age_days_multiplier (+pts/day)")
    max_age  = mo.ui.slider(0,   50,  value=20,  step=5,   label="max_age_bonus (cap)")
    thr_crit = mo.ui.slider(0,   150, value=70,  step=5,   label="CRITICAL ≥")
    thr_high = mo.ui.slider(0,   100, value=45,  step=5,   label="HIGH ≥")
    thr_med  = mo.ui.slider(0,   70,  value=25,  step=5,   label="MEDIUM ≥")
    mo.hstack([age_mult, max_age, thr_crit, thr_high, thr_med])
    return age_mult, max_age, thr_crit, thr_high, thr_med


@app.cell
def _(
    MOCK_ANALYSIS,
    age_mult,
    datetime,
    max_age,
    mo,
    pd,
    thr_crit,
    thr_high,
    thr_med,
    timezone,
    w_bug,
    w_comp,
    w_feat,
    w_neg,
    w_neu,
    w_oth,
    w_pos,
    w_sale,
    w_vn,
    w_vp,
):

    SW = {"very_negative":int(w_vn.value),"negative":int(w_neg.value),
          "neutral":int(w_neu.value),"positive":int(w_pos.value),"very_positive":int(w_vp.value)}
    TW = {"Complaint":int(w_comp.value),"Bug":int(w_bug.value),
          "Sales":int(w_sale.value),"New Feature Request":int(w_feat.value),"Other":int(w_oth.value)}
    now_s4 = datetime.now(tz=timezone.utc)

    def _compute_score(row):
        sw  = SW.get(row["sentiment"], 10)
        tw  = TW.get(row["topic"], 5)
        dp  = datetime.fromisoformat(row["date_parsed"]).replace(tzinfo=timezone.utc)
        age = max(0.0, (now_s4 - dp).total_seconds() / 86400)
        ag  = min(age * float(age_mult.value), float(max_age.value))
        cp  = (1.0 - float(row["confidence"])) * 5
        tot = sw + tw + ag - cp
        if   tot >= float(thr_crit.value): tier = "🔴 CRITICAL"
        elif tot >= float(thr_high.value): tier = "🟠 HIGH"
        elif tot >= float(thr_med.value):  tier = "🟡 MEDIUM"
        else:                               tier = "🟢 LOW"
        return round(tot,1), tier, sw, tw, round(ag,1), round(cp,1)

    out_rows = []
    for a_s4 in MOCK_ANALYSIS:
        sc, tier, sw, tw, ag, cp = _compute_score(a_s4)
        out_rows.append({"subject":a_s4["subject"][:38],"sentiment":a_s4["sentiment"],
                         "topic":a_s4["topic"],"sent_w":sw,"topic_w":tw,
                         "age_bonus":ag,"conf_pen":cp,"SCORE":sc,"TIER":tier})

    df_scored = pd.DataFrame(out_rows).sort_values("SCORE", ascending=False).reset_index(drop=True)
    df_scored.index += 1
    tier_counts = df_scored["TIER"].value_counts()

    mo.vstack([
        mo.hstack([mo.stat(label=str(k), value=str(v)) for k, v in tier_counts.items()]),
        mo.ui.table(df_scored, selection=None),
    ])
    return SW, TW, now_s4


@app.cell
def _(mo):
    mo.md("""
    ### 4b · Single email score breakdown

    Pick an email to see every component of its score — move the sliders above and watch this update.
    """)
    return


@app.cell
def _(MOCK_ANALYSIS, mo):
    sel_score_email = mo.ui.dropdown(
        options=[a["id"] for a in MOCK_ANALYSIS],
        value="msg_001", label="Email to dissect",
    )
    sel_score_email
    return (sel_score_email,)


@app.cell
def _(
    MOCK_ANALYSIS,
    SW,
    TW,
    age_mult,
    datetime,
    max_age,
    mo,
    now_s4,
    sel_score_email,
    thr_crit,
    thr_high,
    thr_med,
    timezone,
):

    row_bd = next(a for a in MOCK_ANALYSIS if a["id"] == sel_score_email.value)
    sw_bd  = SW.get(row_bd["sentiment"], 10)
    tw_bd  = TW.get(row_bd["topic"], 5)
    dp_bd  = datetime.fromisoformat(row_bd["date_parsed"]).replace(tzinfo=timezone.utc)
    age_bd = max(0.0, (now_s4 - dp_bd).total_seconds() / 86400)
    ag_bd  = min(age_bd * float(age_mult.value), float(max_age.value))
    cp_bd  = (1.0 - float(row_bd["confidence"])) * 5
    tot_bd = sw_bd + tw_bd + ag_bd - cp_bd

    if   tot_bd >= float(thr_crit.value): tier_bd = "🔴 CRITICAL"
    elif tot_bd >= float(thr_high.value): tier_bd = "🟠 HIGH"
    elif tot_bd >= float(thr_med.value):  tier_bd = "🟡 MEDIUM"
    else:                                  tier_bd = "🟢 LOW"

    mo.md(f"""
    **{row_bd['id']}** — *{row_bd['subject']}*

    | Component | Value | Contribution |
    |-----------|-------|-------------|
    | Sentiment | `{row_bd['sentiment']}` | **+{sw_bd}** |
    | Topic | `{row_bd['topic']}` | **+{tw_bd}** |
    | Age | {age_bd:.1f} days × {float(age_mult.value)} (cap {int(max_age.value)}) | **+{ag_bd:.1f}** |
    | Confidence penalty | (1 − {row_bd['confidence']:.2f}) × 5 | **−{cp_bd:.1f}** |
    | | **TOTAL** | **{tot_bd:.1f}** |

    ### → {tier_bd}
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### 4c · Scenario presets — try these slider combinations

    | Scenario | What to do | Expected result |
    |----------|-----------|-----------------|
    | **Angry customer first** | Raise `very_negative` → 70, `Complaint` → 50 | msg_001/002 dominate |
    | **Bugs are P0** | Raise `Bug` → 55, above `Complaint` | msg_007/008 rise |
    | **Revenue focus** | Raise `Sales` → 45 | msg_009/003 climb |
    | **Ignore age** | Set `age_days_multiplier` → 0 | Older emails lose bonus |
    | **Everything CRITICAL** | Drop `CRITICAL` threshold → 10 | All emails turn red |
    | **Nothing CRITICAL** | Raise `CRITICAL` threshold → 120 | Queue mostly LOW |
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🤖 Section 5 — LLM Prompt Builder
    """)
    return


@app.cell
def _(MOCK_EMAILS, mo):
    sel_llm_email = mo.ui.dropdown(
        options=[e["id"] for e in MOCK_EMAILS],
        value="msg_001", label="Email to analyse",
    )
    body_cap  = mo.ui.slider(100, 3000, value=3000, step=100, label="body char cap sent to LLM")
    show_sys  = mo.ui.switch(label="Show system prompt", value=True)
    show_user = mo.ui.switch(label="Show user prompt",   value=True)
    mo.hstack([sel_llm_email, body_cap, show_sys, show_user])
    return body_cap, sel_llm_email, show_sys, show_user


@app.cell
def _(
    MOCK_EMAILS,
    body_cap,
    clean_body,
    mo,
    sel_llm_email,
    show_sys,
    show_user,
):
    TOPICS_LLM = ["Complaint","New Feature Request","Bug","Sales","Other"]
    SYS_PROMPT = f"""You are an expert email analyst for a customer-support team.
    Your task is to analyse a single customer email and return a structured JSON object.

    Fields to extract:
    1. "sentiment": "very_negative" | "negative" | "neutral" | "positive" | "very_positive"
    2. "sentiment_score": float 0.0 (very positive) to 1.0 (very negative)
    3. "first_sent_date": ISO 8601 (YYYY-MM-DD) or null
    4. "topic": one of {TOPICS_LLM}
    5. "summary": single sentence, max 25 words
    6. "confidence": float 0.0–1.0

    Return ONLY a valid JSON object. No extra text, no markdown.

    Example:
    {{
      "sentiment": "negative",
      "sentiment_score": 0.75,
      "first_sent_date": "2024-03-15",
      "topic": "Complaint",
      "summary": "Customer upset about delayed delivery and wants a refund.",
      "confidence": 0.92
    }}"""

    em_llm = next(e for e in MOCK_EMAILS if e["id"] == sel_llm_email.value)
    h_llm  = {h["name"]: h["value"] for h in em_llm["payload"]["headers"]}
    body_llm = clean_body(em_llm["body_text_content"])[:int(body_cap.value)]
    USER_PROMPT = f"Subject: {h_llm.get('Subject','')}\nFrom: {h_llm.get('From','')}\nDate: {h_llm.get('Date','')}\n\nBody:\n{body_llm}"

    parts_llm = []
    if show_sys.value:
        parts_llm.append(mo.md(f"**System prompt** ({len(SYS_PROMPT.split())} words):\n```\n{SYS_PROMPT}\n```"))
    if show_user.value:
        parts_llm.append(mo.md(f"**User prompt** ({len(USER_PROMPT.split())} words):\n```\n{USER_PROMPT}\n```"))
    parts_llm.append(mo.hstack([
        mo.stat(label="System words",   value=str(len(SYS_PROMPT.split()))),
        mo.stat(label="User words",     value=str(len(USER_PROMPT.split()))),
        mo.stat(label="Total est.",     value=str(len(SYS_PROMPT.split())+len(USER_PROMPT.split()))),
        mo.stat(label="Body chars cap", value=str(len(body_llm))),
    ]))
    mo.vstack(parts_llm)
    return SYS_PROMPT, USER_PROMPT


@app.cell
def _(mo):
    llm_temp   = mo.ui.slider(0.0, 2.0, value=0.1,  step=0.05, label="temperature")
    llm_tokens = mo.ui.slider(50,  600, value=300,  step=50,   label="num_predict")
    llm_model  = mo.ui.dropdown(options=["gemma3:1b","llama3.1","mistral","phi3"], value="gemma3:1b", label="model")
    btn_llm    = mo.ui.run_button(label="▶ Call Ollama (requires ollama serve)")
    mo.hstack([llm_temp, llm_tokens, llm_model, btn_llm])
    return btn_llm, llm_model, llm_temp, llm_tokens


@app.cell
def _(
    SYS_PROMPT,
    USER_PROMPT,
    btn_llm,
    json,
    llm_model,
    llm_temp,
    llm_tokens,
    mo,
):
    if btn_llm.value:
        try:
            import ollama as _ol
            _raw = _ol.Client(host="http://localhost:11434").chat(
                model=llm_model.value,
                messages=[{"role":"system","content":SYS_PROMPT},{"role":"user","content":USER_PROMPT}],
                options={"temperature":llm_temp.value,"num_predict":int(llm_tokens.value)},
            )["message"]["content"]
            _s = _raw.strip()
            if _s.startswith("```"):
                _s = _s.split("```")[1]
                if _s.startswith("json"): _s = _s[4:]
            _s = _s.strip()
            try:
                llm_retrieved_payload = json.loads(_s); _ok = True
            except:
                llm_retrieved_payload = {}; _ok = False
            mo.vstack([
                mo.hstack([mo.stat(label="JSON valid",value="✅" if _ok else "❌"),
                           mo.stat(label="Chars",value=str(len(_raw))),
                           mo.stat(label="Fields",value=str(len(_p)))]),
                mo.md(f"**Raw:**\n```\n{_raw}\n```"),
                mo.md(f"**Parsed:**\n```json\n{json.dumps(_p,indent=2)}\n```") if _ok
                else mo.callout(mo.md("⚠️ Not valid JSON — try lower temperature"), kind="warn"),
            ])
        except Exception as _e:
            mo.callout(mo.md(f"❌ Ollama: `{_e}`\n\nRun: `ollama serve`"), kind="danger")
    else:
        mo.md("> Press ▶ to call Ollama. Adjust temperature and see the effect.")
    return


@app.cell
def _():
    {
      "sentiment": "negative",
      "sentiment_score": 0.95,
      "first_sent_date": None,
      "topic": "Complaint",
      "summary": "Customer is expressing severe dissatisfaction with the service, citing repeated requests for a refund and blaming the support team for failing to resolve the issue correctly.",
      "confidence": 0.98
    }
    return


@app.cell
def _(mo):
    mo.md("""
    ### 5b · Output validation sandbox — paste any LLM response and see how it's sanitised
    """)
    return


@app.cell
def _(mo):
    llm_sim = mo.ui.text_area(
        label="Paste simulated LLM output — try 'furious' as sentiment to see the fallback",
        value='{\n  "sentiment": "very_negative",\n  "sentiment_score": 0.95,\n  "first_sent_date": "2026-02-20",\n  "topic": "Complaint",\n  "summary": "Customer demands refund after repeated requests.",\n  "confidence": 0.92\n}',
        rows=10,
    )
    llm_sim
    return (llm_sim,)


@app.cell
def _(json, llm_sim, mo, pd):
    VALID_SENTIMENTS = {"very_negative","negative","neutral","positive","very_positive"}
    VALID_TOPICS     = {"Complaint","New Feature Request","Bug","Sales","Other"}
    try:
        _r = llm_sim.value.strip()
        if _r.startswith("```"):
            _r = _r.split("```")[1]
            if _r.startswith("json"): _r = _r[4:]
        _p = json.loads(_r.strip())
        _s_in  = _p.get("sentiment","<missing>"); _s_out = _s_in if _s_in in VALID_SENTIMENTS else "neutral"
        _t_in  = _p.get("topic","<missing>");     _t_out = _t_in if _t_in in VALID_TOPICS else "Other"
        _rows_v = [
            ("sentiment",       str(_s_in),  str(_s_out),  "✅" if _s_in==_s_out else f"⚠️ fallback to '{_s_out}'"),
            ("topic",           str(_t_in),  str(_t_out),  "✅" if _t_in==_t_out else f"⚠️ fallback to '{_t_out}'"),
            ("sentiment_score", str(_p.get("sentiment_score","?")), str(float(_p.get("sentiment_score",0.5))), "✅"),
            ("confidence",      str(_p.get("confidence","?")),      str(float(_p.get("confidence",0.5))), "✅"),
            ("summary",         str(_p.get("summary",""))[:60],     str(_p.get("summary",""))[:60], "✅"),
            ("first_sent_date", str(_p.get("first_sent_date","null")), str(_p.get("first_sent_date","null")), "✅"),
        ]
        mo.ui.table(pd.DataFrame(_rows_v, columns=["field","raw_value","sanitised","status"]), selection=None)
    except Exception as _ev:
        mo.callout(mo.md(f"❌ JSON parse failed: `{_ev}` → `get_json_completion` returns `{{}}`"), kind="danger")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 📋 Section 6 — Python `logging`: why it beats `print()`

    > **One-line pitch:** `print()` is a flashlight. `logging` is an electricity grid.

    | Feature | `print()` | `logging` |
    |---------|-----------|-----------|
    | Severity levels | ❌ all the same | ✅ DEBUG / INFO / WARNING / ERROR / CRITICAL |
    | Turn off without deleting code | ❌ must comment out | ✅ `setLevel(WARNING)` |
    | Timestamps | ❌ manual | ✅ automatic via formatter |
    | Source file + line number | ❌ manual | ✅ `%(filename)s:%(lineno)d` |
    | Write to file AND console | ❌ redirect stdout | ✅ multiple handlers |
    | Per-module control | ❌ impossible | ✅ `logging.getLogger(__name__)` |
    | Production-safe | ❌ leaks debug info | ✅ filter by level per environment |
    | Cost of suppressed messages | ❌ always formats | ✅ `%s` args skipped if suppressed |
    """)
    return


@app.cell
def _(mo):
    level_demo = mo.ui.radio(
        options={
            "DEBUG — internal state, only for developers":   "DEBUG",
            "INFO — normal operational milestones":          "INFO",
            "WARNING — unexpected but recoverable":          "WARNING",
            "ERROR — something failed, pipeline continues":  "ERROR",
            "CRITICAL — pipeline cannot continue":           "CRITICAL",
        },
        value="DEBUG — internal state, only for developers",
        label="Select a level to see example messages from this pipeline",
    )
    level_demo
    return (level_demo,)


@app.cell
def _(level_demo, mo):
    LEVEL_EXAMPLES = {
        "DEBUG":    "Processing email msg_003 — body_clean=142 chars, word_count=28\nRegex _RE_QUOTED matched 2 lines\nDuckDB INSERT completed in 0.003s",
        "INFO":     "Step 1 complete — 10 emails fetched from API\nStep 2 complete — 10 emails written to emails.db\nOllama @ localhost:11434  model=llama3.2",
        "WARNING":  "msg_007 body_clean is very short (8 words) — classification may be unreliable\nLLM response for msg_003 missing 'confidence' — using default 0.5\nRetry 1/3 for msg_008 — LLM timeout after 30s",
        "ERROR":    "JSON parse failed for msg_005: JSONDecodeError — using empty dict fallback\nDuckDB write failed: disk full — pipeline continuing without persisting batch\nHTTP 503 from mock API — aborting fetch",
        "CRITICAL": "Cannot connect to DuckDB — emails.db locked by another process\nOllama unreachable after 3 retries — pipeline aborted\ndata/raw/ does not exist and mkdir failed — check permissions",
    }
    lvl = level_demo.value.split(" — ")[0]
    mo.vstack([
        mo.md(f"### When to use `{lvl}`\n> {level_demo.value}"),
        mo.md(f"**Example messages from this pipeline:**\n```\n{LEVEL_EXAMPLES[lvl]}\n```"),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ### 6b · Live logger — configure format and level, see exactly what's shown
    """)
    return


@app.cell
def _(mo):
    log_level_sel = mo.ui.dropdown(
        options=["DEBUG","INFO","WARNING","ERROR","CRITICAL"],
        value="DEBUG", label="Root logger level (messages below this are suppressed)",
    )
    log_format_sel = mo.ui.radio(
        options={
            "Simple":        "%(levelname)s — %(message)s",
            "With time":     "%(asctime)s  %(levelname)-8s  %(message)s",
            "With module":   "%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s",
            "JSON-style":    '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
        },
        value="With time",
        label="Log format",
    )
    mo.hstack([log_level_sel, log_format_sel])
    return log_format_sel, log_level_sel


@app.cell
def _(io, log_format_sel, log_level_sel, logging, mo):
    _buf = io.StringIO()
    _h = logging.StreamHandler(_buf)
    _h.setFormatter(logging.Formatter(log_format_sel.value, datefmt="%H:%M:%S"))
    _lg = logging.getLogger("pipeline.live_demo")
    _lg.handlers.clear()
    _lg.addHandler(_h)
    _lg.setLevel(getattr(logging, log_level_sel.value))
    _lg.propagate = False

    _lg.debug("Connecting to mock API at http://127.0.0.1:8000/emails")
    _lg.debug("HTTP GET /emails → 200 OK, 10 emails received")
    _lg.info("Step 1 complete — 10 emails saved to data/raw/")
    _lg.info("Step 2 starting — loading .meta.json files")
    _lg.warning("msg_006 body_clean very short (8 words) — may be unreliable")
    _lg.info("Step 2 complete — emails.db updated, 10 rows")
    _lg.debug("Calling Ollama for msg_001: temperature=0.1, num_predict=300")
    _lg.error("JSON parse failed for msg_999 — using fallback {}")
    _lg.info("Step 3 complete — 9 analysed, 1 error")
    _lg.critical("DiskFull: cannot write to prioritised.db")

    _out = _buf.getvalue()
    mo.md(f"**Output** (level={log_level_sel.value}):\n```\n{_out if _out else '(all messages suppressed at this level)'}\n```")
    return


@app.cell
def _(mo):
    mo.md("""
    ### 6c · How to set up logging in a real module

    ```python
    import logging

    # Named logger — one per module. __name__ = module name automatically.
    logger = logging.getLogger(__name__)
    # This lets you silence just THIS module in production:
    # logging.getLogger("src.03_analyse_emails").setLevel(logging.ERROR)


    def setup_logging(level: str = "INFO") -> None:
        "\""Call once at the entry point (main). Never call in library code."\""
        import os
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format="%(asctime)s  %(name)-25s  %(levelname)-8s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.StreamHandler(),                   # → console
                logging.FileHandler("logs/pipeline.log"), # → file
            ],
        )


    # Usage: use %s NOT f-strings in logger calls
    logger.debug("Processing %s — %d chars", email_id, len(body))
    # ↑ String is NOT formatted until/unless this message will be shown.
    # f-string would format it even when DEBUG is suppressed — wasted CPU.

    logger.info("Step 3 complete — %d analysed, %d errors", ok, err)
    logger.warning("Low confidence (%.2f) for %s", conf, email_id)
    logger.error("JSON parse failed for %s: %s", email_id, exc)
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### 6d · Time-based log rotation — never let logs fill your disk
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ```python
    from logging.handlers import TimedRotatingFileHandler
    import logging, os

    os.makedirs("logs", exist_ok=True)

    handler = TimedRotatingFileHandler(
        filename="logs/pipeline.log",
        when="midnight",   # rotate at midnight → one file per day
        # other options: "H" hourly | "W0" weekly Monday | "S" every second (for tests)
        interval=1,
        backupCount=7,     # keep last 7 rotated files = 1 week of history
        encoding="utf-8",
    )
    # After rotation, old files are renamed automatically:
    #   logs/pipeline.log             ← current (today)
    #   logs/pipeline.log.2026-02-21  ← yesterday
    #   logs/pipeline.log.2026-02-20  ← 2 days ago
    #   ...
    #   logs/pipeline.log.2026-02-15  ← oldest kept (7 days ago)
    #   (anything older is deleted automatically)

    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(name)-25s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logging.getLogger().addHandler(handler)
    ```

    **Why not `logging.basicConfig(filename=...)`?**
    That writes to a single file forever — it grows until disk is full.
    `TimedRotatingFileHandler` auto-deletes files older than `backupCount` days.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### 6e · Simulated rotating log — adjust volume and minimum level
    """)
    return


@app.cell
def _(mo):
    num_log_msgs  = mo.ui.slider(1, 50, value=15, step=1, label="Messages to generate")
    log_min_level = mo.ui.dropdown(options=["DEBUG","INFO","WARNING","ERROR"], value="INFO", label="Min level to display")
    mo.hstack([num_log_msgs, log_min_level])
    return log_min_level, num_log_msgs


@app.cell
def _(io, log_min_level, logging, mo, num_log_msgs, random):
    _MSGS = [
        (logging.DEBUG,    "Processing email {id} — body_clean={chars} chars"),
        (logging.DEBUG,    "Regex _RE_QUOTED matched {n} lines in {id}"),
        (logging.INFO,     "Step 1 complete — {n} emails fetched"),
        (logging.INFO,     "DuckDB INSERT — {n} rows written in {ms}ms"),
        (logging.WARNING,  "Low confidence ({conf:.2f}) for {id}"),
        (logging.WARNING,  "Retrying LLM call for {id} (attempt {n}/3)"),
        (logging.ERROR,    "JSON parse failed for {id}: JSONDecodeError"),
        (logging.CRITICAL, "Cannot write to prioritised.db — disk full"),
    ]
    _buf2 = io.StringIO()
    _h2 = logging.StreamHandler(_buf2)
    _h2.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S"))
    _h2.setLevel(getattr(logging, log_min_level.value))
    _lg2 = logging.getLogger("pipeline.sim")
    _lg2.handlers.clear(); _lg2.addHandler(_h2)
    _lg2.setLevel(logging.DEBUG); _lg2.propagate = False
    _ids = [f"msg_{i:03d}" for i in range(1,11)]
    for _i in range(int(num_log_msgs.value)):
        _lvl, _tmpl = random.choice(_MSGS)
        _lg2.log(_lvl, _tmpl.format(
            id=random.choice(_ids), n=random.randint(1,20),
            chars=random.randint(50,400), ms=random.randint(1,50),
            conf=random.uniform(0.3,0.99),
        ))
    _out2 = _buf2.getvalue()
    mo.md(f"**Simulated log** ({int(num_log_msgs.value)} events, showing ≥ {log_min_level.value}):\n```\n{_out2 if _out2 else '(all suppressed)'}\n```")
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🐛 Section 7 — Debugging in Marimo

    Marimo's reactive DAG changes how you debug. These 6 techniques cover 95% of cases.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 1 · `mo.md()` as an inline inspector

    Add a temporary cell anywhere. No `print`, no breakpoint. Delete when done.

    ```python
    # Temporary debug cell — inspect intermediate DataFrame
    mo.md(f"\""
    **df at this point:**
    - Shape: `{df.shape}`
    - Columns: `{list(df.columns)}`
    - Nulls: `{df.isnull().sum().to_dict()}`

    Sample:
    ```json
    {df.head(3).to_json(orient='records', indent=2)}
    ```
    "\"")
    ```
    """)
    return


@app.cell
def _(MOCK_EMAILS, clean_body, mo, pd):
    # Live example of Technique 1 — shows the intermediate DataFrame after cleaning
    _rows_dbg = []
    for _e_dbg in MOCK_EMAILS[:4]:
        _h_dbg = {h["name"]: h["value"] for h in _e_dbg["payload"]["headers"]}
        _bc_dbg = clean_body(_e_dbg["body_text_content"])
        _rows_dbg.append({"id":_e_dbg["id"],"subject":_h_dbg.get("Subject","")[:35],
                          "body_clean":_bc_dbg[:55]+"…","word_count":len(_bc_dbg.split())})
    df_dbg = pd.DataFrame(_rows_dbg)

    mo.vstack([
        mo.md(f"**🔍 Inspecting `df` after Step 2 cleaning (first 4 rows):**\n"
              f"Shape: `{df_dbg.shape}` · Columns: `{list(df_dbg.columns)}` · "
              f"Nulls: `{df_dbg.isnull().sum().to_dict()}`"),
        mo.ui.table(df_dbg, selection=None),
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 2 · `mo.ui.table()` for DataFrames

    Always use `mo.ui.table(df)` instead of `print(df)` — renders as a sortable table.

    ```python
    # ❌ Don't
    print(df.head(10))

    # ✅ Do
    mo.ui.table(df.head(10), selection=None)

    # ✅ With stats
    mo.vstack([
        mo.hstack([mo.stat(label=col, value=str(df[col].dtype)) for col in df.columns]),
        mo.ui.table(df, selection=None),
    ])
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 3 · `mo.stop()` — conditional halt with a message

    `mo.stop(condition, message)` stops cell execution (and all downstream cells
    that depend on its outputs) when the condition is True.

    ```python
    # Stop the pipeline gracefully if emails list is empty
    mo.stop(
        len(emails) == 0,
        mo.callout(mo.md("⛔ No emails — is the mock API running?"), kind="danger")
    )
    # Code below this line only runs if emails is non-empty

    # Stop if database file is missing
    mo.stop(
        not DB_EMAILS_PATH.exists(),
        mo.callout(mo.md("⛔ emails.db not found — run step 2 first"), kind="warn")
    )
    ```

    Better than `sys.exit()` in notebooks: only stops **this branch** of the DAG.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    **Live `mo.stop()` demo:** adjust the two sliders to trigger/clear the stop.
    """)
    return


@app.cell
def _(mo):
    stop_threshold = mo.ui.slider(0, 20, value=5,  step=1, label="Minimum emails required")
    fake_count     = mo.ui.slider(0, 15, value=10, step=1, label="Simulated emails returned by API")
    mo.hstack([fake_count, stop_threshold])
    return fake_count, stop_threshold


@app.cell
def _(fake_count, mo, stop_threshold):
    mo.stop(
        int(fake_count.value) < int(stop_threshold.value),
        mo.callout(
            mo.md(f"⛔ Only **{int(fake_count.value)}** emails returned — need at least **{int(stop_threshold.value)}**. "
                  "Increase the simulated count or lower the threshold."),
            kind="info"
        )
    )
    mo.callout(
        mo.md(f"✅ **{int(fake_count.value)}** emails received — above threshold of {int(stop_threshold.value)}. Pipeline continues."),
        kind="success"
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 4 · `mo.callout()` — structured status messages

    ```python
    mo.callout(mo.md("ℹ️ Using analysis.db fallback — prioritised.db not found"), kind="info")
    mo.callout(mo.md("⚠️ 2 emails had empty body — skipped LLM"),                kind="warn")
    mo.callout(mo.md("❌ DuckDB locked — check for other processes"),             kind="danger")
    mo.callout(mo.md("✅ Pipeline complete — 10 prioritised, 0 errors"),          kind="success")
    ```
    """)
    return


@app.cell
def _(mo):
    callout_kind = mo.ui.radio(
        options=["info","warn","danger","success"],
        value="info",
        label="Callout kind",
    )
    callout_kind
    return (callout_kind,)


@app.cell
def _(callout_kind, mo):
    CALLOUT_MSGS = {
        "info":    "ℹ️ Using `analysis.db` fallback — `prioritised.db` not found yet",
        "warn":    "⚠️ 2 emails had empty `body_clean` — skipped LLM analysis",
        "danger":  "❌ `DuckDB` connection failed — `emails.db` may be locked by another process",
        "success": "✅ Pipeline complete — 10 emails prioritised, 0 errors",
    }
    mo.callout(mo.md(CALLOUT_MSGS[callout_kind.value]), kind=callout_kind.value)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 5 · The DAG is your debugger — trace execution order

    The single most common Marimo bug: a variable doesn't update when a widget changes.
    **Root cause:** the cell that produces it doesn't `return` it, so the DAG has no edge.

    ```python
    # ❌ WRONG — out_branch exists locally but the next cell can't see it
    @app.cell
    def _(git, mo):
        out_branch = git("git push origin feature/add-docs")
        mo.md(f"Pushed: {out_branch}")
        return        # ← nothing exposed to the DAG!

    # ✅ CORRECT — next cell can declare out_branch as a dependency
    @app.cell
    def _(git, mo):
        out_branch = git("git push origin feature/add-docs")
        mo.md(f"Pushed: {out_branch}")
        return (out_branch,)    # ← now in the DAG

    # ✅ CORRECT — downstream cell forces execution order
    @app.cell
    def _(mo, out_branch):   # ← receiving out_branch guarantees this runs after
        _ = out_branch       # explicit "I depend on this" even if value isn't used
        ...
    ```

    **Checklist when something doesn't update:**
    1. Does the producing cell `return` the variable?
    2. Does the consuming cell list it as a parameter?
    3. Is the variable name free of `_` prefix? (`_x` is local; `x` crosses cells)
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ### Technique 6 · `mo.ui.run_button()` — gate expensive operations

    Without a run button, every widget change re-runs dependent cells.
    For LLM calls, DB writes, or HTTP requests: use `run_button`.

    ```python
    # Cell A — define button
    btn = mo.ui.run_button(label="▶ Run LLM analysis")
    btn
    return btn,

    # Cell B — gated execution
    def _(mo, btn, df):
        if not btn.value:
            return mo.md("> Press ▶ to start.")
        # ── only runs after button press ──
        results = [analyse_email(row) for _, row in df.iterrows()]
        return mo.ui.table(pd.DataFrame(results))
    ```

    `btn.value` is `True` for exactly **one reactive cycle** after pressing, then resets.
    This prevents accidental re-runs when other widgets change.
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ---
    # 🔁 Section 8 — Full Pipeline Trace
    One email, all four steps, end to end. Weights come from Section 4 sliders.
    """)
    return


@app.cell
def _(MOCK_EMAILS, mo):
    sel_trace = mo.ui.dropdown(
        options=[e["id"] for e in MOCK_EMAILS],
        value="msg_001",
        label="Trace this email through all 4 steps",
    )
    sel_trace
    return (sel_trace,)


@app.cell
def _(
    MOCK_ANALYSIS,
    MOCK_EMAILS,
    SW,
    TW,
    age_mult,
    clean_body,
    datetime,
    json,
    max_age,
    mo,
    now_s4,
    sel_trace,
    thr_crit,
    thr_high,
    thr_med,
    timezone,
):

    et  = next(e for e in MOCK_EMAILS   if e["id"] == sel_trace.value)
    at  = next(a for a in MOCK_ANALYSIS if a["id"] == sel_trace.value)
    ht  = {h["name"]: h["value"] for h in et["payload"]["headers"]}

    # Step 1
    meta_t = {"id":et["id"],"thread_id":et["threadId"],"from":ht.get("From",""),
               "subject":ht.get("Subject",""),"date":ht.get("Date",""),
               "label_ids":et["labelIds"],"internal_date_ms":int(et["internalDate"])}
    body_raw_t   = et["body_text_content"]
    body_clean_t = clean_body(body_raw_t)

    # Step 4 scoring
    sw_t  = SW.get(at["sentiment"], 10)
    tw_t  = TW.get(at["topic"], 5)
    dp_t  = datetime.fromisoformat(at["date_parsed"]).replace(tzinfo=timezone.utc)
    age_t = max(0.0, (now_s4 - dp_t).total_seconds() / 86400)
    ag_t  = min(age_t * float(age_mult.value), float(max_age.value))
    cp_t  = (1.0 - float(at["confidence"])) * 5
    score_t = sw_t + tw_t + ag_t - cp_t
    if   score_t >= float(thr_crit.value): tier_t = "🔴 CRITICAL"
    elif score_t >= float(thr_high.value): tier_t = "🟠 HIGH"
    elif score_t >= float(thr_med.value):  tier_t = "🟡 MEDIUM"
    else:                                   tier_t = "🟢 LOW"

    mo.vstack([
        mo.md("### ① Step 1 — `01_fetch_emails.py` → writes two files to `data/raw/`"),
        mo.hstack([
            mo.md(f"**`{et['id']}.meta.json`**\n```json\n{json.dumps(meta_t,indent=2)}\n```"),
            mo.md(f"**`{et['id']}.txt`**\n```\n{body_raw_t}\n```"),
        ]),
        mo.md("### ② Step 2 — `02_clean_emails.py` → writes row to DuckDB `emails`"),
        mo.md(f"```\nbody_raw   : {body_raw_t[:80]}…\nbody_clean : {body_clean_t[:80]}…\nword_count : {len(body_clean_t.split())}\n```"),
        mo.md("### ③ Step 3 — `03_analyse_emails.py` → LLM output → DuckDB `email_analysis`"),
        mo.md(f"```json\n{json.dumps({k:v for k,v in at.items() if k not in ['from_addr','subject','date_parsed']},indent=2)}\n```"),
        mo.md("### ④ Step 4 — `04_prioritise_emails.py` → score → DuckDB `prioritised_emails`"),
        mo.md(f"""
    | sent_w | topic_w | age_bonus | conf_penalty | **SCORE** | **TIER** |
    |--------|---------|-----------|--------------|-----------|----------|
    | {sw_t} | {tw_t} | {ag_t:.1f} | {cp_t:.1f} | **{score_t:.1f}** | {tier_t} |

    *(Change the weight sliders in Section 4 and the score here updates too)*
    """),
    ])
    return


if __name__ == "__main__":
    app.run()
