# ─────────────────────────────────────────────
#  ROTA IAF v2 — novo frontend (testes)
#  Adicionar logo abaixo da rota @app.get("/iaf")
# ─────────────────────────────────────────────

@app.get("/iaf-v2", response_class=HTMLResponse)
def iaf_v2():
    for p in ["index_v2.html", "/app/index_v2.html"]:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>IAF v2 — arquivo não encontrado</h1>")
