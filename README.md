## Lucian API

Backend do **Lucian**, o agente de IA da [SynastrIA Networks](https://synastria.dev) — servidor Flask que expõe autenticação, chat com agente, ferramentas (tools), memória persistente, subagentes, sandbox de código, geração de leads e mais.

> Este repositório está sendo disponibilizado como open source após o encerramento do ciclo do Lucian. O desenvolvimento atual da SynastrIA seguiu para o **Uirapuru**, com um backend novo, e modelo construído do zero.

## Stack

- **Flask** + `flask-cors`
- **PostgreSQL** (`psycopg2`)
- **JWT** para autenticação (`PyJWT`)
- **APScheduler** para tarefas agendadas
- **Stripe** para cobrança
- **E2B** (`e2b-code-interpreter`) para sandboxes de execução de código
- **Groq**, **Gemini** (via SDK OpenAI-compatible) e **Maritaca AI** como provedores de modelo
- **Playwright** para screenshots/automação de navegador

## Funcionalidades principais

- **Auth**: registro/login com JWT, OAuth com GitHub
- **Chat com agente**: roteamento entre múltiplos modelos/provedores (Groq, Gemini, Maritaca), com parsing de tool calls "pseudo" pra modelos que não suportam tool calling nativo
- **Sistema de créditos**: custo por ferramenta usada, limites diários e mensais por plano (free/paid)
- **Memória persistente do usuário** (`save_memory`, endpoints `/memory`)
- **Subagentes**: criação e delegação de tarefas (`create_subagent`, `delegate_to_subagents`)
- **Skills**: import, listagem e execução (`list_skills`, `run_skill`)
- **Sandbox de código** via E2B (`run_sandbox`)
- **Publicação de sites** gerados pelo agente (`create_site`, `/publish`, `/site/<slug>`)
- **Integração com GitHub**: OAuth, push de código, correção automática de vulnerabilidades (`github_fix_vulnerabilities`)
- **Agendamento de tarefas** (`schedule_task`, `/scheduler/tasks`)
- **Aprovações pendentes** pra ações sensíveis do agente (`request_user_approval`, `/approvals`)
- **TTS/STT e modo voz** (`/tts`, `/stt`, `/voice`)
- **Histórico e compartilhamento de conversas** (`/history`, `/share/<conversation_id>`)
- **Geração e qualificação de leads**: descoberta, análise, scoring e perfis de ICP (`/leads/*`)
- **Retry inteligente** com backoff exponencial + jitter pra chamadas a APIs externas
- **Upload de imagens** pro Vercel Blob

## Variáveis de ambiente

| Variável | Descrição |
|---|---|
| `DATABASE_URL` | Conexão com o PostgreSQL |
| `JWT_SECRET` | Segredo pra assinatura dos tokens JWT (obrigatório) |
| `MODEL_API_URL` | URL do provedor de modelo padrão |
| `GROQ_API_KEY` | Chave da API da Groq |
| `GEMINI_API_KEY` | Chave da API do Gemini |
| `MARITACA_API_KEY` | Chave da API da Maritaca AI |
| `STRIPE_SECRET_KEY` | Chave secreta do Stripe |
| `STRIPE_WEBHOOK_SECRET` | Segredo do webhook do Stripe |
| `STRIPE_PRICE_ID` | ID do preço/plano no Stripe |
| `FRONTEND_URL` | URL do frontend (usada em redirects) |
| `BLOB_READ_WRITE_TOKEN` | Token do Vercel Blob Storage |
| `SERPAPI_KEY` | Chave da SerpAPI (busca web) |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | Credenciais do OAuth App do GitHub |
| `REGISTRATION_KEY` | Chave opcional pra restringir novos registros |
| `PORT` | Porta do servidor (padrão 5000) |

Crie um arquivo `.env` na raiz com essas variáveis antes de rodar o projeto — **nenhuma chave real está incluída neste repositório**.

## Rodando localmente

```bash
pip install -r requirements.txt
python lucian.py
```

O servidor cria as tabelas necessárias automaticamente na primeira execução (`init_db`).

## Estrutura do banco

O backend gerencia, entre outras, as tabelas: `users`, `conversations`, `messages`, `user_memories`, `subagents`, `scheduled_tasks`, `pending_approvals`, `smart_followups`, `published_sites`, `shared_conversations`, `sandbox_logs`, `user_tool_usage`, `tool_chains`, `leads`, `lead_searches`, `lead_analyses` e `lead_icp_profiles`.

## Aviso

Este código representa uma fase específica do desenvolvimento da SynastrIA (era do Lucian) e é publicado como referência/estudo. O produto ativo da empresa é o **Uirapuru**, com backend reconstruído separadamente.

## Licença

MIT — sinta-se à vontade pra estudar, adaptar e reutilizar.
