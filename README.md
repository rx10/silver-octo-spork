# Socratic.pro — Backend API

The backend service powering [socratic.pro](https://socratic.pro): a job-board aggregation, resume-generation, and application-tracking API built with FastAPI.

It scrapes job listings from multiple sources, stores them in Postgres, and exposes authenticated REST endpoints for user profiles, resumes, job applications, and billing — with an AI layer (Groq) for content generation and PDF export for resumes.

## Tech Stack

- **Framework:** FastAPI + Uvicorn
- **Database:** PostgreSQL via SQLAlchemy 2.0
- **Cache:** Redis
- **Scraping:** httpx, BeautifulSoup4, lxml, and Playwright (for headless-browser API key interception, e.g. Dice)
- **Scheduling:** APScheduler (daily automated scrape job)
- **Auth:** JWT via `python-jose`, password hashing via `passlib` / `bcrypt`
- **AI:** Groq
- **PDF generation:** ReportLab (resume export)
- **Payments:** Stripe
- **Monitoring:** Sentry (FastAPI + SQLAlchemy integrations)

## Project Structure

```
silver-octo-spork/
├── main.py            # App entrypoint — FastAPI app, middleware, scheduler, core routes
├── auth.py             # Registration, login, JWT/session handling
├── database.py / db.py # DB engine, session management
├── models.py           # SQLAlchemy models
├── schemas.py          # Pydantic request/response schemas
├── scraper.py          # Job-scraping logic
├── cache.py            # Redis cache helpers
├── routes/
│   ├── profile.py       # User profile endpoints
│   ├── resumes.py       # Resume CRUD + generation
│   ├── applications.py  # Job application tracking
│   ├── oauth.py         # OAuth login flows
│   ├── billing.py       # Stripe billing endpoints
│   └── otp.py           # OTP / email verification
└── .github/workflows/   # CI
```

## API Overview

### Auth
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/auth/signup` | Register a new account, returns JWT + user |
| POST | `/api/auth/login` | Authenticate, returns JWT + user |
| GET | `/api/auth/me` | Current authenticated user (Bearer token) |

### Profile
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/profile` | Get profile |
| PUT | `/api/v1/profile` | Update profile |
| PATCH | `/api/v1/profile/skills` | Replace skills list |
| PATCH | `/api/v1/profile/experience` | Append experience entry |
| PATCH | `/api/v1/profile/education` | Append education entry |
| PUT | `/api/v1/profile/preferences` | Update job preferences |

### Resumes
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/resumes` | List resumes |
| POST | `/api/v1/resumes` | Create resume |
| GET | `/api/v1/resumes/{id}` | Get resume |
| PUT | `/api/v1/resumes/{id}` | Update resume |
| POST | `/api/v1/resumes/{id}/regenerate` | Bump version / regenerate |
| DELETE | `/api/v1/resumes/{id}` | Soft-delete (archive) |

### Applications
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/applications` | List applications |
| POST | `/api/v1/applications` | Log an application |
| GET | `/api/v1/applications/stats` | Counts by status |
| PATCH | `/api/v1/applications/{id}/status` | Update application status |

### Jobs & Scraping
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/jobs` | List/search jobs (filter by title, location, source) |
| POST | `/api/scrape` | Trigger a fresh scrape in the background |
| GET | `/api/scrape/status` | Check scrape progress |
| GET | `/health` | Health check |

Additional routers for OAuth (`routes/oauth.py`), billing (`routes/billing.py`), and OTP verification (`routes/otp.py`) are mounted in `main.py`. Interactive API docs are available at `/docs` (Swagger UI) once the server is running.

## Getting Started

### Prerequisites

- Python 3.11+ (see `python-version`)
- PostgreSQL
- Redis
- A Groq API key (AI features)
- A Stripe secret key (billing)

### Installation

```bash
git clone https://github.com/socratic-pro/silver-octo-spork.git
cd silver-octo-spork
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium  # required for Dice scraping
```

### Environment Variables

Create a `.env` file:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/socratic
REDIS_URL=redis://localhost:6379/0
JWT_SECRET=your-secret-key
GROQ_API_KEY=your-groq-key
STRIPE_SECRET_KEY=your-stripe-key
SCRAPE_ROLE="Software Developer"     # default role for the daily scheduled scrape
SCRAPE_LOCATION="California"         # default location for the daily scheduled scrape
```

### Run locally

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`, with docs at `http://localhost:8000/docs`.

On startup, the app creates database tables (if missing) and starts an APScheduler job that re-scrapes jobs every 24 hours.

## Scraping

`scraper.py` aggregates listings from multiple job boards (LinkedIn, Indeed, Dice, ZipRecruiter, RemoteOK, Glassdoor). Scrapes can be triggered manually via `POST /api/scrape` with a custom `role` and `location`, or run automatically once a day via the built-in scheduler. Only one scrape runs at a time (guarded by a lock); check progress via `GET /api/scrape/status`.

## Deployment

A `Procfile` is included for Heroku-style platforms (e.g. Railway, Render). CORS is currently locked to `https://socratic.pro` in `main.py` — update `ALLOWED_ORIGINS` if you add another frontend origin. Sentry is pre-wired for error/performance monitoring; set your own DSN before deploying to production.

## Related Repositories

- **Frontend:** [socratic-pro/socratic-fe](https://github.com/socratic-pro/socratic-fe)

## License

Proprietary — © Socratic Pro Private Limited. All rights reserved.
