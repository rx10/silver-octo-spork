from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class JobOut(BaseModel):
    id:          str
    title:       str
    company:     str
    location:    str
    posted_date: Optional[str]
    description: Optional[str]
    salary:      Optional[str]
    url:         str
    source:      str
    scraped_at:  datetime

    class Config:
        from_attributes = True


class ScrapeRequest(BaseModel):
    role:     str = "Software Developer"
    location: str = "California"


class ScrapeResponse(BaseModel):
    scraped: int
    new:     int
    message: str
