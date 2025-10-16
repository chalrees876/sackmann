import os
from .settings_base import *
import os

ALLOWED_HOSTS = [
    "127.0.0.1", "localhost",
    "18.216.90.98",        # your public IP
    ".compute-1.amazonaws.com",
"tennisml.duckdns.org"  # EC2 public DNS
]

def _csv(name, default=""):
    raw = os.getenv(name, default)
    # Return list, stripping whitespace and dropping empties
    return [x.strip() for x in raw.split(",") if x.strip()]

CSRF_TRUSTED_ORIGINS = [
    o for o in _csv("DJANGO_CSRF_TRUSTED_ORIGINS")
    if o.startswith("http://") or o.startswith("https://")
]
SECRET_KEY = os.environ["SECRET_KEY"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("DB_NAME"),
        "USER": os.getenv("DB_USER"),
        "PASSWORD": os.getenv("DB_PASSWORD"),
        "HOST": os.getenv("DB_HOST"),
        "PORT": os.getenv("DB_PORT"),
    }
}
