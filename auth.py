"""Admin password + signed session cookie (stdlib only)."""
import base64
import hashlib
import hmac
import secrets
import time

COOKIE = "ura_session"
SESSION_DAYS = 30


def hash_password(pw):
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(pw.encode(), salt=salt, n=2 ** 14, r=8, p=1)
    return "scrypt$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(dk).decode()


def check_password(pw, stored):
    try:
        _, salt, dk = stored.split("$")
        got = hashlib.scrypt(pw.encode(), salt=base64.b64decode(salt), n=2 ** 14, r=8, p=1)
        return hmac.compare_digest(got, base64.b64decode(dk))
    except Exception:
        return False


def _sig(secret, payload):
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def make_session(secret, pw_hash):
    # binding the cookie to the password hash logs everyone out when the password changes
    exp = str(int(time.time()) + SESSION_DAYS * 86400)
    return exp + "." + _sig(secret, exp + pw_hash)


def valid_session(token, secret, pw_hash):
    if not token or not pw_hash or "." not in token:
        return False
    exp, sig = token.split(".", 1)
    if not exp.isdigit() or int(exp) < time.time():
        return False
    return hmac.compare_digest(sig, _sig(secret, exp + pw_hash))
