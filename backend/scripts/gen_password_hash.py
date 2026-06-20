#!/usr/bin/env python3
"""
Generate a bcrypt password hash for use as ADMIN_PASSWORD_HASH.

Usage:
    uv run python scripts/gen_password_hash.py
    # or:
    python scripts/gen_password_hash.py

Paste the printed hash (including the $2b$ prefix) into your .env file as:
    ADMIN_PASSWORD_HASH=<hash>

Never store the plaintext password in .env or anywhere in the codebase.
"""

import getpass
import sys

try:
    import bcrypt
except ImportError:
    print("bcrypt is not installed. Run: uv add bcrypt", file=sys.stderr)
    sys.exit(1)

password = getpass.getpass("Enter password to hash: ")
if not password:
    print("Password cannot be empty.", file=sys.stderr)
    sys.exit(1)

confirm = getpass.getpass("Confirm password: ")
if password != confirm:
    print("Passwords do not match.", file=sys.stderr)
    sys.exit(1)

hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
print(f"\nADMIN_PASSWORD_HASH={hashed}")
print("\nAdd the line above to your backend/.env file.")
