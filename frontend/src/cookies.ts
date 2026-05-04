export function getCookie(name: string): string | undefined {
  const entry = document.cookie.split("; ").find((r) => r.startsWith(name + "="));
  return entry ? decodeURIComponent(entry.split("=")[1]) : undefined;
}

export function setCookie(name: string, value: string, days = 30): void {
  const expires = new Date(Date.now() + days * 864e5).toUTCString();
  document.cookie = `${name}=${encodeURIComponent(value)}; expires=${expires}; path=/; SameSite=Strict`;
}

export function deleteCookie(name: string): void {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
}
