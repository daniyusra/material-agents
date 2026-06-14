/**
 * Blog API client.
 *
 * All mutating calls include credentials (the httpOnly session cookie) and the
 * X-Requested-With header required by the server's CSRF guard.
 * Fetch is used directly, mirroring the pattern in src/api.ts.
 */

const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

const CSRF_HEADERS = {
  "X-Requested-With": "XMLHttpRequest",
};

async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    credentials: "include",
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw Object.assign(new Error(body.detail ?? "Request failed"), {
      status: res.status,
      body,
    });
  }
  return res.json() as Promise<T>;
}

// ── Types ─────────────────────────────────────────────────────────────────────

export interface Article {
  id: number;
  title: string;
  slug: string;
  body?: string;           // raw Markdown (admin only)
  body_html?: string;      // rendered HTML (public endpoint)
  excerpt: string;
  meta_description?: string;
  cover_image_url?: string;
  og_image_url?: string;
  tags: string[];
  status: "draft" | "published";
  published_at?: number;
  created_at: number;
  updated_at: number;
  author: string;
  slug_frozen: number;
  reading_time_minutes: number;
}

export interface ArticleList {
  articles: Article[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

export interface MediaUploadResult {
  url: string;
  thumb_url: string;
  width: number;
  height: number;
  alt_text: string;
}

// ── Auth ──────────────────────────────────────────────────────────────────────

export const blogApi = {
  me(): Promise<{ authenticated: boolean }> {
    return request("/api/auth/me");
  },

  login(username: string, password: string): Promise<{ ok: boolean }> {
    return request("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },

  logout(): Promise<{ ok: boolean }> {
    return request("/api/auth/logout", {
      method: "POST",
      headers: { ...CSRF_HEADERS },
    });
  },

  // ── Public ──────────────────────────────────────────────────────────────────

  listArticles(page = 1, perPage = 10): Promise<ArticleList> {
    return request(`/api/articles?page=${page}&per_page=${perPage}`);
  },

  getArticle(slug: string): Promise<Article & { body_html: string }> {
    return request(`/api/articles/${slug}`);
  },

  // ── Admin ────────────────────────────────────────────────────────────────────

  adminListArticles(): Promise<{ articles: Article[] }> {
    return request("/api/admin/articles");
  },

  adminGetArticle(id: number): Promise<Article> {
    return request(`/api/admin/articles/${id}`);
  },

  adminCreateArticle(title: string): Promise<Article> {
    return request("/api/admin/articles", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...CSRF_HEADERS },
      body: JSON.stringify({ title }),
    });
  },

  adminUpdateArticle(
    id: number,
    fields: Partial<Omit<Article, "id" | "created_at" | "updated_at" | "reading_time_minutes">>
  ): Promise<{ article: Article; warnings: string[] }> {
    return request(`/api/admin/articles/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", ...CSRF_HEADERS },
      body: JSON.stringify(fields),
    });
  },

  adminDeleteArticle(id: number): Promise<{ ok: boolean }> {
    return request(`/api/admin/articles/${id}`, {
      method: "DELETE",
      headers: { ...CSRF_HEADERS },
    });
  },

  // ── Media ────────────────────────────────────────────────────────────────────

  uploadMedia(file: File, altText: string): Promise<MediaUploadResult> {
    const form = new FormData();
    form.append("file", file);
    form.append("alt_text", altText);
    return request("/api/media", {
      method: "POST",
      headers: { ...CSRF_HEADERS },
      body: form,
    });
  },
};
