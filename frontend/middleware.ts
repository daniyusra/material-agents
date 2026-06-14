/**
 * Vercel Edge Middleware — OG meta tag injection for blog routes.
 *
 * Problem: The app is a client-rendered SPA. Link unfurlers (WhatsApp, Slack,
 * LinkedIn, iMessage, Telegram) and search crawlers read the raw initial HTML
 * before any JavaScript executes, so they see an empty <div id="root"> and
 * pick up no title, description, or image.
 *
 * Fix: This middleware intercepts requests to /blog and /blog/:slug. If the
 * User-Agent looks like a bot, it fetches the article metadata from the FastAPI
 * backend, injects proper <title>, <meta>, Open Graph, and Twitter Card tags
 * into a lightweight HTML response, and returns it. Human browsers receive
 * nothing — the static Vite SPA is served as normal.
 *
 * Env vars needed in Vercel project settings:
 *   BLOG_API_URL    — absolute URL of the FastAPI backend (no trailing slash)
 *                     e.g. https://material-agents-backend.fly.dev
 *   SITE_BASE_URL   — absolute origin of this Vercel deployment
 *                     e.g. https://material-agents.vercel.app
 */

const BOT_UAS = [
  "whatsapp",
  "slack",
  "twitterbot",
  "facebookexternalhit",
  "facebookcatalog",
  "linkedinbot",
  "telegrambot",
  "discordbot",
  "googlebot",
  "bingbot",
  "duckduckbot",
  "applebot",
  "iframely",
  "embedly",
  "outbrain",
  "pinterestbot",
  "redditbot",
  "curl",
  "wget",
];

function isBot(userAgent: string): boolean {
  const ua = userAgent.toLowerCase();
  return BOT_UAS.some((b) => ua.includes(b));
}

function esc(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function buildOgHtml(meta: {
  title: string;
  description: string;
  image?: string;
  canonical: string;
  type?: string;
}): string {
  const { title, description, image, canonical, type = "website" } = meta;
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${esc(title)}</title>
  <meta name="description" content="${esc(description)}">
  <link rel="canonical" href="${esc(canonical)}">

  <meta property="og:type" content="${esc(type)}">
  <meta property="og:site_name" content="Blog">
  <meta property="og:title" content="${esc(title)}">
  <meta property="og:description" content="${esc(description)}">
  <meta property="og:url" content="${esc(canonical)}">
  ${image ? `<meta property="og:image" content="${esc(image)}">` : ""}

  <meta name="twitter:card" content="${image ? "summary_large_image" : "summary"}">
  <meta name="twitter:title" content="${esc(title)}">
  <meta name="twitter:description" content="${esc(description)}">
  ${image ? `<meta name="twitter:image" content="${esc(image)}">` : ""}

  <!-- Redirect human browsers who somehow land here to the SPA -->
  <meta http-equiv="refresh" content="0; url=${esc(canonical)}">
  <script>window.location.replace(${JSON.stringify(canonical)});</script>
</head>
<body>
  <p>Loading: <a href="${esc(canonical)}">${esc(title)}</a></p>
</body>
</html>`;
}

export default async function middleware(
  request: Request
): Promise<Response | void> {
  const ua = request.headers.get("user-agent") ?? "";
  if (!isBot(ua)) return; // Human: serve the static SPA as normal

  const url = new URL(request.url);
  const apiBase = (process.env.BLOG_API_URL ?? "").replace(/\/$/, "");
  const siteBase = (process.env.SITE_BASE_URL ?? url.origin).replace(/\/$/, "");

  if (!apiBase) return; // Middleware not configured yet — pass through

  try {
    const slugMatch = url.pathname.match(/^\/blog\/(.+)$/);

    if (slugMatch) {
      const slug = slugMatch[1];
      const res = await fetch(`${apiBase}/api/articles/${slug}`, {
        headers: { "Accept": "application/json" },
      });

      if (!res.ok) return; // 404 or error — serve SPA normally

      const article = await res.json();
      const canonical = `${siteBase}/blog/${article.slug}`;
      const description: string =
        article.meta_description || article.excerpt || "";
      const imageUrl: string | undefined =
        article.og_image_url || article.cover_image_url
          ? `${siteBase}${article.og_image_url || article.cover_image_url}`
          : undefined;

      return new Response(
        buildOgHtml({
          title: article.title,
          description,
          image: imageUrl,
          canonical,
          type: "article",
        }),
        {
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Cache-Control": "no-cache, no-store",
          },
        }
      );
    }

    // /blog index
    return new Response(
      buildOgHtml({
        title: "Blog",
        description: "Articles, notes, and experiments.",
        canonical: `${siteBase}/blog`,
      }),
      {
        headers: {
          "Content-Type": "text/html; charset=utf-8",
          "Cache-Control": "no-cache, no-store",
        },
      }
    );
  } catch {
    return; // Any fetch failure → fall through to the static SPA
  }
}

export const config = {
  matcher: ["/blog", "/blog/:slug*"],
};
