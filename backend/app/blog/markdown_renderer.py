"""
Markdown → sanitised HTML pipeline for blog article bodies.

1. mistune 3.x renders Markdown to HTML with a custom renderer that wraps
   images in <figure>/<figcaption> and adds loading="lazy".
2. nh3 sanitises the output with a strict allowlist — raw embedded HTML in
   the source is neutralised here, not trusted.
"""

import re

import mistune
import nh3


# ── Allowed HTML after sanitisation ───────────────────────────────────────────

_ALLOWED_TAGS = {
    "a", "abbr", "b", "blockquote", "br", "caption", "cite", "code", "col",
    "colgroup", "dd", "del", "details", "dfn", "div", "dl", "dt", "em",
    "figure", "figcaption", "h1", "h2", "h3", "h4", "h5", "h6", "hr", "i",
    "img", "ins", "kbd", "li", "mark", "ol", "p", "pre", "q", "s", "samp",
    "small", "span", "strike", "strong", "sub", "summary", "sup", "table",
    "tbody", "td", "th", "thead", "tr", "u", "ul", "var",
}

_ALLOWED_ATTRS: dict[str, set[str]] = {
    "a":    {"href", "title", "rel"},
    "abbr": {"title"},
    "cite": {"title"},
    "code": {"class"},           # language-* class for highlight.js
    "col":  {"span"},
    "div":  {"class", "id"},
    "figure": {"class"},
    "h1":   {"id"}, "h2": {"id"}, "h3": {"id"},
    "h4":   {"id"}, "h5": {"id"}, "h6": {"id"},
    "img":  {"src", "alt", "title", "loading", "width", "height"},
    "pre":  {"class"},
    "span": {"class"},
    "td":   {"align", "colspan", "rowspan"},
    "th":   {"align", "colspan", "rowspan", "scope"},
}


# ── Custom renderer ────────────────────────────────────────────────────────────

class _BlogRenderer(mistune.HTMLRenderer):
    """Adds loading="lazy" to images and wraps them in <figure>."""

    def image(self, alt: str, url: str, title: str = "") -> str:  # type: ignore[override]
        img_parts = [f'<img src="{url}" alt="{alt}" loading="lazy"']
        if title:
            img_parts.append(f' title="{title}"')
        img_parts.append(">")
        img_tag = "".join(img_parts)

        if title:
            return f"<figure>{img_tag}<figcaption>{title}</figcaption></figure>\n"
        return f"<figure>{img_tag}</figure>\n"

    def block_code(self, code: str, **attrs) -> str:  # type: ignore[override]
        lang = (attrs.get("info") or "").strip().split()[0] if attrs.get("info") else ""
        lang_class = f' class="language-{lang}"' if lang else ""
        return f"<pre><code{lang_class}>{mistune.escape(code)}</code></pre>\n"


_md = mistune.create_markdown(
    renderer=_BlogRenderer(escape=False),
    plugins=["strikethrough", "table", "url"],
)


# ── Public API ─────────────────────────────────────────────────────────────────

def render_markdown(body: str) -> str:
    """Render Markdown to sanitised HTML safe for dangerouslySetInnerHTML."""
    raw_html: str = _md(body)  # type: ignore[assignment]
    return nh3.clean(
        raw_html,
        tags=_ALLOWED_TAGS,
        attributes=_ALLOWED_ATTRS,
        url_schemes={"http", "https", "mailto", "data"},
        link_rel=None,       # don't clobber explicitly set rel attrs
        strip_comments=True,
    )


def extract_media_urls(body: str) -> list[str]:
    """Extract /media/... URLs from Markdown image syntax for media association."""
    return re.findall(r"!\[.*?\]\((/media/[^)\s]+)\)", body)


def missing_alt_images(body: str) -> list[str]:
    """Return /media/... URLs whose alt text is blank — flagged before publish."""
    return re.findall(r"!\[\s*\]\((/media/[^)\s]+)\)", body)
