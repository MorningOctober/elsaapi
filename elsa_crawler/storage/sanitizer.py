"""
HTML sanitization utilities for Elsa Crawler.
Converts HTML content to clean, structured Markdown text for vector indexing.
"""

import re
from typing import Optional

import html2text
from bs4 import BeautifulSoup


class HTMLSanitizer:
    """Sanitizes HTML content to clean Markdown text."""

    def __init__(self) -> None:
        """Initialize HTML sanitizer with html2text configuration."""
        self.h2t = html2text.HTML2Text()
        # Preserve structure
        self.h2t.body_width = 0  # No line wrapping
        self.h2t.ignore_links = False  # Keep link text, remove URLs
        self.h2t.ignore_images = True
        self.h2t.ignore_emphasis = False  # Keep bold/italic
        self.h2t.skip_internal_links = True
        self.h2t.single_line_break = False  # Use proper paragraph breaks
        self.h2t.mark_code = True
        # Remove unwanted elements
        self.h2t.ignore_tables = False  # Convert tables to text

    def sanitize(self, html_content: str) -> str:
        """
        Sanitize HTML content to clean Markdown text.

        Args:
            html_content: Raw HTML string

        Returns:
            Clean Markdown text with preserved structure
        """
        if not html_content or not html_content.strip():
            return ""

        try:
            # Step 1: Pre-process with BeautifulSoup
            cleaned_html = self._preprocess_html(html_content)

            # Step 2: Convert to Markdown
            markdown_text = self.h2t.handle(cleaned_html)

            # Step 3: Post-process Markdown
            clean_text = self._postprocess_markdown(markdown_text)

            return clean_text

        except Exception:
            # Fallback: Simple tag removal
            return self._fallback_sanitize(html_content)

    def _preprocess_html(self, html: str) -> str:
        """
        Pre-process HTML before conversion to Markdown.

        Args:
            html: Raw HTML string

        Returns:
            Cleaned HTML string
        """
        soup = BeautifulSoup(html, "lxml")

        # Remove unwanted elements
        for tag in soup(["script", "style", "meta", "link", "noscript", "iframe"]):
            tag.decompose()

        # Remove navigation/UI elements by common class/id patterns
        for pattern in ["nav", "menu", "sidebar", "footer", "header", "breadcrumb"]:
            for element in soup.find_all(class_=re.compile(pattern, re.I)):
                element.decompose()
            for element in soup.find_all(id=re.compile(pattern, re.I)):
                element.decompose()

        # Get cleaned HTML
        return str(soup)

    def _postprocess_markdown(self, markdown: str) -> str:
        """
        Post-process Markdown text for optimal indexing.

        Args:
            markdown: Markdown text from html2text

        Returns:
            Cleaned and normalized text
        """
        text = markdown

        # Remove excessive blank lines (more than 2)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Clean up list formatting
        text = re.sub(r"\n\s*\*\s+", "\n• ", text)  # Bullet points

        # Remove leftover HTML entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)  # Multiple spaces to single
        text = re.sub(r" \n", "\n", text)  # Remove trailing spaces

        # Remove markdown link URLs but keep text: [text](url) -> text
        text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)

        # Strip leading/trailing whitespace
        text = text.strip()

        return text

    def _fallback_sanitize(self, html: str) -> str:
        """
        Fallback sanitization using simple regex (if BeautifulSoup fails).

        Args:
            html: Raw HTML string

        Returns:
            Text with HTML tags removed
        """
        # Remove script/style tags and their content
        text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.I)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.I)

        # Remove all HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Decode common HTML entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)
        text = text.strip()

        return text


# Singleton instance for reuse
_sanitizer: Optional[HTMLSanitizer] = None


def sanitize_html(html_content: str) -> str:
    """
    Sanitize HTML content to clean Markdown text.
    Uses singleton instance for performance.

    Args:
        html_content: Raw HTML string

    Returns:
        Clean Markdown text with preserved structure
    """
    global _sanitizer
    if _sanitizer is None:
        _sanitizer = HTMLSanitizer()
    return _sanitizer.sanitize(html_content)
