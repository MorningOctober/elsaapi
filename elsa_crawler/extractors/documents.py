"""
Documents extractor for Elsa Crawler.
Extracts document content from ElsaPro pages.
"""

from typing import Any, Optional, cast

from playwright.async_api import Frame, Page

from elsa_crawler.models import ExtractedDocument


class DocumentExtractor:
    """Extracts document content from ElsaPro."""

    @staticmethod
    async def extract_document_content(
        page: Page,
        document_frame: Optional[Frame],
        vorgangs_nr: str,
        category_id: str,
        category_name: str,
    ) -> Optional[ExtractedDocument]:
        """
        Extract document content from page/frame.

        Args:
            page: Playwright page
            document_frame: Cached document frame (if available)
            vorgangs_nr: Document ID
            category_id: Category identifier
            category_name: Category display name

        Returns:
            ExtractedDocument if content found, None otherwise
        """
        # Try document frame first
        if document_frame:
            content = await DocumentExtractor._try_extract_from_frame(document_frame)
            if content:
                return ExtractedDocument(
                    category_id=category_id,
                    category_name=category_name,
                    vorgangs_nr=vorgangs_nr,
                    title=vorgangs_nr,
                    content=content["text"],
                    url=page.url,
                    extraction_method="new",
                    metadata={"html_preview": content["html"][:1000]},
                )

        # Fallback: search all frames
        best_content = None
        best_length = 0

        for frame in page.frames:
            content = await DocumentExtractor._try_extract_from_frame(frame)
            if content and content.get("length", 0) > best_length:
                best_content = content
                best_length = content["length"]

        if best_content:
            return ExtractedDocument(
                category_id=category_id,
                category_name=category_name,
                vorgangs_nr=vorgangs_nr,
                title=vorgangs_nr,
                content=best_content["text"],
                url=page.url,
                extraction_method="fallback",
                metadata={"html_preview": best_content["html"][:1000]},
            )

        return None

    @staticmethod
    async def _try_extract_from_frame(frame: Frame) -> Optional[dict[str, Any]]:
        """
        Try to extract valid document content from a frame.

        Args:
            frame: Frame to extract from

        Returns:
            Dict with text, html, length if valid, None otherwise
        """
        try:
            content = cast(
                Optional[dict[str, Any]],
                await frame.evaluate("""
                () => {
                    const text = document.body.innerText || '';
                    
                    // Minimum length check
                    if (text.length < 100) return null;
                    
                    // Exclude navigation frame
                    if (text.includes('Neuheiten') && 
                        text.includes('Feldmaßnahmen') && 
                        text.includes('Hinweise')) return null;
                    
                    // Check for document markers
                    const hasDocMarkers = text.includes('Vorgangs-Nr') || 
                                         text.includes('Kundenaussage') ||
                                         text.includes('Kundenbemerkung') ||
                                         text.includes('Lösung') ||
                                         text.includes('Datum:') ||
                                         text.includes('Fahrzeug');
                    
                    if (!hasDocMarkers) return null;
                    
                    return {
                        text: text,
                        html: document.body.innerHTML || '',
                        length: text.length
                    };
                }
            """),
            )

            return content
        except Exception:
            return None
