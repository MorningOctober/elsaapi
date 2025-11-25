"""
Categories extractor for Elsa Crawler.
Extracts category tree from navigation frame.
"""

from typing import Any

from playwright.async_api import Frame

from elsa_crawler.models import Category


class CategoryExtractor:
    """Extracts categories from ElsaPro navigation tree."""

    @staticmethod
    async def collect_all_categories(frame: Frame) -> list[Category]:
        """
        Collect all categories from navigation tree.

        Args:
            frame: Navigation frame containing category tree

        Returns:
            List of Category instances
        """
        raw_categories: list[dict[str, Any]] = await frame.evaluate("""
            () => {
                const results = [];
                
                function parseNode(li, path, depth) {
                    const link = li.querySelector(':scope > a');
                    if (!link) return;
                    
                    // Extract category name
                    let name = '';
                    for (const node of link.childNodes) {
                        if (node.nodeType === Node.TEXT_NODE) {
                            name += (node.textContent || '').trim();
                        }
                    }
                    if (!name) {
                        name = (link.textContent || '').trim();
                    }
                    name = name.replace(/^image/i, '').trim();
                    
                    if (!name) return;
                    
                    const href = link.getAttribute('href') || '';
                    const currentPath = [...path, name];
                    
                    // Extract levelCode from href
                    const levelMatch = href.match(/levelCode=([^&]+)/);
                    const levelCode = levelMatch ? levelMatch[1] : '';
                    
                    // Only add if valid href (not emptyPage)
                    if (href && !href.includes('emptyPage')) {
                        results.push({
                            id: levelCode || name,
                            name: name,
                            path: currentPath,
                            href: href,
                            depth: depth,
                            hasChildren: !!li.querySelector(':scope > ul')
                        });
                    }
                    
                    // ALWAYS recursively process children (even if parent is emptyPage)
                    const childUl = li.querySelector(':scope > ul');
                    if (childUl) {
                        const childLis = childUl.querySelectorAll(':scope > li');
                        childLis.forEach(childLi => {
                            // Use currentPath even if parent wasn't added (emptyPage)
                            parseNode(childLi, currentPath, depth + 1);
                        });
                    }
                }
                
                // Find root LI
                const allLis = document.querySelectorAll('li');
                for (const li of allLis) {
                    const text = (li.textContent || '').trim();
                    if (text.startsWith('Handbuch Service Technik') || 
                        text.includes('Neuheiten')) {
                        const childUl = li.querySelector(':scope > ul');
                        if (childUl) {
                            const childLis = childUl.querySelectorAll(':scope > li');
                            childLis.forEach(childLi => {
                                parseNode(childLi, ['Handbuch Service Technik'], 1);
                            });
                            break;
                        } else {
                            parseNode(li, [], 0);
                        }
                    }
                }
                
                return results;
            }
        """)

        # Convert to Category models
        categories: list[Category] = []
        for raw_cat in raw_categories:
            categories.append(
                Category(
                    id=raw_cat.get("id", ""),
                    name=raw_cat.get("name", ""),
                    url=raw_cat.get("href"),
                    depth=raw_cat.get("depth", 0),
                    has_children=raw_cat.get("hasChildren", False),
                )
            )

        return categories

    @staticmethod
    async def find_child_categories(frame: Frame, parent_id: str) -> list[str]:
        """
        Find child category IDs for a parent.

        Args:
            frame: Navigation frame
            parent_id: Parent category ID/name

        Returns:
            List of child category names
        """
        children: list[str] = await frame.evaluate(
            """
            (parentId) => {
                const childIds = [];
                const allLis = document.querySelectorAll('li');
                let parentLi = null;
                
                // Find parent LI
                for (const li of allLis) {
                    const fullText = (li.textContent || '').trim();
                    if (fullText.includes(parentId)) {
                        if (li.querySelector(':scope > ul')) {
                            parentLi = li;
                            break;
                        }
                    }
                }
                
                if (!parentLi) return childIds;
                
                // Find direct child UL
                const childUl = parentLi.querySelector(':scope > ul');
                if (!childUl) return childIds;
                
                // Get direct child LIs
                const childLis = childUl.querySelectorAll(':scope > li');
                childLis.forEach(childLi => {
                    const link = childLi.querySelector(':scope > a');
                    if (link) {
                        let name = '';
                        for (const node of link.childNodes) {
                            if (node.nodeType === Node.TEXT_NODE) {
                                name += (node.textContent || '').trim();
                            }
                        }
                        if (!name) {
                            name = (link.textContent || '').trim();
                        }
                        name = name.replace(/^image/i, '').trim();
                        if (name) {
                            childIds.push(name);
                        }
                    }
                });
                
                return childIds;
            }
            """,
            parent_id,
        )

        return children
