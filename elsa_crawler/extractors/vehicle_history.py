"""
Vehicle History Extractor for ElsaPro.
Extracts complete service history including repairs, complaints, and invoices.
"""

from datetime import datetime
from typing import Any, Optional

from playwright.async_api import Frame, Page

from elsa_crawler.browser.manager import BrowserManager
from elsa_crawler.models import (
    ComplaintEntry,
    InvoiceEntry,
    ServicePlanEntry,
    VehicleHistory,
)


class VehicleHistoryExtractor:
    """Extracts vehicle service history from ElsaPro Fahrzeughistorie section."""

    @staticmethod
    async def extract_complete_history(
        browser_manager: BrowserManager, vin: str
    ) -> VehicleHistory:
        """
        Extract complete vehicle history.

        Args:
            browser_manager: Active browser manager with authenticated session
            vin: Vehicle VIN

        Returns:
            VehicleHistory with all extracted entries

        Raises:
            RuntimeError: If navigation or extraction fails
        """
        if not browser_manager.page:
            raise RuntimeError("Browser page not available")

        # Navigate to Fahrzeughistorie
        history_page = await VehicleHistoryExtractor._navigate_to_history(
            browser_manager.page
        )

        # Get iframe containing history table
        history_frame = await VehicleHistoryExtractor._get_history_frame(history_page)

        # Expand all entries
        await VehicleHistoryExtractor._expand_all_entries(history_frame)

        # Extract all entries
        entries = await VehicleHistoryExtractor._extract_all_entries(history_frame)

        # Close history page to clean up
        await history_page.close()
        print("  ✓ History page closed")

        # Count statistics
        total = len(entries)
        successful = len([e for e in entries if e is not None])
        failed = total - successful

        # Determine status
        status = "complete" if failed == 0 else "partial"

        return VehicleHistory(
            vin=vin,
            extraction_timestamp=datetime.now().timestamp(),
            extraction_status=status,
            total_entries=total,
            successful_entries=successful,
            failed_entries=failed,
            entries=[e for e in entries if e is not None],
        )

    @staticmethod
    async def _navigate_to_history(page: Page) -> Page:
        """
        Navigate to Fahrzeughistorie by clicking link.

        Args:
            page: Current page with VIN details

        Returns:
            New page with vehicle history

        Raises:
            RuntimeError: If navigation fails
        """
        try:
            # Get mainFs iframe
            main_frame = page.frame(name="mainFs")
            if not main_frame:
                raise RuntimeError("mainFs frame not found")

            # Click Fahrzeughistorie link (opens new tab)
            async with page.context.expect_page() as new_page_info:
                await main_frame.locator('a:has-text("Fahrzeughistorie")').click()

            history_page = await new_page_info.value
            await history_page.wait_for_load_state("networkidle")
            print("  ✓ History page opened")

            # Wait for frames to load
            await history_page.wait_for_timeout(2000)

            return history_page

        except Exception as exc:
            raise RuntimeError(f"Failed to navigate to history: {exc}") from exc

    @staticmethod
    async def _get_history_frame(page: Page) -> Frame:
        """
        Get iframe containing history table.

        Args:
            page: History page

        Returns:
            Frame with history table

        Raises:
            RuntimeError: If frame not found
        """
        try:
            # Wait for iframe to be present
            print("  ⏳ Waiting for history iframe...")
            await page.wait_for_selector("iframe", timeout=15000)

            # Get all frames
            frames = page.frames
            print(f"  🔍 Found {len(frames)} frames total")

            # Find the frame containing the history grid
            # The iframe contains history.html URL
            history_frame = None
            for idx, frame in enumerate(frames):
                frame_url = frame.url
                print(f"  🔍 Frame {idx}: {frame_url}")

                # Check if this frame contains history data
                if "history.html" in frame_url or "VehicleInfo" in frame_url:
                    history_frame = frame
                    print(f"  ✓ Found history frame at index {idx}")
                    break

            if not history_frame:
                # Fallback: try second frame (index 1)
                if len(frames) >= 2:
                    history_frame = frames[1]
                    print("  ⚠️  Using fallback frame (index 1)")
                else:
                    raise RuntimeError("No suitable iframe found")

            # Wait for frame to fully load
            print("  ⏳ Waiting for frame to load...")
            await history_frame.wait_for_load_state("domcontentloaded", timeout=15000)

            # Wait for Angular app to initialize (typically 2 seconds)
            print("  ⏳ Waiting for Angular to initialize...")
            await page.wait_for_timeout(2000)

            # Wait for grid to appear in the frame
            print("  ⏳ Waiting for grid...")
            await history_frame.wait_for_selector(
                '[role="grid"]', state="visible", timeout=10000
            )
            print("  ✓ Grid loaded successfully")

            return history_frame

        except Exception as exc:
            raise RuntimeError(f"Failed to get history frame: {exc}") from exc

    @staticmethod
    async def _expand_all_entries(frame: Frame) -> None:
        """
        Expand all history entries by clicking toggleAllRows button.

        Args:
            frame: Frame with history table

        Raises:
            RuntimeError: If expansion fails
        """
        try:
            # First, change display from 10 to 100 entries
            print("  ⏳ Changing display to 100 entries...")
            entries_dropdown = frame.locator("select")
            await entries_dropdown.wait_for(state="visible", timeout=5000)
            await entries_dropdown.select_option("100")

            # Wait for table to re-render with 100 entries
            await frame.wait_for_timeout(2000)
            print("  ✓ Display changed to 100 entries")

            # Find and click toggleAllRows button
            print("  ⏳ Expanding all rows...")
            toggle_button = frame.locator('div[ng-click="toggleAllRows()"]')
            await toggle_button.wait_for(state="visible", timeout=5000)
            await toggle_button.click()

            # Wait for Angular to render all expanded rows
            await frame.wait_for_timeout(3000)
            print("  ✓ All rows expanded")

        except Exception as exc:
            raise RuntimeError(f"Failed to expand entries: {exc}") from exc

    @staticmethod
    async def _extract_all_entries(
        frame: Frame,
    ) -> list[Optional[ServicePlanEntry | ComplaintEntry | InvoiceEntry]]:
        """
        Extract all history entries from table.

        Args:
            frame: Frame with expanded history table

        Returns:
            List of extracted entries (None for failed extractions)
        """
        entries: list[Optional[ServicePlanEntry | ComplaintEntry | InvoiceEntry]] = []

        try:
            # Get only the main data rows with ng-repeat attribute (one per history entry)
            # Each entry is a <tr ng-repeat="repair in filteredVehicleHistory">
            rows = await frame.locator(
                'table#history-table tbody tr[ng-repeat="repair in filteredVehicleHistory"]'
            ).all()

            print(f"  Found {len(rows)} history entries")

            for idx, row in enumerate(rows):
                try:
                    # Extract entry type from first .col-xs-2 span
                    entry_type_text = await row.locator(
                        "td .col-xs-2 span.text-bold"
                    ).first.inner_text()

                    entry_type = entry_type_text.strip()

                    # Route to appropriate parser
                    if "Digitaler Serviceplan" in entry_type:
                        entry = await VehicleHistoryExtractor._parse_service_plan(row)
                    elif "Beanstandung" in entry_type:
                        entry = await VehicleHistoryExtractor._parse_complaint(row)
                    elif "Rechnung" in entry_type:
                        entry = await VehicleHistoryExtractor._parse_invoice(row)
                    else:
                        print(f"  ⚠️  Unknown entry type: {entry_type}")
                        entry = None

                    entries.append(entry)

                except Exception as exc:
                    print(f"  ⚠️  Failed to extract entry {idx + 1}: {exc}")
                    entries.append(None)

            return entries

        except Exception as exc:
            print(f"  ❌ Failed to extract entries: {exc}")
            return []

    @staticmethod
    async def _parse_service_plan(row: Any) -> ServicePlanEntry:
        """
        Parse Digitaler Serviceplan entry.

        Args:
            row: Row element handle

        Returns:
            ServicePlanEntry with extracted data
        """
        # Extract basic fields
        date = await VehicleHistoryExtractor._extract_field_value(row, "Annahmetermin:")
        mileage = await VehicleHistoryExtractor._extract_field_value(
            row, "Laufleistung:"
        )
        order_num = await VehicleHistoryExtractor._extract_field_value(
            row, "Auftrags-Nr"
        )
        service_proof = await VehicleHistoryExtractor._extract_field_value(
            row, "Service-Nachweis:"
        )

        # Extract additional work table
        additional_work = await VehicleHistoryExtractor._extract_table(
            row, "Zusatzarbeiten"
        )

        # Extract remarks/description table (fallback structure)
        remarks = await VehicleHistoryExtractor._extract_generic_table(row)

        return ServicePlanEntry(
            acceptance_date=date,
            mileage=int(mileage) if mileage.isdigit() else 0,
            order_number=order_num,
            service_proof=service_proof or "",
            additional_work=additional_work,
            remarks=remarks,
        )

    @staticmethod
    async def _parse_complaint(row: Any) -> ComplaintEntry:
        """
        Parse Beanstandung entry.

        Args:
            row: Row element handle

        Returns:
            ComplaintEntry with extracted data
        """
        # Extract basic fields
        date = await VehicleHistoryExtractor._extract_field_value(row, "Annahmetermin:")
        mileage = await VehicleHistoryExtractor._extract_field_value(
            row, "Laufleistung:"
        )
        order_num = await VehicleHistoryExtractor._extract_field_value(
            row, "Auftrags-Nr"
        )
        ba_id = await VehicleHistoryExtractor._extract_field_value(row, "BA-ID:")

        # Extract complaint details from table
        details = await VehicleHistoryExtractor._extract_complaint_details(row)

        return ComplaintEntry(
            acceptance_date=date,
            mileage=int(mileage) if mileage.isdigit() else 0,
            order_number=order_num,
            ba_id=ba_id,
            customer_complaint=details.get("customer_complaint"),
            customer_coding=details.get("customer_coding"),
            workshop_finding=details.get("workshop_finding"),
            workshop_coding=details.get("workshop_coding"),
            damage_part=details.get("damage_part"),
        )

    @staticmethod
    async def _parse_invoice(row: Any) -> InvoiceEntry:
        """
        Parse Rechnung entry.

        Args:
            row: Row element handle

        Returns:
            InvoiceEntry with extracted data
        """
        # Extract basic fields
        date = await VehicleHistoryExtractor._extract_field_value(row, "Annahmetermin:")
        mileage = await VehicleHistoryExtractor._extract_field_value(
            row, "Laufleistung:"
        )
        order_num = await VehicleHistoryExtractor._extract_field_value(
            row, "Auftrags-Nr"
        )
        invoice_num = await VehicleHistoryExtractor._extract_field_value(
            row, "Rechnungs-Nr"
        )
        remark = await VehicleHistoryExtractor._extract_field_value(row, "Anmerk.")

        # Extract work positions
        work_positions = await VehicleHistoryExtractor._extract_table(
            row, "Arbeitsposition"
        )

        # Extract parts positions
        parts_positions = await VehicleHistoryExtractor._extract_table(
            row, "Teileposition"
        )

        return InvoiceEntry(
            acceptance_date=date,
            mileage=int(mileage) if mileage.isdigit() else 0,
            order_number=order_num,
            invoice_number=invoice_num,
            remark=remark or None,
            work_positions=work_positions,
            parts_positions=parts_positions,
        )

    @staticmethod
    async def _extract_field_value(row: Any, label: str) -> str:
        """
        Extract field value by label.

        Args:
            row: Row element
            label: Field label to search for (translated text, e.g., "Annahmetermin")

        Returns:
            Field value or empty string
        """
        try:
            # Find all .col-xs-2 divs and check their text content
            # Structure: <div class="col-xs-2"><span translate="...">Label</span>:<br>Value</div>
            field_divs = await row.locator("td .col-xs-2").all()

            for div in field_divs:
                text = await div.inner_text()
                # Check if this div contains the label
                if label in text:
                    # Split by line breaks and get the value (after label and colon)
                    lines = text.split("\n")
                    if len(lines) >= 2:
                        # First line is label, second line is value
                        return lines[1].strip()
            return ""
        except Exception:
            return ""

    @staticmethod
    async def _extract_table(row: Any, table_title: str) -> list[dict[str, str]]:
        """
        Extract table data by title.

        Args:
            row: Row element
            table_title: Title of table section

        Returns:
            List of row dicts with column headers as keys
        """
        try:
            # Find the section div containing the title
            section = row.locator(f'div:has-text("{table_title}")').first

            # Find the table within the parent container of this section
            # Structure: <div ng-if="repair.showX"> contains both title div and table div
            table = section.locator(
                "xpath=ancestor::div[1]//table.embedded-table"
            ).first

            # Extract headers
            headers: list[str] = await table.locator("thead th").all_inner_texts()  # type: ignore[misc]

            # Extract data rows
            data_rows = await table.locator("tbody tr").all()

            results: list[dict[str, str]] = []
            for data_row in data_rows:
                cells: list[str] = await data_row.locator("td").all_inner_texts()  # type: ignore[misc]
                row_dict = {
                    headers[i]: cells[i].strip()
                    for i in range(min(len(headers), len(cells)))
                }
                results.append(row_dict)

            return results

        except Exception:
            return []

    @staticmethod
    async def _extract_generic_table(row: Any) -> list[dict[str, str]]:
        """
        Extract generic table (no specific title).

        Args:
            row: Row element

        Returns:
            List of row dicts
        """
        try:
            # Find any table in expanded section
            tables = await row.locator("table").all()

            if not tables:
                return []

            # Use first table found
            table = tables[0]

            # Extract all rows
            data_rows = await table.locator("tbody tr").all()

            results: list[dict[str, str]] = []
            for data_row in data_rows:
                cells: list[str] = await data_row.locator("td").all_inner_texts()  # type: ignore[misc]
                if len(cells) >= 2:
                    row_dict = {
                        "label": cells[0].strip(),
                        "value": cells[1].strip() if len(cells) > 1 else "",
                    }
                    results.append(row_dict)

            return results

        except Exception:
            return []

    @staticmethod
    async def _extract_complaint_details(row: Any) -> dict[str, Optional[str]]:
        """
        Extract complaint details from table.

        Args:
            row: Row element

        Returns:
            Dict with complaint details
        """
        details: dict[str, Optional[str]] = {
            "customer_complaint": None,
            "customer_coding": None,
            "workshop_finding": None,
            "workshop_coding": None,
            "damage_part": None,
        }

        try:
            # Find detail table
            table = row.locator("table").first

            # Extract all rows
            rows = await table.locator("tbody tr").all()

            for detail_row in rows:
                cells = await detail_row.locator("td").all_inner_texts()

                if len(cells) >= 2:
                    label = cells[0].strip()
                    value = cells[1].strip()

                    if "Kundenbeanstandung" in label:
                        details["customer_complaint"] = value
                    elif "Kundenkodierung" in label:
                        details["customer_coding"] = value
                    elif "Werkstattfeststellung" in label:
                        details["workshop_finding"] = value
                    elif "Werkstattkodierung" in label:
                        details["workshop_coding"] = value
                    elif "Schadensbehebendes Ersatzteil" in label:
                        details["damage_part"] = value

            return details

        except Exception:
            return details
