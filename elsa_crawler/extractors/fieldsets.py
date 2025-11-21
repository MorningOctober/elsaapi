"""
Fieldsets extractor for Elsa Crawler.
Extracts customer and vehicle fieldset data.
"""

from playwright.async_api import Frame
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from elsa_crawler.models import FieldsetDetails


class FieldsetExtractor:
    """Extracts fieldset data from ElsaPro."""

    @staticmethod
    async def wait_for_job_details(frame: Frame, timeout: float = 20000) -> None:
        """
        Wait for job details to load in frame.

        Args:
            frame: Frame containing job details
            timeout: Timeout in milliseconds
        """
        detail_selectors = [
            "input[name='job.customer.account']",
            "input[name='job.customer.lastName']",
            "input[name='job.vehicle.vin']",
            "input[name='job.vehicle.licensePlate']",
        ]

        for selector in detail_selectors:
            try:
                await frame.wait_for_selector(selector, timeout=timeout)
                print(f"✅ Job detail field detected: {selector}")
                return
            except PlaywrightTimeoutError:
                continue

        print("⚠️  Job detail selectors not detected, proceeding anyway")

    @staticmethod
    async def extract_fieldset(frame: Frame, fieldset_id: str) -> FieldsetDetails:
        """
        Extract fieldset details (customer/vehicle data).

        Args:
            frame: Frame containing fieldset
            fieldset_id: HTML ID of fieldset element

        Returns:
            FieldsetDetails instance

        Raises:
            RuntimeError: If fieldset not found
        """
        print(f"📦 Extracting fieldset: {fieldset_id}")

        fieldset_locator = frame.locator(f"#{fieldset_id}")

        try:
            await fieldset_locator.wait_for(state="visible", timeout=6000)
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"Fieldset {fieldset_id} not found") from exc

        data = await frame.evaluate(
            """
            (fid) => {
                const fieldset = document.querySelector(`#${fid}`);
                if (!fieldset) return null;
                
                const rows = [];
                
                const collectFromLabelCell = (labelCell) => {
                    const labelText = labelCell.innerText.trim();
                    if (!labelText) return;
                    
                    const row = labelCell.closest('tr');
                    let valueText = '';
                    let inputName = null;
                    let inputId = null;
                    
                    if (row) {
                        const fieldCell = row.querySelector('td.field, td:nth-of-type(2)');
                        if (fieldCell) {
                            const input = fieldCell.querySelector('input, select, textarea');
                            if (input) {
                                valueText = input.value || input.textContent || '';
                                inputName = input.getAttribute('name');
                                inputId = input.id || null;
                            } else {
                                valueText = fieldCell.innerText.trim();
                            }
                        }
                    }
                    
                    rows.push({
                        label: labelText,
                        value: valueText.trim(),
                        raw: valueText.trim(),
                        inputName,
                        inputId
                    });
                };
                
                // Try td.label cells first
                fieldset.querySelectorAll('td.label').forEach(collectFromLabelCell);
                
                // Fallback to label elements
                if (!rows.length) {
                    fieldset.querySelectorAll('label').forEach((labelElement) => {
                        const labelText = labelElement.innerText.trim();
                        if (!labelText) return;
                        
                        const field = labelElement.nextElementSibling;
                        const text = field?.innerText?.trim();
                        
                        rows.push({
                            label: labelText,
                            value: text || '',
                            raw: text || '',
                            inputName: null,
                            inputId: null
                        });
                    });
                }
                
                return {
                    id: fieldset.id || fid,
                    title: fieldset.querySelector('legend')?.innerText.trim() || null,
                    rawText: fieldset.innerText.trim(),
                    rows,
                    html: fieldset.innerHTML
                };
            }
            """,
            fieldset_id,
        )

        if data is None or not isinstance(data, dict):
            raise RuntimeError(f"Fieldset {fieldset_id} extraction failed")

        return FieldsetDetails.model_validate(data)
