"""
Unified Pydantic models for Elsa Crawler.
All data validation models consolidated in one place.
"""

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

# ============================================================================
# Authentication Models
# ============================================================================


class Credentials(BaseModel):
    """User credentials for ElsaPro authentication."""

    username: str = Field(..., min_length=1, description="ElsaPro username")
    password: str = Field(..., min_length=1, description="ElsaPro password")
    totp_secret: Optional[str] = Field(
        default=None, description="TOTP secret for OTP generation"
    )
    otp_code: Optional[str] = Field(default=None, description="Manual OTP code")

    @field_validator("otp_code")
    @classmethod
    def validate_otp_code(cls, v: Optional[str]) -> Optional[str]:
        """Validate OTP code format if provided."""
        if v and (not v.isdigit() or len(v) != 6):
            raise ValueError("OTP code must be 6 digits")
        return v


# ============================================================================
# Document & Content Models
# ============================================================================


class FieldsetRow(BaseModel):
    """Single row in a fieldset (label-value pair)."""

    label: str
    value: str
    raw: str
    input_name: Optional[str] = Field(default=None, alias="inputName")
    input_id: Optional[str] = Field(default=None, alias="inputId")

    class Config:
        populate_by_name = True


class FieldsetDetails(BaseModel):
    """Extracted fieldset information (customer/vehicle data)."""

    id: str
    title: Optional[str] = None
    raw_text: str = Field(alias="rawText")
    rows: list[FieldsetRow]
    html: str

    class Config:
        populate_by_name = True


class MessageBoxPayload(BaseModel):
    """Message box content from ElsaPro UI."""

    text: str
    html: str


class InfomediaTreeNode(BaseModel):
    """Node in the Infomedia tree structure."""

    frame_name: Optional[str] = Field(default=None, alias="frameName")
    frame_url: Optional[str] = Field(default=None, alias="frameUrl")
    selector: str
    element_id: Optional[str] = Field(default=None, alias="elementId")
    role: Optional[str] = None
    classes: list[str] = Field(default_factory=list)
    text: str
    title: Optional[str] = None
    node_id: Optional[str] = Field(default=None, alias="nodeId")
    relative_href: Optional[str] = Field(default=None, alias="relativeHref")
    href: Optional[str] = None
    target: Optional[str] = None
    level_code: Optional[str] = Field(default=None, alias="levelCode")
    tree_type: Optional[str] = Field(default=None, alias="treeType")
    depth: int = 0
    path: list[str] = Field(default_factory=list)
    path_string: Optional[str] = Field(default=None, alias="pathString")
    child_ids: list[str] = Field(default_factory=list, alias="childIds")
    has_children: bool = Field(default=False, alias="hasChildren")
    detail_frame: Optional[str] = Field(default=None, alias="detailFrame")
    detail_url: Optional[str] = Field(default=None, alias="detailUrl")
    detail_html: Optional[str] = Field(default=None, alias="detailHtml")
    detail_text: Optional[str] = Field(default=None, alias="detailText")
    raw_html: Optional[str] = Field(default=None, alias="rawHtml")

    class Config:
        populate_by_name = True


class FieldsetSnapshot(BaseModel):
    """Complete snapshot of VIN fieldset data."""

    vin: str = Field(..., min_length=17, max_length=17, description="17-character VIN")
    customer: FieldsetDetails
    vehicle: FieldsetDetails
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())
    message_box: Optional[MessageBoxPayload] = Field(default=None, alias="messageBox")
    infomedia_trees: Optional[list[InfomediaTreeNode]] = Field(
        default=None, alias="infomediaTrees"
    )

    class Config:
        populate_by_name = True

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: str) -> str:
        """Validate and normalize VIN."""
        return v.upper()


class DocumentData(BaseModel):
    """Document data for Kafka/Qdrant pipeline."""

    vin: str = Field(..., description="Vehicle VIN")
    category: str = Field(..., description="Document category")
    title: str = Field(..., description="Document title")
    content: str = Field(..., description="Document content/text")
    url: Optional[str] = Field(default=None, description="Source URL")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())


class DocumentResponse(BaseModel):
    """API response for document retrieval."""

    vin: str
    documents: list[DocumentData]
    total: int
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())


# ============================================================================
# Crawler State Models
# ============================================================================


class CrawlerStats(BaseModel):
    """Statistics for crawler progress."""

    categories_crawled: int = 0
    documents_extracted: int = 0
    errors: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class CrawlerStatus(BaseModel):
    """Current status of the crawler."""

    is_running: bool = False
    current_vin: Optional[str] = None
    num_workers: int = 0
    stats: CrawlerStats = Field(default_factory=CrawlerStats)


class CrawlerConfig(BaseModel):
    """Configuration for crawler execution."""

    vin: str = Field(..., min_length=17, max_length=17)
    max_workers: int = Field(default=3, ge=1, le=10)
    headless: bool = Field(default=True)
    timeout: int = Field(default=30000, ge=5000, description="Timeout in milliseconds")

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: str) -> str:
        """Validate and normalize VIN."""
        return v.upper()


# ============================================================================
# Category & Extraction Models
# ============================================================================


class Category(BaseModel):
    """Represents a category in the document tree."""

    id: str
    name: str
    url: Optional[str] = None
    parent_id: Optional[str] = None
    depth: int = 0
    has_children: bool = False


class ExtractedDocument(BaseModel):
    """Extracted document with metadata."""

    category_id: str
    category_name: str
    title: str
    content: str
    url: Optional[str] = None
    extraction_method: Literal["new", "fallback", "basic"] = "new"
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())


# ============================================================================
# API Request/Response Models
# ============================================================================


class StartCrawlerRequest(BaseModel):
    """Request to start crawler."""

    vin: str = Field(..., min_length=17, max_length=17)
    max_workers: int = Field(default=3, ge=1, le=10)

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: str) -> str:
        """Validate and normalize VIN."""
        return v.upper()


class ApiResponse(BaseModel):
    """Generic API response."""

    success: bool
    message: str
    data: Optional[dict[str, Any]] = None


# ============================================================================
# Vehicle History Models
# ============================================================================


class HistoryEntryBase(BaseModel):
    """Base model for vehicle history entries."""

    acceptance_date: str = Field(..., description="Annahmetermin")
    mileage: int = Field(..., description="Laufleistung in km")
    order_number: str = Field(..., description="Auftrags-Nr")


class ServicePlanEntry(HistoryEntryBase):
    """Digitaler Serviceplan entry."""

    entry_type: Literal["Digitaler Serviceplan"] = "Digitaler Serviceplan"
    service_proof: Optional[str] = Field(default=None, description="Service-Nachweis")
    additional_work: list[dict[str, str]] = Field(  # type: ignore[misc]
        default_factory=list, description="Zusatzarbeiten table"
    )
    remarks: list[dict[str, str]] = Field(  # type: ignore[misc]
        default_factory=list, description="Anmerkungen/Beschreibungen"
    )


class ComplaintEntry(HistoryEntryBase):
    """Beanstandung (BA) entry."""

    entry_type: Literal["Beanstandung"] = "Beanstandung"
    ba_id: str = Field(..., description="BA-ID")
    customer_complaint: Optional[str] = Field(
        default=None, description="Kundenbeanstandung"
    )
    customer_coding: Optional[str] = Field(default=None, description="Kundenkodierung")
    workshop_finding: Optional[str] = Field(
        default=None, description="Werkstattfeststellung"
    )
    workshop_coding: Optional[str] = Field(
        default=None, description="Werkstattkodierung"
    )
    damage_part: Optional[str] = Field(
        default=None, description="Schadensbehebendes Ersatzteil"
    )


class InvoiceEntry(HistoryEntryBase):
    """Rechnung entry."""

    entry_type: Literal["Rechnung"] = "Rechnung"
    invoice_number: str = Field(..., description="Rechnungs-Nr")
    remark: Optional[str] = Field(default=None, description="Anmerk (Wartung/Gewährl.)")
    work_positions: list[dict[str, str]] = Field(  # type: ignore[misc]
        default_factory=list, description="Arbeitspositionen table"
    )
    parts_positions: list[dict[str, str]] = Field(  # type: ignore[misc]
        default_factory=list, description="Teilepositionen table"
    )


class VehicleHistory(BaseModel):
    """Complete vehicle history with all entries."""

    vin: str = Field(..., min_length=17, max_length=17, description="Vehicle VIN")
    extraction_timestamp: float = Field(
        default_factory=lambda: datetime.now().timestamp()
    )
    extraction_status: Literal["complete", "partial"] = Field(
        default="complete", description="Extraction completion status"
    )
    total_entries: int = Field(..., ge=0, description="Total entries found")
    successful_entries: int = Field(..., ge=0, description="Successfully extracted")
    failed_entries: int = Field(default=0, ge=0, description="Failed extractions")
    entries: list[ServicePlanEntry | ComplaintEntry | InvoiceEntry] = Field(  # type: ignore[misc]
        default_factory=list
    )

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: str) -> str:
        """Validate and normalize VIN."""
        return v.upper()
