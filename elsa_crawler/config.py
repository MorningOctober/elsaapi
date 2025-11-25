"""
Unified configuration module for Elsa Crawler.
Handles environment variables and manual user input for missing values.
"""

from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class ElsaConfig(BaseSettings):
    """Central configuration with manual input fallback for missing credentials."""

    # URLs
    elsa_base_url: str = Field(
        default="https://grp.volkswagenag.com/elsapro/elsaweb/ctr/elsaFs",
        description="Base URL for ElsaPro",
    )

    # Credentials (optional in env, will prompt if missing)
    elsa_username: Optional[str] = Field(default=None, alias="ELSA_USERNAME")
    elsa_password: Optional[str] = Field(default=None, alias="ELSA_PASSWORD")
    totp_secret: Optional[str] = Field(default=None, alias="ELSA_SECRET")
    otp_code: Optional[str] = Field(default=None, alias="OTP_CODE")

    # VIN (optional, will prompt if missing)
    vin: Optional[str] = Field(default=None, alias="ELSA_VIN")

    # Redis
    redis_url: str = Field(default="redis://localhost:6379", alias="REDIS_URL")
    redis_db: int = Field(default=0, alias="REDIS_DB")

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic: str = Field(default="elsa-documents", alias="KAFKA_TOPIC")

    # Qdrant
    qdrant_url: str = Field(default="http://localhost:6333", alias="QDRANT_URL")
    qdrant_collection: str = Field(default="elsa_documents", alias="QDRANT_COLLECTION")
    embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2", alias="EMBEDDING_MODEL"
    )

    # Crawler settings
    max_workers: int = Field(default=3, ge=1, le=10)
    max_documents_per_category: int = Field(
        default=200,
        ge=1,
        le=500,
        description="Maximum documents to extract per category (1-500)",
    )

    clear_before_crawl: bool = Field(
        default=True,
        description="Clear existing VIN data before crawling for fresh data (recommended)",
    )
    headless: bool = Field(default=True)
    timeout: int = Field(default=30000, ge=5000)  # milliseconds

    # API
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    @field_validator("vin")
    @classmethod
    def validate_vin(cls, v: Optional[str]) -> Optional[str]:
        """Validate VIN format if provided."""
        if v and len(v) != 17:
            raise ValueError("VIN must be exactly 17 characters")
        return v.upper() if v else None


async def prompt_user_input(prompt: str, password: bool = False) -> str:
    """
    Async wrapper for user input with optional password masking.

    Args:
        prompt: The prompt message to display
        password: Whether to mask input (for passwords)

    Returns:
        User input string
    """
    import asyncio
    from getpass import getpass

    loop = asyncio.get_event_loop()

    if password:
        # Run getpass in executor since it's blocking
        value = await loop.run_in_executor(None, getpass, prompt)
    else:
        # Run input in executor since it's blocking
        value = await loop.run_in_executor(None, input, prompt)

    return value.strip()


async def get_otp_from_user() -> str:
    """
    Prompt user for OTP code.

    Returns:
        6-digit OTP code
    """
    while True:
        otp = await prompt_user_input("🔑 Bitte OTP-Code eingeben (6 Ziffern): ")
        if otp.isdigit() and len(otp) == 6:
            return otp
        print("❌ Ungültiger OTP-Code. Bitte 6 Ziffern eingeben.")


async def ensure_credentials(config: ElsaConfig) -> tuple[str, str]:
    """
    Ensure username and password are available, prompt if missing.

    Args:
        config: ElsaConfig instance

    Returns:
        Tuple of (username, password)
    """
    username = config.elsa_username
    password = config.elsa_password

    if not username:
        print("ℹ️  ELSA_USERNAME nicht in Umgebungsvariablen gefunden.")
        username = await prompt_user_input("👤 Benutzername: ")

    if not password:
        print("ℹ️  ELSA_PASSWORD nicht in Umgebungsvariablen gefunden.")
        password = await prompt_user_input("🔒 Passwort: ", password=True)

    return username, password


async def ensure_vin(config: ElsaConfig) -> str:
    """
    Ensure VIN is available, prompt if missing.

    Args:
        config: ElsaConfig instance

    Returns:
        17-character VIN
    """
    vin = config.vin

    if not vin:
        print("ℹ️  ELSA_VIN nicht in Umgebungsvariablen gefunden.")
        while True:
            vin = await prompt_user_input("🚗 VIN (17 Zeichen): ")
            vin = vin.strip().upper()

            if len(vin) == 17:
                return vin

            print(f"❌ VIN muss genau 17 Zeichen haben (eingegeben: {len(vin)})")

    return vin


# Singleton instance
_config_instance: Optional[ElsaConfig] = None


def get_config() -> ElsaConfig:
    """
    Get or create the global config instance.

    Returns:
        ElsaConfig singleton instance
    """
    global _config_instance

    if _config_instance is None:
        _config_instance = ElsaConfig()

    return _config_instance
