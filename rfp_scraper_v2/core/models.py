from pydantic import BaseModel, Field
from typing import List, Optional

# --- New Schemas (Requested) ---

class AgencySchema(BaseModel):
    name: str = Field(description="The name of the government agency.")
    state: str = Field(description="The 2-letter state abbreviation or full name.")
    homepage_url: str = Field(description="The root URL of the agency.")

class DiscoverySchema(BaseModel):
    procurement_url: Optional[str] = Field(
        description="The absolute URL pointing to the agency's bids, RFPs, or purchasing portal. Return null if none exists."
    )

class BidExtractionSchema(BaseModel):
    title: str = Field(description="The official title or name of the project/RFP.")
    clientName: str = Field(description="The name of the agency issuing the bid.")
    deadline: str = Field(description="The due date of the bid in YYYY-MM-DD format. Return empty string if unknown.")
    description: str = Field(description="A brief 1-3 sentence summary of the work required.")
    link: str = Field(description="The absolute URL pointing to the bid details or PDF document. Critical: Must be absolute.")

class ClassificationSchema(BaseModel):
    is_construction_related: bool = Field(
        description="True if the RFP involves construction, infrastructure, maintenance, or engineering. False if it is software, janitorial, staffing, etc."
    )
    csi_divisions: List[str] = Field(
        description="A list of relevant CSI MasterFormat divisions (e.g., 'Division 03 Concrete'). Empty list if none."
    )
    confidence_score: int = Field(
        description="A score from 1-100 indicating confidence in the classification."
    )

# --- Existing Models (Preserved for compatibility with database.py and other modules) ---

class Agency(BaseModel):
    """
    Represents a government agency or local jurisdiction.
    """
    name: str = Field(..., description="The official name of the agency (e.g. 'City of Bridgeport').")
    state: str = Field(..., description="The state abbreviation (e.g. 'CT').")
    type: str = Field(..., description="The type of agency (e.g. 'city', 'county', 'state_agency').")
    homepage_url: Optional[str] = Field(None, description="The official homepage URL (if known).")
    procurement_url: Optional[str] = Field(None, description="The discovered procurement/bids page URL.")

class Bid(BaseModel):
    """
    Represents a single extracted bid opportunity.
    """
    title: str = Field(..., description="Title of the bid or RFP.")
    client_name: str = Field(..., alias="clientName", description="The specific department or agency issuing the bid.")
    deadline: Optional[str] = Field(None, description="Bid deadline in YYYY-MM-DD format.")
    description: Optional[str] = Field(None, description="Brief description of the opportunity.")
    link: str = Field(..., description="Direct link to the bid detail page or PDF.")

    # Enriched Fields (Step 4)
    full_text: Optional[str] = Field(None, description="Unabridged text content extracted from the detail page/PDF.")
    csi_divisions: Optional[List[str]] = Field(None, description="List of CSI MasterFormat divisions applicable to this bid.")
    slug: Optional[str] = Field(None, description="Unique identifier for database storage.")

    class Config:
        populate_by_name = True
