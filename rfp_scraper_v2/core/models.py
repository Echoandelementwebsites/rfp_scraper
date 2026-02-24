from pydantic import BaseModel, Field
from typing import Optional, List

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
