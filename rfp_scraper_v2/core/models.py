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

class Agency(BaseModel):
    """Internal model representing an agency being processed in the pipeline."""
    name: str
    state: str
    type: str
    homepage_url: str
    procurement_url: Optional[str] = None

class Bid(BaseModel):
    """Internal model representing a fully processed bid ready for DB insertion."""
    title: str
    clientName: str
    deadline: str
    description: str
    link: str
    full_text: str
    csi_divisions: List[str]
    slug: str
