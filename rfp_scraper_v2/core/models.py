from pydantic import BaseModel, Field
from typing import List, Optional

# --- New Schemas (Requested) ---

class AgencySchema(BaseModel):
    name: str = Field(description="The name of the government agency.")
    state: str = Field(description="The 2-letter state abbreviation or full name.")
    homepage_url: str = Field(description="The root URL of the agency.")

class DiscoverySchema(BaseModel):
    procurement_url: Optional[str] = Field(
        default=None,
        description="The absolute URL pointing to the agency's bids, RFPs, or purchasing portal. Return null if none exists."
    )

class BidExtractionSchema(BaseModel):
    reasoning: Optional[str] = Field(default=None, description="Briefly explain why this active bid is being extracted and how you resolved its link.")
    title: Optional[str] = Field(default="Unknown Title", description="The official title or name of the project/RFP.")
    clientName: Optional[str] = Field(default="Unknown Client", description="The name of the agency issuing the bid.")
    deadline: Optional[str] = Field(default=None, description="The due date of the bid in YYYY-MM-DD format. Return empty string if unknown.")
    description: Optional[str] = Field(default=None, description="A brief 1-3 sentence summary of the work required.")
    link: Optional[str] = Field(default="", description="The absolute URL pointing to the bid details or PDF document. Critical: Must be an absolute URL.")

class ClassificationSchema(BaseModel):
    reasoning: Optional[str] = Field(
        default=None,
        description="Think step-by-step. Briefly analyze the scope of work, determine if it involves physical construction/heavy infrastructure, and explicitly justify your CSI division choices before classifying."
    )
    is_construction_related: bool = Field(
        description="True if the RFP involves construction, infrastructure, maintenance, or engineering. False if it is software, janitorial, staffing, etc."
    )
    csi_divisions: List[str] = Field(
        default_factory=list,
        description="A list of relevant CSI MasterFormat divisions exactly as they appear in the provided reference list (e.g., 'Division 03 - Concrete'). Empty list if none."
    )
    confidence_score: int = Field(
        default=0,
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
    deadline: Optional[str] = None
    description: Optional[str] = None
    link: str
    full_text: str
    csi_divisions: List[str]
    slug: str
